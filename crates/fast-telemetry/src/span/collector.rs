//! Thread-local span collector with zero-atomic submit path.
//!
//! Each thread buffers completed spans in a thread-local [`Vec`] — avoiding atomics,
//! CAS loops, and hopefully cache-line contention.  When the buffer reaches
//! [`FLUSH_THRESHOLD`] spans (or on thread exit), it is moved to a shared
//! outbox that the exporter drains via [`SpanCollector::drain_into`].
//!
//! The outbox transfer uses a [`parking_lot::Mutex`] that is per-thread and
//! therefore uncontended during normal operation (the exporter only touches it
//! every few seconds).
//!
//! Each outbox is capped at [`OUTBOX_CAPACITY`] spans to bound memory.
//! When the outbox is full, flushes are silently dropped.
//!
//! External adapters can use an owning [`SpanSink`] instead. It publishes
//! immediately through fixed producer shards with one shared capacity limit,
//! bypassing native thread-local buffering and adaptive sampling.

use std::borrow::Cow;
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;

use super::context::SpanContext;
use super::types::{CollectorRef, CompletedSpan, Span, SpanKind};
use crate::metric::Counter;
use crate::thread_id::thread_id;

/// Number of spans buffered thread-locally before flushing to the shared outbox.
/// Higher values amortize the mutex cost but increase latency to export.
const FLUSH_THRESHOLD: usize = 64;

/// Maximum number of spans held per outbox.  When a flush would exceed this
/// limit the batch is silently dropped, bounding memory to
/// `OUTBOX_CAPACITY × num_threads × sizeof(CompletedSpan)`.
const OUTBOX_CAPACITY: usize = 4096;

/// Fixed adapter shards bound registry size independently of producer lifetimes.
const SINK_SHARDS: usize = 32;

/// Reuse small bursts per producer without preallocating storage on idle shards.
const SINK_RETAINED_BATCH: usize = FLUSH_THRESHOLD * 4;

/// A shared outbox that a single thread flushes into and the exporter drains.
struct Outbox {
    spans: Mutex<Vec<CompletedSpan>>,
}

impl Outbox {
    fn new() -> Self {
        Self {
            spans: Mutex::new(Vec::with_capacity(FLUSH_THRESHOLD * 2)),
        }
    }
}

/// Native outboxes keep their original buffering and allocation policy.
enum RegisteredOutbox {
    Native(Arc<Outbox>),
    Sink(Arc<SinkOutbox>),
}

/// Capacity is shared by every shard of one sink, never divided into quotas.
struct SinkBudget {
    capacity: usize,
    queued: AtomicUsize,
}

struct SinkQueue {
    spans: Vec<CompletedSpan>,
    accepted: u64,
    rejected: u64,
}

struct SinkOutbox {
    queue: CachePadded<Mutex<SinkQueue>>,
    // The budget owns neither the sink nor the collector, avoiding an Arc cycle.
    budget: Arc<SinkBudget>,
    retained_capacity: usize,
}

impl SinkOutbox {
    fn drain_into(&self, buf: &mut Vec<CompletedSpan>) {
        let mut queue = self.queue.lock();
        let drained = queue.spans.len();
        if drained != 0 {
            buf.append(&mut queue.spans);
            // Keep the shard locked until capacity is returned: another publish
            // on this shard cannot observe its emptied queue with a stale budget.
            self.budget.queued.fetch_sub(drained, Ordering::Relaxed);
            // Reuse ordinary batches without retaining each shard's high-water
            // allocation indefinitely. Native outboxes retain their own policy.
            queue.spans.shrink_to(self.retained_capacity);
        }
    }
}

/// Per-thread buffer for a single [`SpanCollector`].
struct ThreadBuffer {
    /// Thread-local span buffer.  `push()` is a plain Vec append — zero atomics.
    buffer: Vec<CompletedSpan>,
    /// Shared outbox registered with the collector.
    outbox: Arc<Outbox>,
    /// Adaptive sampling: log2 of the sampling denominator.
    /// 0 = record every span, 5 = 1/32, 6 = 1/64, 7 = 1/128.
    sample_shift: u32,
    /// Monotonic counter for sampling decisions.
    span_counter: u64,
}

impl ThreadBuffer {
    fn new(outbox: Arc<Outbox>) -> Self {
        Self {
            buffer: Vec::with_capacity(FLUSH_THRESHOLD),
            outbox,
            sample_shift: 0,
            span_counter: 0,
        }
    }

    /// Returns `true` if the next span should be recorded, based on the
    /// current adaptive sampling rate.  Pure thread-local arithmetic —
    /// zero atomics.
    #[inline]
    fn should_record(&mut self) -> bool {
        self.span_counter = self.span_counter.wrapping_add(1);
        if self.sample_shift == 0 {
            return true;
        }
        (self.span_counter & ((1u64 << self.sample_shift) - 1)) == 0
    }

    #[inline]
    fn push(&mut self, span: CompletedSpan) {
        self.buffer.push(span);
        if self.buffer.len() >= FLUSH_THRESHOLD {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if !self.buffer.is_empty() {
            let mut outbox = self.outbox.spans.lock();
            let occupancy = outbox.len();
            if occupancy < OUTBOX_CAPACITY {
                outbox.append(&mut self.buffer);
            } else {
                self.buffer.clear();
            }
            // Adjust sampling rate based on outbox pressure.
            self.sample_shift = if occupancy <= OUTBOX_CAPACITY / 4 {
                0 // ≤25% full — record everything
            } else if occupancy <= OUTBOX_CAPACITY / 2 {
                5 // ≤50% — 1/32
            } else if occupancy <= OUTBOX_CAPACITY * 3 / 4 {
                6 // ≤75% — 1/64
            } else {
                7 // >75% — 1/128
            };
        }
    }
}

impl Drop for ThreadBuffer {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Per-thread state: maps collector pointer → thread buffer.
///
/// Uses a raw pointer as key to avoid Arc overhead on the collector itself.
/// This is safe because `submit()` is called through a `CollectorRef` that
/// is guaranteed to outlive the span (see `CollectorRef` safety comments).
struct ThreadLocalState {
    /// Sorted by collector pointer for binary search.  In practice there is
    /// one collector per process, so this is a single-element vec.
    entries: Vec<(usize, ThreadBuffer)>,
}

impl ThreadLocalState {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    #[inline]
    fn get_or_register(&mut self, collector: &SpanCollector) -> &mut ThreadBuffer {
        let key = collector as *const SpanCollector as usize;
        // Fast path: check if we already have an entry for this collector.
        let pos = self.entries.iter().position(|(k, _)| *k == key);
        if let Some(pos) = pos {
            return &mut self.entries[pos].1;
        }
        self.register(collector, key)
    }

    #[cold]
    fn register(&mut self, collector: &SpanCollector, key: usize) -> &mut ThreadBuffer {
        // Register a new outbox with the collector.
        let outbox = Arc::new(Outbox::new());
        collector
            .outboxes
            .lock()
            .push(RegisteredOutbox::Native(Arc::clone(&outbox)));
        self.entries.push((key, ThreadBuffer::new(outbox)));
        &mut self.entries.last_mut().expect("just pushed").1
    }
}

impl Drop for ThreadLocalState {
    fn drop(&mut self) {
        // Flush all remaining spans on thread exit.
        for (_, buffer) in &mut self.entries {
            buffer.flush();
        }
    }
}

thread_local! {
    /// Thread-local span state.
    ///
    /// Uses `UnsafeCell` rather than `RefCell` to skip the runtime borrow
    /// check on the per-span hot path (~5 cycles saved per submit). Safety
    /// is preserved by enforcing two invariants:
    ///   1. All access is single-threaded (`thread_local!`).
    ///   2. Every `&mut` borrow is fully consumed inside the closure passed
    ///      to `LOCAL.with`. No closure recursively re-enters `with`, and
    ///      none of the called methods (`get_or_register`, `push`,
    ///      `should_record`) re-enter the cell.
    static LOCAL: UnsafeCell<ThreadLocalState> = UnsafeCell::new(ThreadLocalState::new());
}

/// Thread-local span collector with zero-atomic submit path.
///
/// Completed spans are buffered in a thread-local [`Vec`] and periodically
/// flushed to a shared outbox.  The exporter calls
/// [`drain_into`](SpanCollector::drain_into) to harvest all pending spans.
///
/// Created explicitly and held as `Arc<SpanCollector>`.
pub struct SpanCollector {
    /// Native per-thread outboxes and fixed adapter shards. The registry lock
    /// is taken for registration, drains, and queue-length queries, never for
    /// individual adapter publications.
    outboxes: Mutex<Vec<RegisteredOutbox>>,
    /// Spans that were recorded (passed sampling).
    spans_recorded: Counter,
    /// Spans that were dropped by adaptive sampling.
    spans_sampled_out: Counter,
}

/// An owning, bounded publication path for external instrumentation adapters.
///
/// Unlike native spans, this sink never uses thread-local buffering or adaptive
/// sampling. Every accepted span is immediately visible to `drain_into`, even
/// while the submitting worker stays alive and idle. Clones share one capacity
/// limit across fixed producer shards and their counters. The collector is kept
/// alive until the last sink is dropped.
#[derive(Clone)]
pub struct SpanSink {
    inner: Arc<SpanSinkInner>,
}

struct SpanSinkInner {
    _collector: Arc<SpanCollector>,
    outboxes: Box<[Arc<SinkOutbox>]>,
    budget: Arc<SinkBudget>,
}

impl SpanSink {
    /// Publish one completed span without sampling.
    ///
    /// Returns ownership of the span on capacity exhaustion. Rejection is also
    /// counted by `rejected_count`; the caller decides whether to retry or drop.
    #[allow(
        deprecated,
        reason = "MSRV 1.93 predates try_update, stabilized in Rust 1.95"
    )]
    pub fn publish(&self, span: CompletedSpan) -> Result<(), Box<CompletedSpan>> {
        let shard = thread_id() & (self.inner.outboxes.len() - 1);
        let mut queue = self.inner.outboxes[shard].queue.lock();
        // Reserve exactly one global slot while holding the destination lock.
        // Other shards can fill all remaining slots; no shard-local quota can
        // reject a publication while capacity is available elsewhere.
        if self
            .inner
            .budget
            .queued
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |queued| {
                (queued < self.inner.budget.capacity).then(|| queued + 1)
            })
            .is_err()
        {
            queue.rejected = queue.rejected.wrapping_add(1);
            return Err(Box::new(span));
        }
        queue.spans.push(span);
        queue.accepted = queue.accepted.wrapping_add(1);
        Ok(())
    }

    /// Total successful publications (including spans already drained).
    /// Exact after publishers quiesce; concurrent reads aggregate shards in turn.
    pub fn accepted_count(&self) -> u64 {
        self.inner.outboxes.iter().fold(0u64, |total, outbox| {
            total.wrapping_add(outbox.queue.lock().accepted)
        })
    }

    /// Total publication attempts rejected because the queue was full.
    /// Exact after publishers quiesce; concurrent reads aggregate shards in turn.
    pub fn rejected_count(&self) -> u64 {
        self.inner.outboxes.iter().fold(0u64, |total, outbox| {
            total.wrapping_add(outbox.queue.lock().rejected)
        })
    }

    /// Maximum number of queued completed spans.
    pub fn capacity(&self) -> usize {
        self.inner.budget.capacity
    }
}

impl SpanCollector {
    /// Create a new collector.
    ///
    /// The `_num_shards` and `_capacity_per_shard` parameters are accepted for
    /// API compatibility but are no longer used — each thread gets its own
    /// buffer automatically, and buffers are unbounded.
    pub fn new(_num_shards: usize, _capacity_per_shard: usize) -> Self {
        Self {
            outboxes: Mutex::new(Vec::new()),
            spans_recorded: Counter::new(8),
            spans_sampled_out: Counter::new(8),
        }
    }

    /// Register a bounded, owning sink for externally completed spans.
    ///
    /// Capacity bounds queued spans, not the size of individual spans. A sink's
    /// fixed producer shards remain registered until the collector is dropped,
    /// so reuse sinks rather than creating one per request. Producer thread exit
    /// creates no additional outboxes. Native submission is unchanged.
    /// Queue storage is allocated lazily. After draining, each of at most 32
    /// shards keeps up to the larger of 256 slots or its equal capacity share
    /// for reuse (never more than `capacity` slots per shard).
    ///
    /// # Panics
    /// Panics if `capacity` is zero.
    pub fn span_sink(self: &Arc<Self>, capacity: usize) -> SpanSink {
        assert!(capacity > 0, "span sink capacity must be nonzero");
        let shard_count = capacity.min(SINK_SHARDS).next_power_of_two();
        let budget = Arc::new(SinkBudget {
            capacity,
            queued: AtomicUsize::new(0),
        });
        let retained_capacity = capacity
            .div_ceil(shard_count)
            .max(SINK_RETAINED_BATCH)
            .min(capacity);
        let outboxes: Box<[_]> = (0..shard_count)
            .map(|_| {
                Arc::new(SinkOutbox {
                    queue: CachePadded::new(Mutex::new(SinkQueue {
                        spans: Vec::new(),
                        accepted: 0,
                        rejected: 0,
                    })),
                    budget: Arc::clone(&budget),
                    retained_capacity,
                })
            })
            .collect();
        self.outboxes.lock().extend(
            outboxes
                .iter()
                .map(|outbox| RegisteredOutbox::Sink(Arc::clone(outbox))),
        );
        SpanSink {
            inner: Arc::new(SpanSinkInner {
                _collector: Arc::clone(self),
                outboxes,
                budget,
            }),
        }
    }

    /// Create a new root span with a fresh trace ID.
    ///
    /// The span is associated with this collector and will be submitted
    /// here when it drops.  Under high load, adaptive sampling may return
    /// a no-op span that skips all recording and submission.
    pub fn start_span(
        self: &Arc<Self>,
        name: impl Into<Cow<'static, str>>,
        kind: SpanKind,
    ) -> Span {
        let collector_ref = CollectorRef::from_arc(self);
        if self.should_record() {
            self.spans_recorded.inc();
            Span::new_root(name, kind, collector_ref)
        } else {
            self.spans_sampled_out.inc();
            Span::noop(collector_ref)
        }
    }

    /// Create a root span from an incoming W3C `traceparent` header.
    ///
    /// If the header is valid, the span inherits the remote trace ID and sets
    /// `parent_span_id` to the remote span ID.  If the header is `None` or
    /// invalid, behaves like [`start_span`](Self::start_span) (new trace ID).
    ///
    /// Adaptive sampling applies: under load, may return a no-op span.
    pub fn start_span_from_traceparent(
        self: &Arc<Self>,
        traceparent: Option<&str>,
        name: impl Into<Cow<'static, str>>,
        kind: SpanKind,
    ) -> Span {
        let collector_ref = CollectorRef::from_arc(self);
        if !self.should_record() {
            self.spans_sampled_out.inc();
            return Span::noop(collector_ref);
        }
        self.spans_recorded.inc();
        match traceparent.and_then(SpanContext::from_traceparent) {
            Some(remote_ctx) => Span::new_from_remote(name, kind, remote_ctx, collector_ref),
            None => Span::new_root(name, kind, collector_ref),
        }
    }

    /// Check the thread-local adaptive sampling counter.
    ///
    /// Returns `true` if the next span should be recorded. Pure thread-local
    /// arithmetic — zero atomics, zero contention.
    #[inline]
    fn should_record(&self) -> bool {
        LOCAL.with(|cell| {
            // SAFETY: see LOCAL definition. Single-thread access; the &mut
            // borrow is fully consumed before this closure returns.
            let state = unsafe { &mut *cell.get() };
            state.get_or_register(self).should_record()
        })
    }

    /// Submit a completed span.  Called by [`Span::drop`].
    ///
    /// Pushes to a thread-local `Vec` — zero atomics on the fast path.
    /// Every [`FLUSH_THRESHOLD`] spans, the buffer is moved to the shared
    /// outbox under a per-thread mutex (uncontended).
    #[inline]
    pub(crate) fn submit(&self, span: CompletedSpan) {
        LOCAL.with(|cell| {
            // SAFETY: see LOCAL definition. Single-thread access; the &mut
            // borrow is fully consumed before this closure returns.
            let state = unsafe { &mut *cell.get() };
            state.get_or_register(self).push(span);
        });
    }

    /// Flush the current thread's local buffer to the shared outbox.
    ///
    /// Call this before [`drain_into`](Self::drain_into) when running on the
    /// same thread that submitted spans (e.g., in tests or single-threaded
    /// exporters).  In production, thread-local buffers are flushed
    /// automatically when they reach the internal flush threshold or on thread
    /// exit.
    pub fn flush_local(&self) {
        LOCAL.with(|cell| {
            // SAFETY: see LOCAL definition.
            let state = unsafe { &mut *cell.get() };
            let key = self as *const SpanCollector as usize;
            if let Some(pos) = state.entries.iter().position(|(k, _)| *k == key) {
                state.entries[pos].1.flush();
            }
        });
    }

    /// Drain all pending spans into the provided buffer.
    ///
    /// This is the primary method for exporters.  It collects spans from all
    /// registered thread outboxes.  Spans still in thread-local buffers below
    /// the flush threshold are NOT included unless [`flush_local`](Self::flush_local)
    /// is called first (or the thread exits).
    ///
    /// The caller can reuse the buffer across export cycles to avoid repeated
    /// allocation.
    pub fn drain_into(&self, buf: &mut Vec<CompletedSpan>) {
        let outboxes = self.outboxes.lock();
        for outbox in outboxes.iter() {
            match outbox {
                RegisteredOutbox::Native(outbox) => {
                    let mut spans = outbox.spans.lock();
                    buf.append(&mut spans);
                    // Preserve native outboxes' allocation policy.
                    spans.shrink_to(FLUSH_THRESHOLD * 2);
                }
                RegisteredOutbox::Sink(outbox) => outbox.drain_into(buf),
            }
        }
    }

    /// Number of spans that were dropped.
    ///
    /// Always returns 0.  Retained for API compatibility; use
    /// [`sampled_out_count`](Self::sampled_out_count) for adaptive sampling stats.
    pub fn dropped_count(&self) -> u64 {
        0
    }

    /// Total spans that passed adaptive sampling and were recorded.
    pub fn recorded_count(&self) -> u64 {
        self.spans_recorded.sum() as u64
    }

    /// Total spans that were dropped by adaptive sampling.
    pub fn sampled_out_count(&self) -> u64 {
        self.spans_sampled_out.sum() as u64
    }

    /// Current number of spans waiting across all outboxes.
    ///
    /// Does not include spans still in thread-local buffers that haven't
    /// been flushed yet.
    pub fn len(&self) -> usize {
        let outboxes = self.outboxes.lock();
        outboxes
            .iter()
            .map(|outbox| match outbox {
                RegisteredOutbox::Native(outbox) => outbox.spans.lock().len(),
                RegisteredOutbox::Sink(outbox) => outbox.queue.lock().spans.len(),
            })
            .sum()
    }

    /// Returns `true` if all outboxes are empty.
    ///
    /// Does not account for spans in thread-local buffers below the flush
    /// threshold.
    pub fn is_empty(&self) -> bool {
        let outboxes = self.outboxes.lock();
        outboxes.iter().all(|outbox| match outbox {
            RegisteredOutbox::Native(outbox) => outbox.spans.lock().is_empty(),
            RegisteredOutbox::Sink(outbox) => outbox.queue.lock().spans.is_empty(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SpanId, SpanStatus, TraceId};

    /// Helper: flush + drain for same-thread tests.
    fn flush_and_drain(collector: &SpanCollector, buf: &mut Vec<CompletedSpan>) {
        collector.flush_local();
        collector.drain_into(buf);
    }

    #[test]
    fn start_and_drain() {
        let collector = Arc::new(SpanCollector::new(1, 16));
        {
            let _span = collector.start_span("op1", SpanKind::Server);
            let _span2 = collector.start_span("op2", SpanKind::Client);
        }
        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn small_batches_no_drops() {
        let collector = Arc::new(SpanCollector::new(1, 2));
        // Small batches below OUTBOX_CAPACITY are fully collected.
        {
            let _s1 = collector.start_span("a", SpanKind::Internal);
            let _s2 = collector.start_span("b", SpanKind::Internal);
            let _s3 = collector.start_span("c", SpanKind::Internal);
        }
        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn from_traceparent_valid() {
        let collector = Arc::new(SpanCollector::new(1, 16));
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        {
            let _span =
                collector.start_span_from_traceparent(Some(tp), "handler", SpanKind::Server);
        }
        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 1);
        assert_eq!(
            buf[0].trace_id.to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn from_traceparent_invalid_falls_back() {
        let collector = Arc::new(SpanCollector::new(1, 16));
        {
            let _span =
                collector.start_span_from_traceparent(Some("garbage"), "handler", SpanKind::Server);
        }
        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 1);
        assert!(!buf[0].trace_id.is_invalid());
        assert!(buf[0].parent_span_id.is_invalid());
    }

    #[test]
    fn from_traceparent_none_creates_root() {
        let collector = Arc::new(SpanCollector::new(1, 16));
        {
            let _span = collector.start_span_from_traceparent(None, "handler", SpanKind::Server);
        }
        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 1);
        assert!(buf[0].parent_span_id.is_invalid());
    }

    #[test]
    fn concurrent_submission() {
        let collector = Arc::new(SpanCollector::new(8, 1024));
        let mut handles = Vec::new();

        for t in 0..4 {
            let c = Arc::clone(&collector);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let _span =
                        c.start_span(format!("thread_{}_span_{}", t, i), SpanKind::Internal);
                }
            }));
        }

        for h in handles {
            h.join().expect("thread join");
        }

        // Thread-local Drop flushes on thread exit, so drain_into is sufficient.
        let mut buf = Vec::new();
        collector.drain_into(&mut buf);
        assert_eq!(buf.len(), 400);
        assert_eq!(collector.dropped_count(), 0);
    }

    #[test]
    fn flush_threshold_batching() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        // Submit fewer spans than FLUSH_THRESHOLD — they should stay in
        // thread-local buffer until flushed or the threshold is reached.
        for _ in 0..FLUSH_THRESHOLD - 1 {
            let _span = collector.start_span("sub_threshold", SpanKind::Internal);
        }
        // Outbox should be empty (all in thread-local buffer).
        assert_eq!(collector.len(), 0);

        // Submit one more to cross the threshold.
        {
            let _span = collector.start_span("trigger", SpanKind::Internal);
        }
        // Now the outbox should have FLUSH_THRESHOLD spans.
        assert_eq!(collector.len(), FLUSH_THRESHOLD);
    }

    #[test]
    fn flush_local_forces_transfer() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        // Submit fewer than threshold.
        for _ in 0..5 {
            let _span = collector.start_span("local", SpanKind::Internal);
        }
        assert_eq!(collector.len(), 0);
        collector.flush_local();
        assert_eq!(collector.len(), 5);
    }

    #[test]
    fn thread_exit_flushes() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        let c = Arc::clone(&collector);
        let handle = std::thread::spawn(move || {
            // Submit fewer than FLUSH_THRESHOLD.
            for _ in 0..10 {
                let _span = c.start_span("thread_exit", SpanKind::Internal);
            }
            // Thread-local Drop should flush on thread exit.
        });
        handle.join().expect("thread join");

        let mut buf = Vec::new();
        collector.drain_into(&mut buf);
        assert_eq!(buf.len(), 10);
    }

    #[test]
    fn sink_storage_is_lazy_and_bounded_across_short_lived_producers() {
        let tiny_collector = Arc::new(SpanCollector::new(1, 1));
        let tiny = tiny_collector.span_sink(1);
        assert_eq!(tiny.inner.outboxes.len(), 1);
        assert_eq!(tiny.inner.outboxes[0].queue.lock().spans.capacity(), 0);

        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(4096);
        let registered = collector.outboxes.lock().len();
        assert!(registered <= SINK_SHARDS);
        assert!(
            sink.inner
                .outboxes
                .iter()
                .all(|outbox| outbox.queue.lock().spans.capacity() == 0)
        );
        fn completed() -> CompletedSpan {
            CompletedSpan {
                trace_id: TraceId::INVALID,
                span_id: SpanId::INVALID,
                parent_span_id: SpanId::INVALID,
                name: Cow::Borrowed("short lived"),
                kind: SpanKind::Internal,
                start_time_ns: 1,
                end_time_ns: 2,
                status: SpanStatus::Unset,
                attributes: Vec::new(),
                events: Vec::new(),
            }
        }
        let mut spans = Vec::new();
        for _ in 0..SINK_SHARDS * 2 {
            let sink = sink.clone();
            std::thread::spawn(move || {
                // Grow past the reusable allocation target on each producer.
                for _ in 0..512 {
                    assert!(sink.publish(completed()).is_ok());
                }
            })
            .join()
            .expect("producer thread");
            collector.drain_into(&mut spans);
            assert_eq!(spans.len(), 512);
            spans.clear();
        }
        assert_eq!(collector.outboxes.lock().len(), registered);
        assert_eq!(sink.accepted_count(), (SINK_SHARDS * 2 * 512) as u64);
        assert_eq!(sink.rejected_count(), 0);
        let retained: usize = sink
            .inner
            .outboxes
            .iter()
            .map(|outbox| outbox.queue.lock().spans.capacity())
            .sum();
        assert!(retained <= SINK_SHARDS * SINK_RETAINED_BATCH);
    }
}

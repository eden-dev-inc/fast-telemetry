//! Per-OS-thread double-buffered span collection.
//!
//! Native writers fill contiguous buckets and publish ownership in batches.
//! Exporters swap bucket allocations into reusable [`SpanBatch`] storage. The
//! native path uses no mutex for recording or draining; registration publishes
//! immutable nodes. External [`SpanSink`] adapters retain their bounded shards.

use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};
use std::sync::{Arc, Weak};

use crossbeam_utils::CachePadded;
use parking_lot::Mutex;

#[cfg(test)]
use super::buckets::BATCH_SIZE;
use super::buckets::{Registry, SpanBatch, Writer};
use super::context::SpanContext;
use super::types::{CollectorRef, CompletedSpan, Span, SpanKind};
use crate::thread_id::thread_id;

/// Completed spans per native publication boundary.
#[cfg(test)]
const FLUSH_THRESHOLD: usize = BATCH_SIZE;

/// Spread root statistics across wider worker pools while keeping reads and
/// counter storage bounded independently of registered producer lifetimes.
const ROOT_COUNTER_SHARDS: usize = 32;

const ROOT_RECORDED: usize = 0;
const ROOT_SAMPLED_OUT: usize = 1;

/// Related native statistics share a padded row for each thread stripe.
/// The fixed array makes the shard bound known at compile time while retaining
/// one immediately visible relaxed atomic update for every root decision.
struct RootCounters {
    rows: Box<[CachePadded<[AtomicIsize; 2]>; ROOT_COUNTER_SHARDS]>,
}

impl RootCounters {
    fn new() -> Self {
        const { assert!(ROOT_COUNTER_SHARDS.is_power_of_two()) };
        Self {
            rows: Box::new(std::array::from_fn(|_| {
                CachePadded::new([AtomicIsize::new(0), AtomicIsize::new(0)])
            })),
        }
    }

    #[inline]
    fn inc(&self, counter: usize) {
        self.rows[thread_id() & (ROOT_COUNTER_SHARDS - 1)][counter].fetch_add(1, Ordering::Relaxed);
    }

    fn sum(&self, counter: usize) -> isize {
        self.rows
            .iter()
            .map(|row| row[counter].load(Ordering::Relaxed))
            .sum()
    }
}

/// Maximum queued native spans per producer, excluding batches already owned
/// by exporters. Each of its two contiguous allocations can retain this many
/// slots; initial allocations hold one publication batch each.
const OUTBOX_CAPACITY: usize = 4096;

/// Fixed adapter shards bound registry size independently of producer lifetimes.
const SINK_SHARDS: usize = 32;

/// Reuse small bursts per producer without preallocating storage on idle shards.
const SINK_RETAINED_BATCH: usize = 256;

/// Maximum unused quota moved to a producer in one rebalance.
const SINK_QUOTA_CHUNK: usize = 64;

/// Shard quotas sum to this capacity. A full certificate is set only while all
/// shards are locked and occupied, and invalidated before a drain removes spans.
struct SinkBudget {
    capacity: usize,
    full: AtomicBool,
}

struct SinkQueue {
    spans: Vec<CompletedSpan>,
    quota: usize,
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
        if !queue.spans.is_empty() {
            // Invalidate the full certificate before freeing any capacity. A
            // setter needs this shard lock too, so it cannot pass this drain.
            self.budget.full.store(false, Ordering::SeqCst);
            buf.append(&mut queue.spans);
            // Reuse ordinary batches without retaining each shard's high-water
            // allocation indefinitely. Native outboxes retain their own policy.
            queue.spans.shrink_to(self.retained_capacity);
        }
    }
}

/// Per-thread buffer for a single [`SpanCollector`].
struct ThreadBuffer {
    writer: Writer,
    /// Keep this allocation's identity unique until the TLS entry is removed.
    /// This does not keep a discarded collector or its metric storage alive.
    collector: Weak<CollectorState>,
    /// Adaptive sampling: log2 of the sampling denominator.
    /// 0 = record every span, 5 = 1/32, 6 = 1/64, 7 = 1/128.
    sample_shift: u32,
    /// Monotonic counter for sampling decisions.
    span_counter: u64,
    recycled: u64,
}

impl ThreadBuffer {
    fn new(collector: Weak<CollectorState>) -> Self {
        Self {
            writer: Writer::new(),
            collector,
            sample_shift: 0,
            span_counter: 0,
            recycled: 0,
        }
    }

    #[inline]
    fn should_record(&mut self) -> bool {
        self.span_counter = self.span_counter.wrapping_add(1);
        if self.sample_shift == 0 {
            return true;
        }
        // Recover promptly after export instead of needing another 64 sampled
        // spans (up to 8192 attempted roots) to notice the freed storage.
        if self.span_counter & 63 == 0 {
            let recycled = self.writer.outbox.recycled.load(Ordering::Relaxed);
            if recycled != self.recycled {
                self.recycled = recycled;
                self.sample_shift = 0;
                return true;
            }
        }
        self.span_counter & ((1u64 << self.sample_shift) - 1) == 0
    }

    #[inline]
    fn push(&mut self, span: CompletedSpan) {
        if let Some(occupancy) = self.writer.push(span) {
            self.pressure(occupancy);
        }
    }

    fn pressure(&mut self, occupancy: usize) {
        self.sample_shift = if occupancy <= OUTBOX_CAPACITY / 4 {
            0
        } else if occupancy <= OUTBOX_CAPACITY / 2 {
            5
        } else if occupancy <= OUTBOX_CAPACITY * 3 / 4 {
            6
        } else {
            7
        };
    }

    fn flush(&mut self) {
        if let Some(occupancy) = self.writer.flush() {
            self.pressure(occupancy);
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
/// Raw addresses remain unique while the entry's Weak retains the collector state
/// allocation. Spans borrow their Arc owner; hot lookups update no refcounts.
/// Registration removes dead entries so collector churn does not accumulate
/// unreachable native buffers indefinitely.
struct ThreadLocalState {
    /// Searched by collector address. In practice there is one active collector
    /// per process, so this is a single-element vector.
    entries: Vec<(usize, ThreadBuffer)>,
}

impl ThreadLocalState {
    fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    #[inline]
    fn get_or_register(&mut self, collector: &CollectorState) -> &mut ThreadBuffer {
        let key = collector as *const CollectorState as usize;
        // Fast path: check if we already have an entry for this collector.
        let pos = self.entries.iter().position(|(k, _)| *k == key);
        if let Some(pos) = pos {
            return &mut self.entries[pos].1;
        }
        self.register(collector, key)
    }

    #[cold]
    fn register(&mut self, collector: &CollectorState, key: usize) -> &mut ThreadBuffer {
        self.entries
            .retain(|(_, buffer)| buffer.collector.strong_count() != 0);
        let buffer = ThreadBuffer::new(collector.weak_self.clone());
        collector.native.register(Arc::clone(&buffer.writer.outbox));
        self.entries.push((key, buffer));
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
    /// Checked borrowing rejects reentry before creating aliased mutable
    /// references. Registration and vector growth may invoke allocator callbacks,
    /// so single-thread confinement alone is not a sufficient exclusivity proof.
    static LOCAL: RefCell<ThreadLocalState> = RefCell::new(ThreadLocalState::new());
}

/// Per-thread contiguous span buckets with atomic batch handoffs.
///
/// Completed spans are buffered in a thread-local [`Vec`] and periodically
/// published by ownership transfer. The exporter calls
/// [`drain_batches`](SpanCollector::drain_batches) to preserve those buffers.
///
/// Created explicitly and held as `Arc<SpanCollector>`.
/// Reentering native recording, completion or flushing from an allocator
/// callback while native TLS is borrowed panics before creating another borrow.
pub struct SpanCollector {
    state: Arc<CollectorState>,
}

/// Stable allocation used as native TLS identity, independently of outer moves.
pub(crate) struct CollectorState {
    native: Registry,
    /// External adapter shards retain their existing publication policy.
    outboxes: Mutex<Vec<Arc<SinkOutbox>>>,
    has_sinks: AtomicBool,
    /// Recorded and sampled-out roots, grouped by thread stripe.
    root_counts: RootCounters,
    /// Retain allocation identity in native TLS without owning the state.
    weak_self: Weak<CollectorState>,
}

impl CollectorState {
    /// Check the thread-local adaptive sampling counter.
    ///
    /// The unpressured path uses local arithmetic. A throttled producer checks
    /// for recycled buckets every 64 root attempts.
    #[inline]
    fn should_record(&self) -> bool {
        LOCAL.with(|cell| {
            let mut state = cell.borrow_mut();
            state.get_or_register(self).should_record()
        })
    }

    /// Submit a completed span.  Called by [`Span::drop`].
    ///
    /// Pushes to a thread-local `Vec` — zero atomics on the fast path.
    /// Batch boundaries publish bucket ownership with an atomic handoff.
    #[inline]
    pub(crate) fn submit(&self, span: CompletedSpan) {
        LOCAL.with(|cell| {
            let mut state = cell.borrow_mut();
            state.get_or_register(self).push(span);
        });
    }
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
    pub fn publish(&self, span: CompletedSpan) -> Result<(), Box<CompletedSpan>> {
        let shard = thread_id() & (self.inner.outboxes.len() - 1);
        self.publish_on_shard(shard, span)
    }

    #[inline]
    fn publish_on_shard(
        &self,
        shard: usize,
        span: CompletedSpan,
    ) -> Result<(), Box<CompletedSpan>> {
        let mut queue = self.inner.outboxes[shard].queue.lock();
        if queue.spans.len() < queue.quota {
            queue.spans.push(span);
            queue.accepted = queue.accepted.wrapping_add(1);
            return Ok(());
        }
        // Only exhausted shards consult the shared certificate. Ordinary
        // accepted publications touch no global atomic.
        if self.inner.budget.full.load(Ordering::SeqCst) {
            queue.rejected = queue.rejected.wrapping_add(1);
            drop(queue);
            return Err(Box::new(span));
        }
        // Never hold the original shard while acquiring the ordered lock set.
        drop(queue);
        self.publish_with_transfer(shard, span)
    }

    #[cold]
    fn publish_with_transfer(
        &self,
        shard: usize,
        span: CompletedSpan,
    ) -> Result<(), Box<CompletedSpan>> {
        // Allocate before locking. Every multi-shard operation takes locks in
        // this order, and never acquires the collector registry.
        let mut queues = Vec::with_capacity(self.inner.outboxes.len());
        for outbox in self.inner.outboxes.iter() {
            queues.push(outbox.queue.lock());
        }
        // A drain or another rebalance may have reopened this shard's quota
        // while its original guard was released.
        if queues[shard].spans.len() == queues[shard].quota {
            let mut remaining = SINK_QUOTA_CHUNK;
            // Prefer never-used shards over active producers between batches.
            // The second pass includes all other donors; counter wrap can only
            // affect priority, never capacity or admission.
            'donors: for never_used in [true, false] {
                for donor in 0..queues.len() {
                    if donor == shard || (queues[donor].accepted == 0) != never_used {
                        continue;
                    }
                    let unused = queues[donor].quota - queues[donor].spans.len();
                    let transferred = unused.min(remaining);
                    queues[donor].quota -= transferred;
                    queues[shard].quota += transferred;
                    remaining -= transferred;
                    if remaining == 0 {
                        break 'donors;
                    }
                }
            }
            if remaining == SINK_QUOTA_CHUNK {
                // Every quota is occupied. Because quotas sum to capacity,
                // this certifies actual global fullness while all locks are held.
                self.inner.budget.full.store(true, Ordering::SeqCst);
                queues[shard].rejected = queues[shard].rejected.wrapping_add(1);
                drop(queues);
                return Err(Box::new(span));
            }
        }
        // Keep the target locked through insertion so transferred quota cannot
        // be taken by another rebalance before this publication completes.
        let queue = &mut queues[shard];
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
    /// buffer automatically. Native queues use fixed flush and outbox limits.
    pub fn new(_num_shards: usize, _capacity_per_shard: usize) -> Self {
        Self {
            state: Arc::new_cyclic(|weak_self| CollectorState {
                native: Registry::new(),
                outboxes: Mutex::new(Vec::new()),
                has_sinks: AtomicBool::new(false),
                root_counts: RootCounters::new(),
                weak_self: weak_self.clone(),
            }),
        }
    }

    /// Borrow the stable state without incrementing its Arc count.
    #[inline]
    pub(crate) fn borrowed_state(&self) -> &CollectorState {
        &self.state
    }

    /// Register a bounded, owning sink for externally completed spans.
    ///
    /// Capacity bounds queued spans, not the size of individual spans. A sink's
    /// fixed producer shards remain registered until the collector is dropped,
    /// so reuse sinks rather than creating one per request. Producer thread exit
    /// creates no additional outboxes. Native submission is unchanged.
    /// Unused shard quotas are transferable: an exhausted shard rejects only
    /// when the sink's actual queued span count reaches the global capacity.
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
            full: AtomicBool::new(false),
        });
        let retained_capacity = capacity
            .div_ceil(shard_count)
            .max(SINK_RETAINED_BATCH)
            .min(capacity);
        let outboxes: Box<[_]> = (0..shard_count)
            .map(|shard| {
                Arc::new(SinkOutbox {
                    queue: CachePadded::new(Mutex::new(SinkQueue {
                        spans: Vec::new(),
                        quota: capacity / shard_count + usize::from(shard < capacity % shard_count),
                        accepted: 0,
                        rejected: 0,
                    })),
                    budget: Arc::clone(&budget),
                    retained_capacity,
                })
            })
            .collect();
        self.state
            .outboxes
            .lock()
            .extend(outboxes.iter().map(Arc::clone));
        self.state.has_sinks.store(true, Ordering::Release);
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
    ) -> Span<'_> {
        let collector_ref = CollectorRef::from_arc(self);
        if self.state.should_record() {
            self.state.root_counts.inc(ROOT_RECORDED);
            Span::new_root(name, kind, collector_ref)
        } else {
            self.state.root_counts.inc(ROOT_SAMPLED_OUT);
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
    ) -> Span<'_> {
        let collector_ref = CollectorRef::from_arc(self);
        if !self.state.should_record() {
            self.state.root_counts.inc(ROOT_SAMPLED_OUT);
            return Span::noop(collector_ref);
        }
        self.state.root_counts.inc(ROOT_RECORDED);
        match traceparent.and_then(SpanContext::from_traceparent) {
            Some(remote_ctx) => Span::new_from_remote(name, kind, remote_ctx, collector_ref),
            None => Span::new_root(name, kind, collector_ref),
        }
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
            let mut state = cell.borrow_mut();
            let key = Arc::as_ptr(&self.state) as usize;
            if let Some(pos) = state.entries.iter().position(|(k, _)| *k == key) {
                state.entries[pos].1.flush();
            }
        });
    }

    /// Drain published spans into one flat vector, appending to existing data.
    ///
    /// Use [`drain_batches`](Self::drain_batches) to preserve contiguous producer
    /// buffers without copying spans. This compatibility API flattens batches;
    /// the first batch exchanges storage when the destination is empty.
    pub fn drain_into(&self, buf: &mut Vec<CompletedSpan>) {
        self.state.native.visit(|outbox| {
            outbox.drain(|spans| {
                if buf.is_empty() {
                    std::mem::swap(buf, spans);
                } else {
                    buf.append(spans);
                }
            })
        });
        if self.state.has_sinks.load(Ordering::Acquire) {
            for outbox in self.state.outboxes.lock().iter() {
                outbox.drain_into(buf);
            }
        }
    }

    /// Transfer whole contiguous native buffers without copying their spans.
    ///
    /// Reuse a [`SpanBatch`] across export cycles: clear its payloads, call this
    /// method, then encode directly from its buffers. Each native handoff swaps
    /// a vector descriptor and returns an empty allocation to the producer. At
    /// most two published buckets per producer are visited per call. Unflushed
    /// local spans require [`flush_local`](Self::flush_local) or thread exit.
    /// External adapter spans are appended through their existing shard locks.
    pub fn drain_batches(&self, batch: &mut SpanBatch) {
        self.state
            .native
            .visit(|outbox| outbox.drain(|spans| batch.take(spans)));
        if self.state.has_sinks.load(Ordering::Acquire) {
            for outbox in self.state.outboxes.lock().iter() {
                outbox.drain_into(batch.sink_buffer());
                batch.finish_sink();
            }
        }
    }

    /// Completed native spans rejected when both per-thread buckets are busy
    /// or full. Adaptive sampling and adapter rejections are counted separately.
    pub fn dropped_count(&self) -> u64 {
        let mut total = 0u64;
        self.state
            .native
            .visit(|outbox| total = total.wrapping_add(outbox.dropped()));
        total
    }

    /// Total spans that passed adaptive sampling and were recorded.
    pub fn recorded_count(&self) -> u64 {
        self.state.root_counts.sum(ROOT_RECORDED) as u64
    }

    /// Total spans that were dropped by adaptive sampling.
    pub fn sampled_out_count(&self) -> u64 {
        self.state.root_counts.sum(ROOT_SAMPLED_OUT) as u64
    }

    /// Current number of spans waiting across all outboxes.
    ///
    /// Does not include spans still in thread-local buffers that haven't
    /// been flushed yet.
    pub fn len(&self) -> usize {
        let mut total = 0;
        self.state.native.visit(|outbox| total += outbox.len());
        if self.state.has_sinks.load(Ordering::Acquire) {
            total += self
                .state
                .outboxes
                .lock()
                .iter()
                .map(|o| o.queue.lock().spans.len())
                .sum::<usize>();
        }
        total
    }

    /// Whether no published spans are waiting. Concurrent reads are snapshots;
    /// spans in a producer's active bucket are excluded until publication.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SpanId, SpanStatus, TraceId};

    #[test]
    fn native_sampling_recovers_within_64_attempts_after_export() {
        let collector = Arc::new(SpanCollector::new(1, 4096));
        let mut local = ThreadLocalState::new();
        let buffer = local.get_or_register(collector.borrowed_state());
        for _ in 0..2048 {
            buffer.push(completed_span("pressure"));
        }
        buffer.flush();
        assert!(buffer.sample_shift > 0);
        let mut batch = SpanBatch::new();
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), 2048);
        for _ in 0..64 {
            buffer.should_record();
        }
        assert_eq!(buffer.sample_shift, 0);
        for _ in 0..64 {
            assert!(buffer.should_record());
        }
    }

    #[test]
    fn batch_drain_appends_native_and_adapter_data_and_reuses_after_clear() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        let sink = collector.span_sink(4);
        let mut batch = SpanBatch::new();
        for _ in 0..4 {
            drop(collector.start_span("native", SpanKind::Internal));
            collector.flush_local();
            assert!(sink.publish(completed_span("adapter")).is_ok());
            collector.drain_batches(&mut batch);
        }
        assert_eq!(batch.len(), 8);
        assert_eq!(batch.iter().filter(|s| s.name == "native").count(), 4);
        assert_eq!(batch.iter().filter(|s| s.name == "adapter").count(), 4);
        assert!(collector.is_empty());
        batch.clear();
        collector.drain_batches(&mut batch);
        assert!(batch.is_empty());
    }

    fn completed_span(name: &'static str) -> CompletedSpan {
        CompletedSpan {
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            parent_span_id: SpanId::INVALID,
            name: Cow::Borrowed(name),
            kind: SpanKind::Internal,
            start_time_ns: 1,
            end_time_ns: 2,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        }
    }

    fn assert_sink_invariants(sink: &SpanSink, expected_queued: usize) {
        let mut queues = Vec::with_capacity(sink.inner.outboxes.len());
        for outbox in sink.inner.outboxes.iter() {
            queues.push(outbox.queue.lock());
        }
        assert_eq!(
            queues.iter().map(|queue| queue.quota).sum::<usize>(),
            sink.capacity()
        );
        assert!(queues.iter().all(|queue| queue.spans.len() <= queue.quota));
        assert_eq!(
            queues.iter().map(|queue| queue.spans.len()).sum::<usize>(),
            expected_queued
        );
        if sink.inner.budget.full.load(Ordering::SeqCst) {
            assert_eq!(expected_queued, sink.capacity());
        }
    }

    /// Helper: flush + drain for same-thread tests.
    fn flush_and_drain(collector: &SpanCollector, buf: &mut Vec<CompletedSpan>) {
        collector.flush_local();
        collector.drain_into(buf);
    }

    #[test]
    fn grouped_root_counters_preserve_independent_totals_under_contention() {
        let counters = RootCounters::new();
        std::thread::scope(|scope| {
            for _ in 0..64 {
                let counters = &counters;
                scope.spawn(move || {
                    for _ in 0..1000 {
                        counters.inc(ROOT_RECORDED);
                        counters.inc(ROOT_SAMPLED_OUT);
                        counters.inc(ROOT_SAMPLED_OUT);
                    }
                });
            }
        });
        assert_eq!(counters.sum(ROOT_RECORDED), 64_000);
        assert_eq!(counters.sum(ROOT_SAMPLED_OUT), 128_000);
    }

    #[test]
    fn grouped_root_counters_share_padding_and_keep_signed_wrap_behavior() {
        let counters = RootCounters::new();
        let paired = std::mem::size_of_val(counters.rows.as_ref());
        let separate = 2 * ROOT_COUNTER_SHARDS * std::mem::size_of::<CachePadded<AtomicIsize>>();
        assert!(paired <= separate);
        let row = &counters.rows[thread_id() & (ROOT_COUNTER_SHARDS - 1)];
        row[ROOT_RECORDED].store(isize::MAX, Ordering::Relaxed);
        counters.inc(ROOT_RECORDED);
        assert_eq!(counters.sum(ROOT_RECORDED), isize::MIN);
        assert_eq!(counters.sum(ROOT_SAMPLED_OUT), 0);
    }

    #[test]
    fn native_flush_survives_unwrapping_collector_owner() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        let span = collector.start_span("before move", SpanKind::Internal);
        let id = span.span_id();
        drop(span);
        let collector = match Arc::try_unwrap(collector) {
            Ok(collector) => collector,
            Err(_) => panic!("native spans must not retain a strong owner"),
        };
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].span_id, id);
        assert_eq!(collector.recorded_count(), 1);
    }

    #[test]
    fn native_replacement_at_same_address_registers_a_distinct_buffer() {
        let mut collector = Arc::new(SpanCollector::new(1, 64));
        drop(collector.start_span("first", SpanKind::Internal));
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "first");
        spans.clear();
        let allocation = Arc::as_ptr(&collector);
        *Arc::get_mut(&mut collector).expect("native TLS must not retain an outer Weak") =
            SpanCollector::new(1, 64);
        assert_eq!(Arc::as_ptr(&collector), allocation);
        drop(collector.start_span("replacement", SpanKind::Internal));
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "replacement");
        assert_eq!(collector.recorded_count(), 1);
        assert_eq!(collector.sampled_out_count(), 0);
    }

    #[test]
    fn native_tls_retains_identity_and_releases_dead_collector_buffers() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        let mut state = ThreadLocalState::new();
        let outbox = Arc::downgrade(
            &state
                .get_or_register(collector.borrowed_state())
                .writer
                .outbox,
        );
        assert_eq!(Arc::weak_count(&collector.state), 2);
        let address = Arc::as_ptr(&collector.state);
        drop(collector);
        assert!(outbox.upgrade().is_some());

        // The TLS Weak keeps the old allocation's address unavailable for reuse
        // until registration removes the dead entry.
        let replacement = Arc::new(SpanCollector::new(1, 64));
        assert_ne!(Arc::as_ptr(&replacement.state), address);
        assert!(
            state
                .get_or_register(replacement.borrowed_state())
                .should_record()
        );
        assert!(outbox.upgrade().is_none());
        assert_eq!(state.entries.len(), 1);
        let mut registered = 0;
        replacement.state.native.visit(|_| registered += 1);
        assert_eq!(registered, 1);
        assert_eq!(Arc::weak_count(&replacement.state), 2);
    }

    #[test]
    fn native_tls_pruning_preserves_live_collector_sampling_and_payloads() {
        let live = Arc::new(SpanCollector::new(1, 64));
        let discarded = Arc::new(SpanCollector::new(1, 64));
        let mut state = ThreadLocalState::new();
        let live_buffer = state.get_or_register(live.borrowed_state());
        live_buffer.sample_shift = 3;
        live_buffer.span_counter = 7;
        live_buffer.push(completed_span("live"));
        state.get_or_register(discarded.borrowed_state());
        drop(discarded);

        let replacement = Arc::new(SpanCollector::new(1, 64));
        state.get_or_register(replacement.borrowed_state());
        assert_eq!(state.entries.len(), 2);
        let live_buffer = state.get_or_register(live.borrowed_state());
        assert_eq!(live_buffer.sample_shift, 3);
        assert!(live_buffer.should_record());
        assert_eq!(live_buffer.span_counter, 8);
        let mut registered = 0;
        live.state.native.visit(|_| registered += 1);
        assert_eq!(registered, 1);
        drop(state);
        let mut spans = Vec::new();
        live.drain_into(&mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "live");
    }

    #[test]
    fn root_counts_are_visible_before_flush_and_survive_worker_exit() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        let sink = collector.span_sink(4);
        assert!(sink.publish(completed_span("external")).is_ok());
        assert_eq!(collector.recorded_count(), 0);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            let worker_collector = Arc::clone(&collector);
            let worker = scope.spawn(move || {
                let root = worker_collector.start_span("root", SpanKind::Internal);
                let child = root.child("child", SpanKind::Internal);
                let remote = worker_collector.start_span_from_traceparent(
                    Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                    "remote",
                    SpanKind::Server,
                );
                ready_tx.send(()).expect("publish readiness");
                release_rx.recv().expect("wait for count snapshot");
                drop((child, remote, root));
            });
            ready_rx.recv().expect("worker has active spans");
            let counts = (
                collector.recorded_count(),
                collector.sampled_out_count(),
                collector.len(),
            );
            release_tx.send(()).expect("release worker");
            worker.join().expect("join native producer");
            assert_eq!(counts.0, 2);
            assert_eq!(counts.1, 0);
            // Only the sink publication is queued; every native span is active.
            assert_eq!(counts.2, 1);
        });
        let mut spans = Vec::new();
        collector.drain_into(&mut spans);
        assert_eq!(spans.len(), 4);
        assert_eq!(collector.recorded_count(), 2);
        assert_eq!(collector.sampled_out_count(), 0);
    }

    #[test]
    fn reentrant_native_access_rejects_aliases_and_recovers_after_unwind() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        drop(collector.start_span("warmup", SpanKind::Internal));
        collector.flush_local();
        LOCAL.with(|cell| {
            let _exclusive = cell.borrow_mut();
            let flush = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                collector.flush_local();
            }));
            assert!(flush.is_err());
            let root = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                drop(collector.start_span("reentrant", SpanKind::Internal));
            }));
            assert!(root.is_err());
        });
        drop(collector.start_span("after unwind", SpanKind::Internal));
        collector.flush_local();
        let mut spans = Vec::new();
        collector.drain_into(&mut spans);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].name, "warmup");
        assert_eq!(spans[1].name, "after unwind");
        assert_eq!(collector.recorded_count(), 2);
        assert_eq!(collector.sampled_out_count(), 0);
        assert_eq!(collector.dropped_count(), 0);
    }

    #[test]
    fn both_root_constructors_publish_sampled_and_recorded_totals() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        LOCAL.with(|cell| {
            let mut state = cell.borrow_mut();
            state
                .get_or_register(collector.borrowed_state())
                .sample_shift = 1;
        });
        for attempt in 0_u64..8 {
            let root = if attempt % 4 < 2 {
                collector.start_span("root", SpanKind::Internal)
            } else {
                collector.start_span_from_traceparent(
                    Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
                    "remote",
                    SpanKind::Server,
                )
            };
            let _child = root.child("child", SpanKind::Internal);
            let decisions = attempt + 1;
            assert_eq!(collector.recorded_count(), decisions / 2);
            assert_eq!(collector.sampled_out_count(), decisions.div_ceil(2));
            assert_eq!(root.trace_id().is_invalid(), attempt % 2 == 0);
        }
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 8);
        assert_eq!(spans.iter().filter(|span| span.name == "root").count(), 2);
        assert_eq!(spans.iter().filter(|span| span.name == "remote").count(), 2);
        assert_eq!(collector.recorded_count(), 4);
        assert_eq!(collector.sampled_out_count(), 4);
    }

    #[test]
    fn producer_root_totals_accumulate_across_threads_and_drains() {
        let collector = Arc::new(SpanCollector::new(1, 64));
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..8 {
                let collector = Arc::clone(&collector);
                workers.push(scope.spawn(move || {
                    for _ in 0..128 {
                        let root = collector.start_span("root", SpanKind::Internal);
                        let _child = root.child("child", SpanKind::Internal);
                    }
                }));
            }
            for worker in workers {
                worker.join().expect("join native producer");
            }
        });
        assert_eq!(collector.recorded_count(), 1024);
        assert_eq!(collector.sampled_out_count(), 0);
        let mut spans = Vec::new();
        collector.drain_into(&mut spans);
        assert_eq!(spans.len(), 2048);
        assert_eq!(
            spans
                .iter()
                .filter(|span| span.parent_span_id.is_invalid())
                .count(),
            1024
        );
        spans.clear();
        collector.drain_into(&mut spans);
        assert!(spans.is_empty());
        assert_eq!(collector.recorded_count(), 1024);
        assert_eq!(collector.sampled_out_count(), 0);
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
    fn sink_initial_quotas_cover_tiny_nonpower_and_chunk_boundaries() {
        for capacity in [
            1,
            2,
            3,
            17,
            31,
            32,
            33,
            SINK_QUOTA_CHUNK - 1,
            SINK_QUOTA_CHUNK,
            SINK_QUOTA_CHUNK + 1,
            SINK_QUOTA_CHUNK * 2 + 1,
            usize::MAX,
        ] {
            let collector = Arc::new(SpanCollector::new(1, 1));
            let sink = collector.span_sink(capacity);
            assert_sink_invariants(&sink, 0);
            assert!(
                sink.inner
                    .outboxes
                    .iter()
                    .all(|outbox| outbox.queue.lock().spans.capacity() == 0)
            );
        }
    }

    #[test]
    fn sink_transfer_preserves_occupied_quota_and_prefers_never_used_donors() {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(SINK_SHARDS * 4);
        assert!(sink.publish_on_shard(0, completed_span("donor")).is_ok());
        let donor_quota = sink.inner.outboxes[0].queue.lock().quota;
        let target = SINK_SHARDS - 1;
        let target_quota = sink.inner.outboxes[target].queue.lock().quota;
        for _ in 0..target_quota {
            assert!(
                sink.publish_on_shard(target, completed_span("target"))
                    .is_ok()
            );
        }
        assert!(
            sink.publish_on_shard(target, completed_span("transferred"))
                .is_ok()
        );
        assert_eq!(sink.inner.outboxes[0].queue.lock().quota, donor_quota);
        let added_quota = sink.inner.outboxes[target].queue.lock().quota - target_quota;
        assert!(added_quota > 0 && added_quota <= SINK_QUOTA_CHUNK);
        assert_sink_invariants(&sink, target_quota + 2);
        let mut spans = Vec::new();
        collector.drain_into(&mut spans);
        assert_eq!(spans.len(), target_quota + 2);
        assert_eq!(spans.iter().filter(|span| span.name == "donor").count(), 1);
        assert_eq!(sink.rejected_count(), 0);
    }

    #[test]
    fn sink_full_certificate_is_invalidated_before_donor_drain_and_migration() {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(3);
        assert_eq!(sink.inner.outboxes[3].queue.lock().quota, 0);
        for shard in 0..3 {
            assert!(
                sink.publish_on_shard(shard, completed_span("initial"))
                    .is_ok()
            );
        }
        let mut rejected = completed_span("full");
        rejected.name = Cow::Owned("owned rejected payload".to_owned());
        let name_ptr = rejected.name.as_ptr();
        let rejected = sink.publish_on_shard(3, rejected).unwrap_err();
        assert_eq!(rejected.name.as_ptr(), name_ptr);
        assert!(sink.inner.budget.full.load(Ordering::SeqCst));

        let mut spans = vec![completed_span("existing")];
        for (round, (donor, target)) in [(0, 3), (1, 0)].into_iter().enumerate() {
            assert_eq!(sink.inner.outboxes[target].queue.lock().quota, 0);
            // This is a nonempty donor, different from the exhausted target.
            // The output already contains spans and must remain append-only.
            sink.inner.outboxes[donor].drain_into(&mut spans);
            assert_eq!(spans.len(), round + 2);
            assert_eq!(spans[0].name, "existing");
            assert!(!sink.inner.budget.full.load(Ordering::SeqCst));
            assert!(
                sink.publish_on_shard(target, completed_span("migrated"))
                    .is_ok()
            );
            assert_sink_invariants(&sink, 3);
            // Only active donors have slack, so migration also needs the
            // second donor pass after the never-used preference finds none.
            assert!(
                sink.publish_on_shard(donor, completed_span("full again"))
                    .is_err()
            );
            assert!(sink.inner.budget.full.load(Ordering::SeqCst));
            // An empty outbox has no capacity to free and retains the certificate.
            sink.inner.outboxes[donor].drain_into(&mut spans);
            assert!(sink.inner.budget.full.load(Ordering::SeqCst));
        }
        collector.drain_into(&mut spans);
        assert_eq!(spans.len(), 6); // Five accepted spans plus the existing output.
        assert_eq!(sink.accepted_count(), 5);
        assert_eq!(sink.rejected_count(), 3);
        assert_sink_invariants(&sink, 0);
    }

    #[test]
    fn sink_rechecks_target_freed_before_ordered_lock_acquisition() {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(4);
        for shard in 0..4 {
            assert!(
                sink.publish_on_shard(shard, completed_span("initial"))
                    .is_ok()
            );
        }
        assert!(sink.publish_on_shard(0, completed_span("full")).is_err());
        // Reproduce the gap between releasing the exhausted target's original
        // guard and acquiring the complete ordered set. Every other quota stays
        // occupied, so donor-only admission would incorrectly reject this span.
        let mut spans = vec![completed_span("existing")];
        sink.inner.outboxes[0].drain_into(&mut spans);
        assert!(
            sink.publish_with_transfer(0, completed_span("rechecked"))
                .is_ok()
        );
        assert_eq!(spans.len(), 2);
        assert!(
            sink.inner
                .outboxes
                .iter()
                .all(|outbox| outbox.queue.lock().quota == 1)
        );
        assert_eq!(sink.accepted_count(), 5);
        assert_eq!(sink.rejected_count(), 1);
        assert_sink_invariants(&sink, 4);
    }

    #[test]
    fn sink_full_certificate_rejects_without_locking_other_shards() {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(3);
        for shard in 0..3 {
            assert!(
                sink.publish_on_shard(shard, completed_span("initial"))
                    .is_ok()
            );
        }
        assert!(sink.publish_on_shard(3, completed_span("prime")).is_err());
        let held_donor = sink.inner.outboxes[0].queue.lock();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let worker_sink = sink.clone();
        let worker = std::thread::spawn(move || {
            let rejected = worker_sink.publish_on_shard(3, completed_span("rejected"));
            ready_tx.send(rejected.is_err()).expect("notify rejection");
        });
        let result = ready_rx.recv_timeout(std::time::Duration::from_secs(5));
        // Release even on timeout so a broken fallback can finish before the
        // assertion reports failure, instead of leaving a blocked worker.
        drop(held_donor);
        worker.join().expect("publisher thread");
        assert!(result.expect("certificate rejection must not wait for donor"));
        assert_eq!(sink.accepted_count(), 3);
        assert_eq!(sink.rejected_count(), 2);
        assert_sink_invariants(&sink, 3);
    }

    #[test]
    fn sink_storage_is_lazy_and_bounded_across_short_lived_producers() {
        let tiny_collector = Arc::new(SpanCollector::new(1, 1));
        let tiny = tiny_collector.span_sink(1);
        assert_eq!(tiny.inner.outboxes.len(), 1);
        assert_eq!(tiny.inner.outboxes[0].queue.lock().spans.capacity(), 0);

        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(4096);
        let registered = collector.state.outboxes.lock().len();
        assert!(registered <= SINK_SHARDS);
        assert!(
            sink.inner
                .outboxes
                .iter()
                .all(|outbox| outbox.queue.lock().spans.capacity() == 0)
        );
        let mut spans = Vec::new();
        for _ in 0..SINK_SHARDS * 2 {
            let sink = sink.clone();
            std::thread::spawn(move || {
                // Grow past the reusable allocation target on each producer.
                for _ in 0..512 {
                    assert!(sink.publish(completed_span("short lived")).is_ok());
                }
            })
            .join()
            .expect("producer thread");
            collector.drain_into(&mut spans);
            assert_eq!(spans.len(), 512);
            spans.clear();
        }
        assert_eq!(collector.state.outboxes.lock().len(), registered);
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

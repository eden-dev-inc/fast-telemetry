//! Single-producer double buffers. Only ownership tags are shared; span storage
//! is accessed exclusively by either its producer or one draining consumer.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use crossbeam_utils::CachePadded;

use super::types::CompletedSpan;

pub(super) const BATCH_SIZE: usize = 256;
pub(super) const BUCKET_CAPACITY: usize = 4096;
const FREE: u64 = 0;
const WRITING: u64 = 1;
const READY: u64 = 2;
const READING: u64 = 3;
const STATE_MASK: u64 = 3;
const LENGTH_SHIFT: u32 = 2;
const SEQUENCE_SHIFT: u32 = 15;
const LENGTH_MASK: u64 = (1 << (SEQUENCE_SHIFT - LENGTH_SHIFT)) - 1;
const SEQUENCE_MASK: u64 = (1 << (64 - SEQUENCE_SHIFT)) - 1;

fn ready_tag(sequence: u64, len: usize) -> u64 {
    ((sequence & SEQUENCE_MASK) << SEQUENCE_SHIFT) | ((len as u64) << LENGTH_SHIFT) | READY
}

fn ready_len(tag: u64) -> usize {
    if tag & STATE_MASK == READY {
        ((tag >> LENGTH_SHIFT) & LENGTH_MASK) as usize
    } else {
        0
    }
}

fn older(a: u64, b: u64) -> bool {
    let distance = ((b >> SEQUENCE_SHIFT).wrapping_sub(a >> SEQUENCE_SHIFT)) & SEQUENCE_MASK;
    distance != 0 && distance < (1 << (63 - SEQUENCE_SHIFT))
}

struct Bucket {
    control: CachePadded<AtomicU64>,
    spans: UnsafeCell<Vec<CompletedSpan>>,
}

// SAFETY: FREE -> WRITING gives the sole producer exclusive access. READY ->
// READING gives one consumer exclusive access. Acquire CAS pairs with Release
// publication/recycling; no vector reference survives a state transition.
unsafe impl Sync for Bucket {}

impl Bucket {
    fn new(state: u64) -> Self {
        Self {
            control: CachePadded::new(AtomicU64::new(state)),
            spans: UnsafeCell::new(Vec::with_capacity(BATCH_SIZE)),
        }
    }
}

pub(super) struct Outbox {
    buckets: [Bucket; 2],
    pub(super) recycled: CachePadded<AtomicU64>,
    dropped: AtomicU64,
}

impl Outbox {
    pub(super) fn new() -> Self {
        Self {
            buckets: [Bucket::new(WRITING), Bucket::new(FREE)],
            recycled: CachePadded::new(AtomicU64::new(0)),
            dropped: AtomicU64::new(0),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.buckets
            .iter()
            .map(|b| ready_len(b.control.load(Ordering::Acquire)))
            .sum()
    }

    pub(super) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    pub(super) fn drain(&self, mut consume: impl FnMut(&mut Vec<CompletedSpan>)) {
        // Snapshot at most two batches. An active producer cannot keep this
        // drain running indefinitely or starve other producers' outboxes.
        let tags = self
            .buckets
            .each_ref()
            .map(|b| b.control.load(Ordering::Acquire));
        let order = if tags[1] & STATE_MASK == READY
            && (tags[0] & STATE_MASK != READY || older(tags[1], tags[0]))
        {
            [1, 0]
        } else {
            [0, 1]
        };
        for index in order {
            let tag = tags[index];
            let bucket = &self.buckets[index];
            if tag & STATE_MASK != READY
                || bucket
                    .control
                    .compare_exchange(
                        tag,
                        (tag & !STATE_MASK) | READING,
                        Ordering::Acquire,
                        Ordering::Relaxed,
                    )
                    .is_err()
            {
                continue;
            }
            let guard = ReadGuard {
                bucket,
                outbox: self,
                version: tag,
            };
            // SAFETY: this successful CAS owns READING until guard drops. The
            // callback is private and must consume/swap every span before return.
            consume(unsafe { &mut *bucket.spans.get() });
            drop(guard);
        }
    }
}

struct ReadGuard<'a> {
    bucket: &'a Bucket,
    outbox: &'a Outbox,
    version: u64,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard still exclusively owns READING, even on unwind.
        let spans = unsafe { &mut *self.bucket.spans.get() };
        spans.clear();
        // Callers may supply a very large flat destination for exchange. Keep
        // memory returned to each producer bounded independently of that size.
        if spans.capacity() > BUCKET_CAPACITY {
            spans.shrink_to(BUCKET_CAPACITY);
        }
        self.bucket.control.store(FREE, Ordering::Release);
        self.outbox.recycled.store(self.version, Ordering::Relaxed);
    }
}

/// One writer, confined to its registering OS thread. There are no atomic
/// ownership operations between batch boundaries on the ordinary push path.
pub(super) struct Writer {
    pub(super) outbox: Arc<Outbox>,
    active: Option<usize>,
    next_flush: usize,
    sequence: u64,
    limit: usize,
    buffer: Vec<CompletedSpan>,
}

impl Writer {
    pub(super) fn new() -> Self {
        let outbox = Arc::new(Outbox::new());
        // SAFETY: the fresh outbox starts with bucket zero exclusively WRITING. Keep
        // its vector descriptor in TLS throughout that ownership interval.
        let buffer = unsafe { std::mem::take(&mut *outbox.buckets[0].spans.get()) };
        Self {
            outbox,
            active: Some(0),
            next_flush: BATCH_SIZE,
            sequence: 0,
            limit: BUCKET_CAPACITY,
            buffer,
        }
    }

    fn claim(&mut self, index: usize, expected: u64) -> bool {
        if self.outbox.buckets[index]
            .control
            .compare_exchange(expected, WRITING, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }
        // SAFETY: successful ownership acquisition excludes every consumer.
        // Called only after publishing the previous local allocation.
        debug_assert!(self.buffer.is_empty() && self.buffer.capacity() == 0);
        self.buffer = unsafe { std::mem::take(&mut *self.outbox.buckets[index].spans.get()) };
        true
    }

    fn acquire(&mut self) -> bool {
        for index in 0..2 {
            if self.claim(index, FREE) {
                self.active = Some(index);
                self.limit = BUCKET_CAPACITY
                    - ready_len(
                        self.outbox.buckets[1 - index]
                            .control
                            .load(Ordering::Acquire),
                    );
                self.next_flush = BATCH_SIZE.min(self.limit);
                return true;
            }
        }
        // Explicit small flushes can leave both buckets READY. Extend only the
        // newest batch, and republish it before returning from push. This keeps
        // already published data visible while a newly started span is alive.
        let tags = self
            .outbox
            .buckets
            .each_ref()
            .map(|b| b.control.load(Ordering::Acquire));
        let index = if tags[1] & STATE_MASK == READY
            && (tags[0] & STATE_MASK != READY || older(tags[0], tags[1]))
        {
            1
        } else {
            0
        };
        let tag = tags[index];
        let limit = BUCKET_CAPACITY - ready_len(tags[1 - index]);
        if ready_len(tag) < limit && tag & STATE_MASK == READY && self.claim(index, tag) {
            self.active = Some(index);
            self.next_flush = 0;
            self.limit = limit;
            return true;
        }
        false
    }

    #[inline]
    pub(super) fn push(&mut self, span: CompletedSpan) -> Option<usize> {
        if self.active.is_none() && !self.acquire() {
            self.outbox.dropped.fetch_add(1, Ordering::Relaxed);
            return Some(BUCKET_CAPACITY * 2);
        }
        let index = self.active.expect("writer acquired a bucket");
        if self.buffer.len() >= self.limit {
            self.limit = BUCKET_CAPACITY
                - ready_len(
                    self.outbox.buckets[1 - index]
                        .control
                        .load(Ordering::Acquire),
                );
            if self.buffer.len() >= self.limit {
                self.outbox.dropped.fetch_add(1, Ordering::Relaxed);
                return Some(BUCKET_CAPACITY);
            }
        }
        self.buffer.push(span);
        if self.buffer.len() >= self.next_flush {
            self.flush_boundary(false)
        } else {
            None
        }
    }

    fn publish(&mut self, index: usize) {
        let len = self.buffer.len();
        // SAFETY: WRITING belongs to this writer. Publish the vector descriptor
        // only after moving its full allocation out of thread-local storage.
        unsafe {
            *self.outbox.buckets[index].spans.get() = std::mem::take(&mut self.buffer);
        }
        self.sequence = self.sequence.wrapping_add(1) & SEQUENCE_MASK;
        self.outbox.buckets[index]
            .control
            .store(ready_tag(self.sequence, len), Ordering::Release);
    }

    fn flush_boundary(&mut self, force: bool) -> Option<usize> {
        let index = self.active?;
        let len = self.buffer.len();
        if len == 0 {
            return None;
        }
        let other = 1 - index;
        let other_tag = self.outbox.buckets[other].control.load(Ordering::Acquire);
        self.limit = BUCKET_CAPACITY - ready_len(other_tag);
        let occupancy = len + ready_len(other_tag);
        // Inspecting the other tag is advisory. Publish before claiming so
        // there is exactly one local active allocation, including CAS failure.
        if self.next_flush == 0 || force || len >= self.limit || other_tag == FREE {
            self.publish(index);
            self.active = if self.claim(other, FREE) {
                Some(other)
            } else {
                None
            };
            self.limit = BUCKET_CAPACITY - len;
            self.next_flush = BATCH_SIZE.min(self.limit);
        } else {
            self.next_flush = (len + BATCH_SIZE).min(self.limit);
        }
        Some(occupancy)
    }

    pub(super) fn flush(&mut self) -> Option<usize> {
        self.flush_boundary(true)
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Immutable nodes remain alive until the collector is exclusively dropped.
/// Readers borrow that collector, so registration never needs a drain mutex or
/// Arc increments per outbox. Thread exit leaves published buckets drainable.
pub(super) struct Registry {
    head: AtomicPtr<Node>,
}

struct Node {
    outbox: Arc<Outbox>,
    next: *mut Node,
}

impl Registry {
    pub(super) fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
        }
    }

    pub(super) fn register(&self, outbox: Arc<Outbox>) {
        let node = Box::into_raw(Box::new(Node {
            outbox,
            next: ptr::null_mut(),
        }));
        let mut head = self.head.load(Ordering::Relaxed);
        loop {
            // SAFETY: node is private until the successful Release publication.
            unsafe {
                (*node).next = head;
            }
            match self
                .head
                .compare_exchange_weak(head, node, Ordering::Release, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(new_head) => head = new_head,
            }
        }
    }

    pub(super) fn visit(&self, mut f: impl FnMut(&Outbox)) {
        let head = self.head.load(Ordering::Acquire);
        if head.is_null() {
            return;
        }
        // SAFETY: immutable node retained by this borrowed registry.
        let first = unsafe { &*head };
        if first.next.is_null() {
            f(&first.outbox);
            return;
        }
        // Reverse the immutable snapshot to preserve producer registration
        // order. Ordinary worker pools require no temporary allocation.
        let mut local = [None; 64];
        let mut overflow = Vec::new();
        let mut len = 0;
        let mut node = head;
        while !node.is_null() {
            // SAFETY: published nodes are immutable and retained while self is
            // borrowed. Drop cannot race a reader of the owning collector.
            let entry = unsafe { &*node };
            if len < local.len() {
                local[len] = Some(entry.outbox.as_ref());
            } else {
                overflow.push(entry.outbox.as_ref());
            }
            len += 1;
            node = entry.next;
        }
        for outbox in overflow.into_iter().rev() {
            f(outbox);
        }
        for outbox in local[..len.min(local.len())].iter().rev().flatten() {
            f(outbox);
        }
    }
}

impl Drop for Registry {
    fn drop(&mut self) {
        let mut node = *self.head.get_mut();
        while !node.is_null() {
            // SAFETY: exclusive drop, and each published Box is freed once.
            let entry = unsafe { Box::from_raw(node) };
            node = entry.next;
        }
    }
}

/// Reusable export storage that preserves each producer's contiguous bucket.
/// Draining swaps vector descriptors rather than moving individual spans.
#[derive(Default)]
pub struct SpanBatch {
    buffers: Vec<Vec<CompletedSpan>>,
    used: usize,
}

impl SpanBatch {
    /// Create an empty batch. Reuse it across export cycles.
    pub fn new() -> Self {
        Self::default()
    }

    /// Iterate through contiguous producer buffers in drain order.
    pub fn buffers(&self) -> impl Iterator<Item = &[CompletedSpan]> {
        self.buffers[..self.used].iter().map(Vec::as_slice)
    }

    /// Read span payloads sequentially without flattening or allocating.
    pub fn iter(&self) -> impl Iterator<Item = &CompletedSpan> + Clone {
        self.buffers[..self.used].iter().flat_map(|b| b.iter())
    }

    /// Number of completed spans in this batch.
    pub fn len(&self) -> usize {
        self.buffers().map(<[CompletedSpan]>::len).sum()
    }

    /// Whether this batch contains no completed spans.
    pub fn is_empty(&self) -> bool {
        self.used == 0
    }

    /// Drop payloads while retaining bounded empty buffers for the next drain.
    pub fn clear(&mut self) {
        for buffer in &mut self.buffers[..self.used] {
            let first_owned = buffer.iter().position(|span| {
                !(span.attributes.capacity() == 0
                    && span.events.capacity() == 0
                    && matches!(span.name, std::borrow::Cow::Borrowed(_))
                    && !matches!(
                        &span.status,
                        super::types::SpanStatus::Error {
                            message: std::borrow::Cow::Owned(_)
                        }
                    ))
            });
            match first_owned {
                None => buffer.drain(..).for_each(std::mem::forget),
                Some(0) => buffer.clear(),
                Some(prefix) => {
                    // This prefix owns no names, status strings, metadata
                    // allocations, or nested payloads. Forget its redundant
                    // drop glue; Drain drops the remaining suffix in place.
                    // Draining the full range leaves no tail to move. Check
                    // capacity: an empty vector can still own storage.
                    let mut drain = buffer.drain(..);
                    drain.by_ref().take(prefix).for_each(std::mem::forget);
                }
            }
        }
        self.used = 0;
    }

    pub(super) fn take(&mut self, spans: &mut Vec<CompletedSpan>) {
        if spans.is_empty() {
            return;
        }
        if self.used == self.buffers.len() {
            self.buffers.push(Vec::with_capacity(BATCH_SIZE));
        }
        std::mem::swap(&mut self.buffers[self.used], spans);
        self.used += 1;
    }

    pub(super) fn sink_buffer(&mut self) -> &mut Vec<CompletedSpan> {
        if self.used == self.buffers.len() {
            self.buffers.push(Vec::new());
        }
        &mut self.buffers[self.used]
    }

    pub(super) fn finish_sink(&mut self) {
        if self.buffers.get(self.used).is_some_and(|b| !b.is_empty()) {
            self.used += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SpanId, SpanKind, SpanStatus, TraceId};
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    fn span(index: usize) -> CompletedSpan {
        CompletedSpan {
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            parent_span_id: SpanId::INVALID,
            name: index.to_string().into(),
            kind: SpanKind::Internal,
            start_time_ns: 1,
            end_time_ns: 2,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        }
    }

    fn drain(outbox: &Outbox, batch: &mut SpanBatch) {
        outbox.drain(|spans| batch.take(spans));
    }

    #[test]
    fn native_handoff_swaps_allocations_and_keeps_contiguous_payloads() {
        let mut writer = Writer::new();
        for i in 0..BATCH_SIZE {
            writer.push(span(i));
        }
        let source = writer.outbox.buckets[0].spans.get();
        // SAFETY: the producer is paused and the bucket is READY.
        let source_pointer = unsafe { &*source }.as_ptr();
        let mut batch = SpanBatch::new();
        drain(&writer.outbox, &mut batch);
        assert_eq!(
            batch.buffers().next().expect("one buffer").as_ptr(),
            source_pointer
        );
        assert_eq!(batch.len(), BATCH_SIZE);
        assert_eq!(
            batch
                .iter()
                .map(|s| s.name.parse::<usize>().expect("index"))
                .collect::<Vec<_>>(),
            (0..BATCH_SIZE).collect::<Vec<_>>()
        );
        assert_eq!(writer.outbox.len(), 0);
        batch.clear();
        assert!(batch.is_empty());
        assert_eq!(batch.buffers[0].as_ptr(), source_pointer);
    }

    #[test]
    fn native_partial_flush_is_visible_and_subsequent_push_keeps_fifo() {
        let mut writer = Writer::new();
        for i in 0..100 {
            writer.push(span(i));
            writer.flush();
        }
        let mut batch = SpanBatch::new();
        drain(&writer.outbox, &mut batch);
        assert_eq!(
            batch
                .iter()
                .map(|s| s.name.parse::<usize>().expect("index"))
                .collect::<Vec<_>>(),
            (0..100).collect::<Vec<_>>()
        );
        assert_eq!(writer.outbox.dropped(), 0);
    }

    #[test]
    fn native_full_buckets_drop_without_waiting_and_recover_after_drain() {
        let mut writer = Writer::new();
        let mut batch = SpanBatch::new();
        for i in 0..(BUCKET_CAPACITY + 10) {
            writer.push(span(i));
        }
        writer.flush();
        // The first automatically published bucket is intentionally not
        // reclaimed: retaining the newest batch preserves per-producer order.
        assert_eq!(writer.outbox.len(), BUCKET_CAPACITY);
        assert_eq!(writer.outbox.dropped(), 10);
        drain(&writer.outbox, &mut batch);
        assert_eq!(batch.len(), BUCKET_CAPACITY);
        batch.clear();
        writer.push(span(9999));
        writer.flush();
        drain(&writer.outbox, &mut batch);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.iter().next().expect("recovered span").name, "9999");
    }

    #[test]
    fn native_reader_owns_one_bucket_while_writer_fills_the_other() {
        let mut writer = Writer::new();
        for i in 0..BATCH_SIZE {
            writer.push(span(i));
        }
        let outbox = Arc::clone(&writer.outbox);
        outbox.drain(|spans| {
            assert_eq!(spans.len(), BATCH_SIZE);
            for i in BATCH_SIZE..BATCH_SIZE * 2 {
                writer.push(span(i));
            }
            assert_eq!(spans[0].name, "0");
            writer.flush();
        });
        let mut batch = SpanBatch::new();
        drain(&outbox, &mut batch);
        assert_eq!(batch.len(), BATCH_SIZE);
        assert_eq!(
            batch.iter().next().expect("second bucket").name,
            BATCH_SIZE.to_string()
        );
    }

    #[test]
    fn native_panicking_drain_releases_ownership() {
        let mut writer = Writer::new();
        for i in 0..BATCH_SIZE {
            writer.push(span(i));
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.outbox.drain(|_| panic!("export failure"));
            }))
            .is_err()
        );
        assert_eq!(writer.outbox.len(), 0);
        writer.push(span(100));
        writer.flush();
        let mut batch = SpanBatch::new();
        drain(&writer.outbox, &mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn native_sequence_wrap_preserves_two_bucket_order() {
        let mut writer = Writer::new();
        writer.sequence = SEQUENCE_MASK - 1;
        for i in 0..BATCH_SIZE * 2 {
            writer.push(span(i));
        }
        writer.flush();
        let mut batch = SpanBatch::new();
        drain(&writer.outbox, &mut batch);
        assert_eq!(
            batch
                .iter()
                .map(|s| s.name.parse::<usize>().expect("index"))
                .collect::<Vec<_>>(),
            (0..BATCH_SIZE * 2).collect::<Vec<_>>()
        );
    }

    #[test]
    fn native_thread_exit_publishes_partial_bucket() {
        let mut writer = Writer::new();
        writer.push(span(1));
        let outbox = Arc::clone(&writer.outbox);
        assert_eq!(outbox.len(), 0);
        drop(writer);
        assert_eq!(outbox.len(), 1);
        let mut batch = SpanBatch::new();
        drain(&outbox, &mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn native_multiple_consumers_never_duplicate_or_corrupt_spans() {
        let mut writer = Writer::new();
        let outbox = Arc::clone(&writer.outbox);
        let done = AtomicBool::new(false);
        let seen = (0..40_000).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>();
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let outbox = &outbox;
                let done = &done;
                let seen = &seen;
                scope.spawn(move || {
                    let mut batch = SpanBatch::new();
                    loop {
                        drain(outbox, &mut batch);
                        for span in batch.iter() {
                            let index = span.name.parse::<usize>().expect("uncorrupted payload");
                            assert_eq!(seen[index].fetch_add(1, Ordering::Relaxed), 0);
                        }
                        batch.clear();
                        if done.load(Ordering::Acquire) && outbox.len() == 0 {
                            break;
                        }
                        std::thread::yield_now();
                    }
                });
            }
            for i in 0..seen.len() {
                writer.push(span(i));
            }
            writer.flush();
            done.store(true, Ordering::Release);
        });
        let received: usize = seen.iter().map(|s| s.load(Ordering::Relaxed)).sum();
        assert_eq!(received as u64 + outbox.dropped(), seen.len() as u64);
    }

    #[test]
    fn native_registry_concurrent_registration_and_visits_keep_every_outbox() {
        let registry = Registry::new();
        std::thread::scope(|scope| {
            for _ in 0..80 {
                let registry = &registry;
                scope.spawn(move || {
                    let mut writer = Writer::new();
                    writer.push(span(1));
                    writer.flush();
                    registry.register(Arc::clone(&writer.outbox));
                    registry.visit(|outbox| {
                        let _ = outbox.len();
                    });
                });
            }
        });
        let mut batch = SpanBatch::new();
        registry.visit(|outbox| drain(outbox, &mut batch));
        assert_eq!(batch.len(), 80);
    }
}

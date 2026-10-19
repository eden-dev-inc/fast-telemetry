//! Native bucket handoffs allocate during registration/warmup, then reuse the
//! same contiguous allocations. Count only allocations on this test's thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Cow;
use std::cell::Cell;
use std::sync::Arc;

use fast_telemetry::{
    CompletedSpan, SpanAttribute, SpanBatch, SpanCollector, SpanEvent, SpanId, SpanKind,
    SpanStatus, TraceId,
};

struct CountingAllocator;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
    static WATCHED: Cell<usize> = const { Cell::new(0) };
    static FREED: Cell<usize> = const { Cell::new(0) };
}

fn count() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
    }
}

// SAFETY: allocation and deallocation are delegated to System unchanged; the
// bookkeeping uses initialized, allocation-free thread-local cells.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarding the caller's valid GlobalAlloc contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if WATCHED.try_with(Cell::get).unwrap_or(0) == ptr as usize {
            let _ = FREED.try_with(|n| n.set(n.get() + 1));
        }
        // SAFETY: forwarding the original pointer and layout to its allocator.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: forwarding the caller's valid allocation and new size.
        unsafe { System.realloc(ptr, layout, size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

#[test]
fn warmed_native_recording_and_bucket_export_allocate_nothing() {
    let collector = Arc::new(SpanCollector::new(1, 4096));
    let mut batch = SpanBatch::new();
    let mut cycle = || {
        for _ in 0..128 {
            let root = collector.start_span("root", SpanKind::Internal);
            for _ in 0..3 {
                drop(root.child("child", SpanKind::Internal));
            }
        }
        collector.flush_local();
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), 512);
        std::hint::black_box(batch.iter().count());
        batch.clear();
    };
    for _ in 0..8 {
        cycle();
    }
    ALLOCATIONS.with(|n| n.set(0));
    COUNTING.with(|enabled| enabled.set(true));
    for _ in 0..32 {
        cycle();
    }
    COUNTING.with(|enabled| enabled.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
    assert_eq!(collector.sampled_out_count(), 0);
    assert_eq!(collector.dropped_count(), 0);
}

fn watch(pointer: usize) {
    assert_ne!(pointer, 0);
    FREED.with(|n| n.set(0));
    WATCHED.with(|n| n.set(pointer));
}

fn clear_and_check_release(batch: &mut SpanBatch) {
    assert_eq!(FREED.with(Cell::get), 0, "payload must survive the drain");
    batch.clear();
    WATCHED.with(|n| n.set(0));
    assert_eq!(
        FREED.with(Cell::get),
        1,
        "clear must release owned payloads"
    );
    assert!(batch.is_empty());
}

#[test]
fn batch_clear_releases_every_owned_payload_in_mixed_native_buffers() {
    let collector = Arc::new(SpanCollector::new(1, 4096));
    let mut batch = SpanBatch::new();
    for (case, prefix) in (0..7).flat_map(|case| [0, 1, 254].map(|prefix| (case, prefix))) {
        for _ in 0..prefix {
            drop(collector.start_span("borrowed", SpanKind::Internal));
        }
        let value = String::from("owned payload survives until batch clear");
        let pointer = value.as_ptr() as usize;
        let span = if case == 0 {
            collector.start_span(value, SpanKind::Internal)
        } else {
            let mut span = collector.start_span("borrowed", SpanKind::Internal);
            match case {
                1 => span.set_status(SpanStatus::Error {
                    message: Cow::Owned(value),
                }),
                2 => span.set_attribute(value, 1i64),
                3 => span.set_attribute("key", value),
                4 => span.add_simple_event(value),
                5 => span.add_event("event", vec![SpanAttribute::new(value, 1i64)]),
                6 => span.add_event("event", vec![SpanAttribute::new("key", value)]),
                _ => unreachable!(),
            }
            span
        };
        watch(pointer);
        drop(span);
        // Cover both an owned suffix and plain records following an owned one.
        drop(collector.start_span("borrowed tail", SpanKind::Internal));
        collector.flush_local();
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), prefix + 2);
        clear_and_check_release(&mut batch);
    }
}

#[test]
fn batch_clear_releases_empty_metadata_vectors_with_retained_capacity() {
    let collector = Arc::new(SpanCollector::new(1, 4096));
    let sink = collector.span_sink(16);
    let mut batch = SpanBatch::new();
    for events in [false, true] {
        let mut span = CompletedSpan {
            trace_id: TraceId::INVALID,
            span_id: SpanId::INVALID,
            parent_span_id: SpanId::INVALID,
            name: Cow::Borrowed("borrowed"),
            kind: SpanKind::Internal,
            start_time_ns: 1,
            end_time_ns: 2,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        };
        let pointer = if events {
            span.events = Vec::<SpanEvent>::with_capacity(8);
            span.events.as_ptr() as usize
        } else {
            span.attributes = Vec::<SpanAttribute>::with_capacity(8);
            span.attributes.as_ptr() as usize
        };
        watch(pointer);
        assert!(sink.publish(span).is_ok());
        collector.drain_batches(&mut batch);
        assert_eq!(batch.len(), 1);
        clear_and_check_release(&mut batch);
    }
}

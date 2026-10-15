use std::borrow::Cow;
use std::sync::Arc;

use fast_telemetry::{CompletedSpan, SpanCollector, SpanId, SpanKind, SpanStatus, TraceId};

fn completed(name: &'static str) -> CompletedSpan {
    CompletedSpan {
        trace_id: TraceId::random(),
        span_id: SpanId::random(),
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

#[test]
fn publication_is_visible_from_another_thread_while_worker_is_alive() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let sink = collector.span_sink(4);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        assert!(sink.publish(completed("low volume")).is_ok());
        ready_tx.send(()).unwrap();
        stop_rx.recv().unwrap();
    });
    ready_rx.recv().unwrap();
    let mut spans = Vec::new();
    collector.drain_into(&mut spans);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "low volume");
    stop_tx.send(()).unwrap();
    worker.join().unwrap();
}

#[test]
fn capacity_rejection_returns_ownership_and_drain_reopens_capacity() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let sink = collector.span_sink(1);
    assert!(sink.publish(completed("first")).is_ok());
    let rejected = sink.publish(completed("retry")).err().unwrap();
    assert_eq!(rejected.name, "retry");
    assert_eq!(sink.accepted_count(), 1);
    assert_eq!(sink.rejected_count(), 1);
    let mut spans = Vec::new();
    collector.drain_into(&mut spans);
    assert!(sink.clone().publish(*rejected).is_ok());
    collector.drain_into(&mut spans);
    assert_eq!(spans.len(), 2);
    assert_eq!(sink.accepted_count(), 2);
    assert_eq!(sink.capacity(), 1);
}

#[test]
fn sink_owns_collector_and_concurrent_publications_do_not_sample() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let weak = Arc::downgrade(&collector);
    let sink = collector.span_sink(512);
    drop(collector);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let sink = sink.clone();
            scope.spawn(move || {
                for _ in 0..100 {
                    assert!(sink.publish(completed("span")).is_ok());
                }
            });
        }
    });
    let mut spans = Vec::new();
    weak.upgrade().unwrap().drain_into(&mut spans);
    assert_eq!(spans.len(), 400);
    assert_eq!(sink.accepted_count(), 400);
    assert_eq!(sink.rejected_count(), 0);
    drop(sink);
    assert!(weak.upgrade().is_none());
}

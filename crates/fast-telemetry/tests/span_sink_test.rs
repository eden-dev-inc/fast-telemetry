use std::borrow::Cow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};

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

#[test]
fn one_producer_can_fill_the_entire_capacity_on_every_refill() {
    for capacity in [1, 3, 17, 31, 32, 33, 37, 63, 64, 65, 127, 128, 129] {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let sink = collector.span_sink(capacity);
        for round in 0..3 {
            for _ in 0..sink.capacity() {
                assert!(sink.publish(completed("accepted")).is_ok());
            }
            assert_eq!(collector.len(), capacity);
            let rejected = sink.publish(completed("retry")).unwrap_err();
            assert_eq!(rejected.name, "retry");
            let mut spans = vec![completed("already drained")];
            collector.drain_into(&mut spans);
            assert_eq!(spans.len(), capacity + 1);
            assert_eq!(spans[0].name, "already drained");
            assert!(collector.is_empty());
            assert_eq!(sink.accepted_count(), (round + 1) * capacity as u64);
            assert_eq!(sink.rejected_count(), round + 1);
        }
    }
}

#[test]
fn eight_producers_share_one_capacity_and_competing_drains_refill_it() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let sink = collector.span_sink(37);
    let published = Barrier::new(10);
    let drained = Barrier::new(10);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let sink = sink.clone();
            let published = &published;
            let drained = &drained;
            scope.spawn(move || {
                for _ in 0..4 {
                    for _ in 0..8 {
                        let _ = sink.publish(completed("concurrent"));
                    }
                    published.wait();
                    drained.wait();
                }
            });
        }
        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let collector = &collector;
                let published = &published;
                let drained = &drained;
                scope.spawn(move || {
                    let mut rounds = Vec::new();
                    for _ in 0..4 {
                        published.wait();
                        let mut spans = Vec::new();
                        collector.drain_into(&mut spans);
                        rounds.push(spans);
                        drained.wait();
                    }
                    rounds
                })
            })
            .collect();
        let mut rounds = vec![0; 4];
        for consumer in consumers {
            for (round, spans) in consumer.join().unwrap().into_iter().enumerate() {
                rounds[round] += spans.len();
                assert!(spans.iter().all(|span| span.name == "concurrent"));
            }
        }
        assert_eq!(rounds, vec![37; 4]);
    });
    assert_eq!(sink.accepted_count(), 4 * 37);
    assert_eq!(sink.rejected_count(), 4 * (64 - 37));
    assert!(collector.is_empty());
}

#[test]
fn concurrent_publications_and_drains_account_for_every_attempt_once() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let sink = collector.span_sink(7);
    let remaining = AtomicUsize::new(8);
    let start = Barrier::new(10);
    let mut collected = Vec::new();
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let sink = sink.clone();
            let remaining = &remaining;
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for item in 0..512 {
                    let mut span = completed("racing");
                    span.start_time_ns = worker * 512 + item;
                    let _ = sink.publish(span);
                }
                remaining.fetch_sub(1, Ordering::Release);
            });
        }
        let consumers: Vec<_> = (0..2)
            .map(|_| {
                let collector = &collector;
                let remaining = &remaining;
                let start = &start;
                scope.spawn(move || {
                    let mut spans = Vec::new();
                    start.wait();
                    while remaining.load(Ordering::Acquire) != 0 {
                        collector.drain_into(&mut spans);
                        std::thread::yield_now();
                    }
                    collector.drain_into(&mut spans);
                    spans
                })
            })
            .collect();
        for consumer in consumers {
            collected.extend(consumer.join().unwrap());
        }
    });
    assert_eq!(sink.accepted_count(), collected.len() as u64);
    assert_eq!(sink.accepted_count() + sink.rejected_count(), 8 * 512);
    collected.sort_unstable_by_key(|span| span.start_time_ns);
    assert!(
        collected
            .windows(2)
            .all(|pair| pair[0].start_time_ns != pair[1].start_time_ns)
    );
    assert!(collector.is_empty());
    // All removed spans released their slots, including when consumers raced.
    for _ in 0..sink.capacity() {
        assert!(sink.publish(completed("refilled")).is_ok());
    }
    assert!(sink.publish(completed("full")).is_err());
}

#[test]
fn multiple_collectors_and_sinks_keep_independent_capacity_and_native_spans() {
    let first = Arc::new(SpanCollector::new(1, 1));
    let second = Arc::new(SpanCollector::new(1, 1));
    let first_sink = first.span_sink(2);
    let other_sink = first.span_sink(3);
    let second_sink = second.span_sink(1);
    for _ in 0..2 {
        assert!(first_sink.publish(completed("first")).is_ok());
    }
    for _ in 0..3 {
        assert!(other_sink.publish(completed("other")).is_ok());
    }
    assert!(second_sink.publish(completed("second")).is_ok());
    assert!(first_sink.publish(completed("full")).is_err());
    assert!(other_sink.publish(completed("full")).is_err());
    assert!(second_sink.publish(completed("full")).is_err());
    drop(first.start_span("native", SpanKind::Internal));
    first.flush_local();
    let mut spans = vec![completed("existing")];
    first.drain_into(&mut spans);
    assert_eq!(spans.len(), 7);
    assert_eq!(spans.iter().filter(|span| span.name == "native").count(), 1);
    assert!(first.is_empty());
    assert_eq!(second.len(), 1);
    assert!(first_sink.publish(completed("first refill")).is_ok());
    assert!(other_sink.publish(completed("other refill")).is_ok());
    assert!(second_sink.publish(completed("still full")).is_err());
    second.drain_into(&mut spans);
    assert_eq!(spans.len(), 8);
    assert!(second_sink.publish(completed("second refill")).is_ok());
    assert_eq!(first_sink.accepted_count(), 3);
    assert_eq!(other_sink.accepted_count(), 4);
    assert_eq!(second_sink.accepted_count(), 2);
    assert_eq!(second_sink.rejected_count(), 2);
    assert_eq!(first.recorded_count(), 1);
}

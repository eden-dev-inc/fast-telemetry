//! Configured native publication stays observable while workers remain alive.

use std::sync::{Arc, Barrier};

use fast_telemetry::{SpanBatch, SpanCollector, SpanCollectorConfig, SpanKind};

#[test]
fn configured_batches_publish_and_refill_at_their_target() {
    assert_eq!(SpanCollectorConfig::default().producer_batch_size(), 384);
    for target in [1, 17, 256, 384, 512, 1024] {
        let collector = Arc::new(SpanCollector::with_config(
            SpanCollectorConfig::default().with_producer_batch_size(target),
        ));
        let mut spans = SpanBatch::new();
        for _ in 0..3 {
            for _ in 0..target - 1 {
                drop(collector.start_span("before boundary", SpanKind::Internal));
            }
            assert_eq!(collector.len(), 0);
            drop(collector.start_span("boundary", SpanKind::Internal));
            collector.drain_batches(&mut spans);
            assert_eq!(spans.len(), target);
            spans.clear();
        }
        assert_eq!(collector.recorded_count(), (target * 3) as u64);
        assert_eq!(collector.sampled_out_count(), 0);
        assert_eq!(collector.dropped_count(), 0);
    }
}

#[test]
fn workers_share_policy_and_publish_partial_batches_on_exit() {
    let collector = Arc::new(SpanCollector::with_config(
        SpanCollectorConfig::default().with_producer_batch_size(17),
    ));
    let ready = Arc::new(Barrier::new(5));
    let finish = Arc::new(Barrier::new(5));
    let workers = (0..4)
        .map(|_| {
            let collector = Arc::clone(&collector);
            let ready = Arc::clone(&ready);
            let finish = Arc::clone(&finish);
            std::thread::spawn(move || {
                for _ in 0..17 {
                    drop(collector.start_span("full", SpanKind::Internal));
                }
                ready.wait();
                finish.wait();
                drop(collector.start_span("partial", SpanKind::Internal));
            })
        })
        .collect::<Vec<_>>();
    ready.wait();
    assert_eq!(collector.len(), 4 * 17);
    finish.wait();
    // Join the OS threads before asserting TLS-destructor publication.
    for worker in workers {
        worker.join().expect("producer joined");
    }
    let mut spans = SpanBatch::new();
    collector.drain_batches(&mut spans);
    assert_eq!(spans.len(), 4 * 18);
    assert_eq!(collector.recorded_count(), 4 * 18);
    assert_eq!(collector.sampled_out_count(), 0);
    assert_eq!(collector.dropped_count(), 0);
}

#[test]
fn local_flush_publishes_a_partial_configured_batch() {
    let collector = Arc::new(SpanCollector::with_config(
        SpanCollectorConfig::default().with_producer_batch_size(1024),
    ));
    drop(collector.start_span("partial", SpanKind::Internal));
    assert_eq!(collector.len(), 0);
    collector.flush_local();
    assert_eq!(collector.len(), 1);
}

#[test]
fn invalid_batch_sizes_are_rejected_before_recording() {
    for target in [0, 1025, usize::MAX] {
        assert!(
            std::panic::catch_unwind(|| {
                SpanCollectorConfig::default().with_producer_batch_size(target)
            })
            .is_err()
        );
    }
}

#[cfg(feature = "runtime")]
#[test]
fn runtime_uses_the_configured_native_collector() {
    use fast_telemetry::{Runtime, RuntimeConfig};
    let runtime = Runtime::new(
        RuntimeConfig::default()
            .with_span_config(SpanCollectorConfig::default().with_producer_batch_size(1)),
    );
    drop(runtime.start_span("immediate", SpanKind::Internal));
    assert_eq!(runtime.span_collector().len(), 1);
    assert_eq!(runtime.config().span_config().producer_batch_size(), 1);
    assert_eq!(runtime.span_collector().config().producer_batch_size(), 1);
}

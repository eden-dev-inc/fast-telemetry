//! Sustained native tracing throughput, including concurrent drain and destruction.
//! Each collector has one consumer and a disjoint group of producers. Increasing
//! collectors measures independent pipelines; one collector measures shared use.
//! No encoding, compression, network, or per-iteration coordination is included.

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

use fast_telemetry::{CompletedSpan, SpanBatch, SpanCollector, SpanId, SpanKind};

#[derive(Clone, Copy)]
struct Options {
    children: bool,
    producers: usize,
    collectors: usize,
    duration: Duration,
    burst: usize,
}

impl Options {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 6 {
            return Err("usage: bench_span_saturation root|children PRODUCERS COLLECTORS DURATION_MS BURST_TRACES".into());
        }
        let children = match args[1].as_str() {
            "root" => false,
            "children" => true,
            _ => return Err("unknown workload".into()),
        };
        let producers = args[2].parse::<usize>()?;
        let collectors = args[3].parse::<usize>()?;
        let milliseconds = args[4].parse::<u64>()?;
        let burst = args[5].parse::<usize>()?;
        if !(1..=256).contains(&producers)
            || !(1..=producers).contains(&collectors)
            || !(1..=60_000).contains(&milliseconds)
            || !(1..=65_536).contains(&burst)
        {
            return Err("require 1..256 producers, 1..PRODUCERS collectors, 1..60000 ms and 1..65536 burst traces".into());
        }
        Ok(Self {
            children,
            producers,
            collectors,
            duration: Duration::from_millis(milliseconds),
            burst,
        })
    }

    fn spans_per_trace(self) -> usize {
        if self.children { 4 } else { 1 }
    }
}

fn generate(collector: &Arc<SpanCollector>, children: bool) {
    let mut root = collector.start_span("operation", SpanKind::Server);
    root.enter();
    if children {
        for _ in 0..3 {
            let mut child = root.child("child", SpanKind::Client);
            child.enter();
        }
    }
}

/// A stopped producer's warmup has no sampling or loss: validate full parent
/// relationships before timing. Under overload, individual completions can be
/// rejected, so the timed path validates spans and reports complete-trace bounds.
fn verify_warmup(spans: &[&CompletedSpan], per_trace: usize) {
    assert_eq!(spans.len() % per_trace, 0);
    for group in spans.chunks_exact(per_trace) {
        let root = group[per_trace - 1];
        assert_eq!(root.parent_span_id, SpanId::INVALID);
        for child in &group[..per_trace - 1] {
            assert_eq!(child.trace_id, root.trace_id);
            assert_eq!(child.parent_span_id, root.span_id);
            assert!(
                child.start_time_ns >= root.start_time_ns && child.end_time_ns <= root.end_time_ns
            );
        }
    }
}

fn consume(spans: &mut SpanBatch) -> (u64, u64) {
    let mut roots = 0;
    for span in spans.iter() {
        assert!(!span.trace_id.is_invalid() && !span.span_id.is_invalid());
        assert!(span.start_time_ns > 0 && span.end_time_ns >= span.start_time_ns);
        if span.parent_span_id == SpanId::INVALID {
            assert_eq!(span.kind, SpanKind::Server);
            assert_eq!(span.name, "operation");
            roots += 1;
        } else {
            assert_eq!(span.kind, SpanKind::Client);
            assert_eq!(span.name, "child");
            assert_ne!(span.span_id, span.parent_span_id);
        }
    }
    let count = spans.len() as u64;
    black_box(&spans);
    spans.clear();
    (count, roots)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse()?;
    let collectors: Vec<_> = (0..options.collectors)
        .map(|_| Arc::new(SpanCollector::new(1, 4096)))
        .collect();
    let done: Vec<_> = (0..options.collectors)
        .map(|_| AtomicUsize::new(0))
        .collect();
    let ready = Barrier::new(options.producers + 1);
    let start = Barrier::new(options.producers + options.collectors + 1);
    let epoch = OnceLock::new();
    let per_trace = options.spans_per_trace();

    let (attempted, collected, roots, seconds) = std::thread::scope(|scope| {
        let mut producers = Vec::new();
        for index in 0..options.producers {
            let collector = &collectors[index % options.collectors];
            let done = &done[index % options.collectors];
            let ready = &ready;
            let start = &start;
            let epoch = &epoch;
            producers.push(scope.spawn(move || {
                for _ in 0..16 {
                    generate(collector, options.children);
                }
                collector.flush_local();
                ready.wait();
                start.wait();
                let deadline = *epoch.get().expect("start epoch") + options.duration;
                let mut attempted = 0u64;
                while Instant::now() < deadline {
                    black_box(collector);
                    for _ in 0..options.burst {
                        generate(collector, options.children);
                    }
                    attempted += options.burst as u64;
                }
                collector.flush_local();
                assert!(fast_telemetry::current_trace_id().is_none());
                assert!(fast_telemetry::current_span_id().is_none());
                done.fetch_add(1, Ordering::Release);
                (attempted, Instant::now())
            }));
        }

        // Producers remain resident. Drain every warmup payload and reuse each
        // consumer's allocation, without charging thread/collector setup.
        ready.wait();
        let mut consumers = Vec::new();
        for (index, collector) in collectors.iter().enumerate() {
            let group_size = (options.producers - 1 - index) / options.collectors + 1;
            let mut spans = SpanBatch::new();
            collector.drain_batches(&mut spans);
            verify_warmup(&spans.iter().collect::<Vec<_>>(), per_trace);
            assert_eq!(consume(&mut spans).0, (group_size * 16 * per_trace) as u64);
            assert_eq!(collector.recorded_count(), (group_size * 16) as u64);
            assert_eq!(collector.sampled_out_count(), 0);
            let done = &done[index];
            let start = &start;
            consumers.push(scope.spawn(move || {
                start.wait();
                let mut collected = 0u64;
                let mut roots = 0u64;
                loop {
                    collector.drain_batches(&mut spans);
                    let (count, root_count) = consume(&mut spans);
                    collected += count;
                    roots += root_count;
                    if done.load(Ordering::Acquire) == group_size {
                        // The final flush precedes each producer's release.
                        collector.drain_batches(&mut spans);
                        let (count, root_count) = consume(&mut spans);
                        collected += count;
                        roots += root_count;
                        assert!(collector.is_empty());
                        break;
                    }
                    if count == 0 {
                        std::thread::yield_now();
                    }
                }
                (collected, roots, Instant::now())
            }));
        }
        let beginning = Instant::now();
        epoch.set(beginning).expect("one start epoch");
        start.wait();
        let mut end = beginning;
        let mut attempted = 0u64;
        for producer in producers {
            let (count, finished) = producer.join().expect("producer panicked");
            attempted += count;
            end = end.max(finished);
        }
        let mut collected = 0u64;
        let mut roots = 0u64;
        for consumer in consumers {
            let (count, root_count, finished) = consumer.join().expect("consumer panicked");
            collected += count;
            roots += root_count;
            end = end.max(finished);
        }
        (attempted, collected, roots, (end - beginning).as_secs_f64())
    });

    let recorded = collectors.iter().map(|c| c.recorded_count()).sum::<u64>()
        - (options.producers * 16) as u64;
    let sampled_out = collectors
        .iter()
        .map(|c| c.sampled_out_count())
        .sum::<u64>();
    assert_eq!(attempted, recorded + sampled_out);
    let expected_spans = recorded * per_trace as u64;
    let uncollected = expected_spans
        .checked_sub(collected)
        .expect("extra collected spans");
    assert_eq!(
        uncollected,
        collectors.iter().map(|c| c.dropped_count()).sum::<u64>()
    );
    let lower = recorded.saturating_sub(uncollected);
    let upper = roots
        .min(collected / per_trace as u64)
        .min(recorded - uncollected.div_ceil(per_trace as u64));
    assert!(lower <= upper);
    let workload = if options.children { "children" } else { "root" };
    println!(
        "workload,producers,collectors,duration_ms,burst_traces,elapsed_seconds,attempted_traces,recorded_traces,sampled_out_traces,collected_root_spans,collected_spans,uncollected_spans,complete_trace_lower_bound,complete_trace_upper_bound,attempted_traces_per_second,complete_trace_lower_bound_per_second,collected_spans_per_second"
    );
    println!(
        "{workload},{},{},{},{},{seconds:.9},{attempted},{recorded},{sampled_out},{roots},{collected},{uncollected},{lower},{upper},{:.3},{:.3},{:.3}",
        options.producers,
        options.collectors,
        options.duration.as_millis(),
        options.burst,
        attempted as f64 / seconds,
        lower as f64 / seconds,
        collected as f64 / seconds,
    );
    Ok(())
}

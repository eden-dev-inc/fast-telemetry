//! Matched unthrottled native / drop-in tracing / Registry lifecycle workloads.
//! Registry is a non-retaining baseline. Encoding and network are excluded.

use std::collections::{HashMap, HashSet};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::time::{Duration, Instant};

use fast_telemetry::{CompletedSpan, SpanBatch, SpanCollector, SpanId, SpanKind, SpanStatus};
use fast_telemetry_tracing::{FastTelemetrySubscriber, LayerHandle, MetadataPolicy};
use tracing::Dispatch;
use tracing_subscriber::Registry;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Native,
    Fast,
    Registry,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Fast => "fast",
            Self::Registry => "registry",
        }
    }
}

#[derive(Clone, Copy)]
struct Options {
    mode: Mode,
    children: bool,
    producers: usize,
    collectors: usize,
    duration: Duration,
    burst: usize,
}

impl Options {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let args: Vec<_> = std::env::args().collect();
        if args.len() != 7 {
            return Err("usage: bench_tracing_saturation native|fast|registry root|children PRODUCERS COLLECTORS DURATION_MS BURST_TRACES".into());
        }
        let mode = match args[1].as_str() {
            "native" => Mode::Native,
            "fast" => Mode::Fast,
            "registry" => Mode::Registry,
            _ => return Err("unknown backend".into()),
        };
        let children = match args[2].as_str() {
            "root" => false,
            "children" => true,
            _ => return Err("unknown workload".into()),
        };
        let producers = args[3].parse::<usize>()?;
        let collectors = args[4].parse::<usize>()?;
        let milliseconds = args[5].parse::<u64>()?;
        let burst = args[6].parse::<usize>()?;
        if !(1..=256).contains(&producers)
            || !(1..=producers).contains(&collectors)
            || !(1..=60_000).contains(&milliseconds)
            || !(1..=65_536).contains(&burst)
        {
            return Err("require 1..256 producers, 1..PRODUCERS collectors, 1..60000 ms and 1..65536 burst traces".into());
        }
        Ok(Self {
            mode,
            children,
            producers,
            collectors,
            duration: Duration::from_millis(milliseconds),
            burst,
        })
    }

    fn spans_per_trace(self) -> u64 {
        if self.children { 4 } else { 1 }
    }
}

struct Backend {
    mode: Mode,
    collector: Arc<SpanCollector>,
    dispatch: Option<Dispatch>,
    handle: Option<LayerHandle>,
    global: bool,
}

impl Backend {
    fn new(mode: Mode, producers: usize, global: bool) -> Self {
        let collector = Arc::new(SpanCollector::new(1, 4096));
        // Match the native aggregate per-producer outbox budget. The adapter's
        // bounded sink shares this budget across its fixed producer shards.
        let (dispatch, handle) = match mode {
            Mode::Native => (None, None),
            Mode::Fast => {
                let subscriber =
                    FastTelemetrySubscriber::with_capacity(collector.clone(), producers * 4096, 1)
                        .with_metadata_policy(MetadataPolicy::UserFields);
                let handle = subscriber.handle();
                (Some(Dispatch::new(subscriber)), Some(handle))
            }
            Mode::Registry => (Some(Dispatch::new(Registry::default())), None),
        };
        if global && let Some(dispatch) = &dispatch {
            tracing::dispatcher::set_global_default(dispatch.clone()).expect("one global dispatch");
        }
        Self {
            mode,
            collector,
            dispatch,
            handle,
            global,
        }
    }

    fn with_producer(&self, run: impl FnOnce()) {
        if !self.global
            && let Some(dispatch) = &self.dispatch
        {
            tracing::dispatcher::with_default(dispatch, run);
        } else {
            run();
        }
    }

    fn generate(&self, children: bool) {
        if self.mode == Mode::Native {
            let mut root = self.collector.start_span("operation", SpanKind::Internal);
            let root = root.enter();
            if children {
                for _ in 0..3 {
                    let mut child = root.child("child", SpanKind::Internal);
                    let _entered = child.enter();
                }
            }
        } else {
            let root = tracing::info_span!(parent: None, "operation");
            let _root = root.enter();
            if children {
                for _ in 0..3 {
                    let child = tracing::info_span!("child");
                    let _child = child.enter();
                    black_box(&child);
                }
            }
            black_box(&root);
        }
    }
}

/// Immediate adapter publication can split traces between drains. Check each
/// span identically in all modes and count collected roots separately. Final
/// accounting gives conservative bounds on complete traces when spans are lost.
struct DrainStorage {
    native: SpanBatch,
    flat: Vec<CompletedSpan>,
    batched: bool,
}

impl DrainStorage {
    fn new(mode: Mode) -> Self {
        Self {
            native: SpanBatch::new(),
            flat: Vec::new(),
            batched: mode == Mode::Native,
        }
    }

    fn drain(&mut self, collector: &SpanCollector) {
        if self.batched {
            collector.drain_batches(&mut self.native);
        } else {
            collector.drain_into(&mut self.flat);
        }
    }

    fn warmup(&self) -> Vec<&CompletedSpan> {
        if self.batched {
            self.native.iter().collect()
        } else {
            self.flat.iter().collect()
        }
    }

    fn consume(&mut self) -> (u64, u64) {
        let result = if self.batched {
            self.native
                .buffers()
                .map(consume)
                .fold((0, 0), |(count, roots), (n, r)| (count + n, roots + r))
        } else {
            consume(&self.flat)
        };
        self.native.clear();
        self.flat.clear();
        result
    }
}

fn consume(spans: &[CompletedSpan]) -> (u64, u64) {
    let mut roots = 0;
    for span in spans {
        assert!(!span.trace_id.is_invalid() && !span.span_id.is_invalid());
        assert!(span.start_time_ns > 0 && span.end_time_ns >= span.start_time_ns);
        assert_eq!(span.kind, SpanKind::Internal);
        assert!(span.attributes.is_empty() && span.events.is_empty());
        assert!(matches!(span.status, SpanStatus::Unset));
        if span.parent_span_id == SpanId::INVALID {
            assert_eq!(span.name, "operation");
            roots += 1;
        } else {
            assert_eq!(span.name, "child");
            assert_ne!(span.span_id, span.parent_span_id);
        }
    }
    black_box(spans);
    (spans.len() as u64, roots)
}

fn verify_warmup(spans: &[&CompletedSpan], traces: u64, per_trace: u64) {
    assert_eq!(spans.len() as u64, traces * per_trace);
    let roots: HashMap<_, _> = spans
        .iter()
        .filter(|span| span.parent_span_id == SpanId::INVALID)
        .map(|span| (span.trace_id, span))
        .collect();
    assert_eq!(roots.len() as u64, traces);
    let mut ids = HashSet::new();
    let mut counts = HashMap::new();
    for span in spans {
        assert!(ids.insert(span.span_id));
        let root = roots.get(&span.trace_id).expect("warmup trace root");
        *counts.entry(span.trace_id).or_insert(0u64) += 1;
        if span.parent_span_id != SpanId::INVALID {
            assert_eq!(span.parent_span_id, root.span_id);
            assert!(span.start_time_ns >= root.start_time_ns);
            assert!(span.end_time_ns <= root.end_time_ns);
        }
    }
    assert!(counts.values().all(|&count| count == per_trace));
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = Options::parse()?;
    let backends: Vec<_> = (0..options.collectors)
        .map(|index| {
            let group_size = (options.producers - 1 - index) / options.collectors + 1;
            Backend::new(options.mode, group_size, options.collectors == 1)
        })
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
            let backend = &backends[index % options.collectors];
            let done = &done[index % options.collectors];
            let ready = &ready;
            let start = &start;
            let epoch = &epoch;
            producers.push(scope.spawn(move || {
                let mut result = None;
                backend.with_producer(|| {
                    for _ in 0..16 {
                        backend.generate(options.children);
                    }
                    backend.collector.flush_local();
                    ready.wait();
                    start.wait();
                    let deadline = *epoch.get().expect("start epoch") + options.duration;
                    let mut attempted = 0u64;
                    while Instant::now() < deadline {
                        black_box(backend);
                        for _ in 0..options.burst {
                            backend.generate(options.children);
                        }
                        attempted += options.burst as u64;
                    }
                    backend.collector.flush_local();
                    assert!(fast_telemetry::current_span_id().is_none());
                    assert!(fast_telemetry::current_trace_id().is_none());
                    assert!(tracing::Span::current().is_none());
                    done.fetch_add(1, Ordering::Release);
                    result = Some((attempted, Instant::now()));
                });
                result.expect("producer completed")
            }));
        }
        ready.wait();
        let mut consumers = Vec::new();
        for (index, backend) in backends.iter().enumerate() {
            let group_size = (options.producers - 1 - index) / options.collectors + 1;
            let mut spans = DrainStorage::new(options.mode);
            spans.drain(&backend.collector);
            let warmup = spans.warmup();
            if options.mode == Mode::Registry {
                assert!(warmup.is_empty());
            } else {
                verify_warmup(&warmup, (group_size * 16) as u64, per_trace);
            }
            spans.consume();
            let done = &done[index];
            let start = &start;
            consumers.push(scope.spawn(move || {
                start.wait();
                let mut collected = 0u64;
                let mut roots = 0u64;
                loop {
                    spans.drain(&backend.collector);
                    let (count, root_count) = spans.consume();
                    collected += count;
                    roots += root_count;
                    if done.load(Ordering::Acquire) == group_size {
                        spans.drain(&backend.collector);
                        let (count, root_count) = spans.consume();
                        collected += count;
                        roots += root_count;
                        assert!(backend.collector.is_empty());
                        break;
                    }
                    if options.mode == Mode::Registry {
                        // Registry retains no output. Avoid spending a core
                        // continuously polling an intentionally empty queue.
                        std::thread::sleep(Duration::from_millis(1));
                    } else if count == 0 {
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
    let retains = options.mode != Mode::Registry;
    let sampled_out = backends
        .iter()
        .map(|b| b.collector.sampled_out_count())
        .sum::<u64>();
    let recorded = if options.mode == Mode::Native {
        backends
            .iter()
            .map(|b| b.collector.recorded_count())
            .sum::<u64>()
            - (options.producers * 16) as u64
    } else {
        attempted
    };
    assert_eq!(attempted, recorded + sampled_out);
    let expected = if retains { recorded * per_trace } else { 0 };
    let missing = expected
        .checked_sub(collected)
        .expect("extra collected spans");
    if options.mode == Mode::Native {
        assert_eq!(
            missing,
            backends
                .iter()
                .map(|b| b.collector.dropped_count())
                .sum::<u64>()
        );
    }
    if options.mode == Mode::Fast {
        let accepted = backends
            .iter()
            .map(|b| {
                b.handle
                    .as_ref()
                    .expect("adapter handle")
                    .accepted_span_count()
            })
            .sum::<u64>()
            - (options.producers * 16) as u64 * per_trace;
        let rejected = backends
            .iter()
            .map(|b| {
                b.handle
                    .as_ref()
                    .expect("adapter handle")
                    .dropped_span_count()
            })
            .sum::<u64>();
        assert_eq!(accepted, collected);
        assert_eq!(rejected, missing);
    }
    let lower = if retains {
        recorded.saturating_sub(missing)
    } else {
        0
    };
    let upper = if retains {
        roots
            .min(collected / per_trace)
            .min(recorded - missing.div_ceil(per_trace))
    } else {
        0
    };
    assert!(lower <= upper);
    if per_trace == 1 {
        assert_eq!(lower, upper);
    }
    let mode = options.mode.name();
    let workload = if options.children { "children" } else { "root" };
    let dispatch = if options.collectors == 1 {
        "global"
    } else {
        "scoped"
    };
    println!(
        "mode,workload,producers,collectors,dispatch,retains_spans,elapsed_seconds,attempted_traces,recorded_traces,sampled_out_traces,collected_root_spans,collected_spans,uncollected_spans,complete_trace_lower_bound,complete_trace_upper_bound,attempted_traces_per_second,collected_spans_per_second"
    );
    println!(
        "{mode},{workload},{},{},{dispatch},{retains},{seconds:.9},{attempted},{recorded},{sampled_out},{roots},{collected},{missing},{lower},{upper},{:.3},{:.3}",
        options.producers,
        options.collectors,
        attempted as f64 / seconds,
        collected as f64 / seconds
    );
    Ok(())
}

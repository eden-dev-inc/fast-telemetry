//! Matched in-memory retention benchmark. Every mode uses `instrument` below.
//! No network or protobuf serialization is timed. See ../../README.md.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use fast_telemetry::{CompletedSpan, SpanCollector, SpanValue};
use fast_telemetry_tracing::{FastTelemetryLayer, LayerHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter};
use parking_lot::Mutex;
use tracing::Dispatch;
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Fast,
    Otel,
    Disabled,
}
#[derive(Clone, Copy, PartialEq)]
enum Workload {
    Root,
    Attributes,
    Children,
}

impl Workload {
    fn spans(self) -> u64 {
        if self == Self::Root {
            1
        } else if self == Self::Attributes {
            2
        } else {
            4
        }
    }
    fn events(self) -> u64 {
        u64::from(self == Self::Attributes)
    }
}

struct Options {
    mode: Mode,
    mode_name: String,
    workload: Workload,
    workload_name: String,
    threads: usize,
    iterations: usize,
    warmup: usize,
    batch: usize,
    capacity: usize,
}

impl Options {
    fn parse() -> Result<Self, String> {
        let mut options = Self {
            mode: Mode::Fast,
            mode_name: "fast".into(),
            workload: Workload::Root,
            workload_name: "root".into(),
            threads: 1,
            iterations: 10_000,
            warmup: 100,
            batch: 64,
            capacity: 4096,
        };
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {arg}"))?;
            match arg.as_str() {
                "--mode" => {
                    options.mode = match value.as_str() {
                        "fast" => Mode::Fast,
                        "otel" => Mode::Otel,
                        "disabled" => Mode::Disabled,
                        _ => return Err("mode must be fast, otel, or disabled".into()),
                    };
                    options.mode_name = value;
                }
                "--workload" => {
                    options.workload = match value.as_str() {
                        "root" => Workload::Root,
                        "attributes" => Workload::Attributes,
                        "children" => Workload::Children,
                        _ => return Err("workload must be root, attributes, or children".into()),
                    };
                    options.workload_name = value;
                }
                "--threads" => options.threads = number(&value)?,
                "--iters" => options.iterations = number(&value)?,
                "--warmup" => options.warmup = value.parse().map_err(|_| "invalid warmup")?,
                "--batch" => options.batch = number(&value)?,
                "--capacity" => options.capacity = number(&value)?,
                _ => return Err(format!("unknown option {arg}")),
            }
        }
        // Avoid integer overflow in expected counts or capacity calculations.
        options
            .threads
            .checked_mul(options.iterations)
            .and_then(|n| n.checked_mul(4))
            .ok_or("work count overflow")?;
        options
            .threads
            .checked_mul(options.warmup)
            .and_then(|n| n.checked_mul(4))
            .ok_or("warmup count overflow")?;
        Ok(options)
    }
}

fn number(value: &str) -> Result<usize, String> {
    value
        .parse()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("expected positive integer: {value}"))
}

// These are the exact same tracing callsites for every backend. Worker defaults
// are installed explicitly; nothing sets or changes the global subscriber.
#[inline(never)]
fn instrument(workload: Workload, worker: usize, iteration: usize) {
    match workload {
        Workload::Root => {
            let root = tracing::info_span!(parent: None, "root");
            let _entered = root.enter();
            std::hint::black_box(&root);
        }
        Workload::Attributes => {
            let root = tracing::info_span!(parent: None, "root", worker = worker as i64, iteration = iteration as i64);
            let _entered = root.enter();
            let child = tracing::info_span!("child", method = "GET", code = 200i64);
            let _child = child.enter();
            tracing::info!(ok = true, "checkpoint");
            std::hint::black_box(&child);
        }
        Workload::Children => {
            let root = tracing::info_span!(parent: None, "root");
            let _entered = root.enter();
            for name in 0..3 {
                let child = match name {
                    0 => tracing::info_span!("child_a"),
                    1 => tracing::info_span!("child_b"),
                    _ => tracing::info_span!("child_c"),
                };
                let _child = child.enter();
                std::hint::black_box(&child);
            }
        }
    }
}

#[derive(Debug)]
struct ExportQueue {
    spans: Mutex<Vec<SpanData>>,
    capacity: usize,
    accepted: AtomicU64,
    dropped: AtomicU64,
}

#[derive(Clone, Debug)]
struct BoundedExporter(Arc<ExportQueue>);

impl SpanExporter for BoundedExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let mut queue = self.0.spans.lock();
        if batch.len() > self.0.capacity.saturating_sub(queue.len()) {
            self.0
                .dropped
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            return Err(OTelSdkError::InternalFailure(
                "benchmark export capacity exceeded".into(),
            ));
        }
        self.0
            .accepted
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        queue.extend(batch);
        Ok(())
    }
}

enum Backend {
    Fast {
        collector: Arc<SpanCollector>,
        handle: LayerHandle,
    },
    Otel {
        provider: SdkTracerProvider,
        exporter: BoundedExporter,
    },
    Disabled,
}

enum Drained {
    Fast(Vec<CompletedSpan>),
    Otel(Vec<SpanData>),
    Disabled,
}

impl Backend {
    fn new(options: &Options) -> (Self, Dispatch) {
        match options.mode {
            Mode::Fast => {
                let collector = Arc::new(SpanCollector::new(1, 1));
                let layer = FastTelemetryLayer::with_capacity(
                    collector.clone(),
                    options.capacity,
                    options.capacity,
                );
                let handle = layer.handle();
                (
                    Self::Fast { collector, handle },
                    Dispatch::new(Registry::default().with(layer)),
                )
            }
            Mode::Otel => {
                let exporter = BoundedExporter(Arc::new(ExportQueue {
                    spans: Mutex::new(Vec::new()),
                    capacity: options.capacity,
                    accepted: AtomicU64::new(0),
                    dropped: AtomicU64::new(0),
                }));
                let provider = SdkTracerProvider::builder()
                    .with_sampler(Sampler::AlwaysOn)
                    .with_max_attributes_per_span(128)
                    .with_max_events_per_span(128)
                    .with_simple_exporter(exporter.clone())
                    .build();
                let layer = tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("matched-tracing-benchmark"))
                    .with_location(true)
                    .with_level(true)
                    .with_target(true)
                    .with_threads(false)
                    .with_tracked_inactivity(false)
                    .with_context_activation(false);
                (
                    Self::Otel { provider, exporter },
                    Dispatch::new(Registry::default().with(layer)),
                )
            }
            Mode::Disabled => (
                Self::Disabled,
                Dispatch::new(tracing::subscriber::NoSubscriber::default()),
            ),
        }
    }

    fn counters(&self) -> (u64, u64) {
        match self {
            Self::Fast { handle, .. } => (
                handle.accepted_span_count(),
                handle.dropped_span_count() + handle.dropped_event_count(),
            ),
            Self::Otel { exporter, .. } => (
                exporter.0.accepted.load(Ordering::Relaxed),
                exporter.0.dropped.load(Ordering::Relaxed),
            ),
            Self::Disabled => (0, 0),
        }
    }

    fn drain(&self) -> Result<Drained, String> {
        match self {
            Self::Fast { collector, handle } => {
                let mut spans = Vec::new();
                collector.drain_into(&mut spans);
                let mut standalone = Vec::new();
                handle.drain_events(&mut standalone);
                if !standalone.is_empty() {
                    return Err("unexpected standalone events".into());
                }
                Ok(Drained::Fast(spans))
            }
            Self::Otel { provider, exporter } => {
                provider.force_flush().map_err(|e| e.to_string())?;
                Ok(Drained::Otel(std::mem::take(&mut *exporter.0.spans.lock())))
            }
            Self::Disabled => Ok(Drained::Disabled),
        }
    }
}

#[derive(Default)]
struct Counts {
    spans: u64,
    events: u64,
    roots: u64,
    children: u64,
    user_fields: u64,
}

struct Shape {
    trace: Vec<u8>,
    id: Vec<u8>,
    parent: Vec<u8>,
    name: String,
    events: u64,
    valid_user_fields: u64,
}

fn verify(drained: &Drained, options: &Options, iterations: usize) -> Result<Counts, String> {
    let shapes: Vec<Shape> = match drained {
        Drained::Fast(spans) => spans.iter().map(|s| {
            let mut valid_user_fields = 0;
            for a in &s.attributes {
                if matches!((a.key.as_ref(), &a.value), ("worker" | "iteration", SpanValue::I64(_)) | ("code", SpanValue::I64(200)))
                    || matches!((a.key.as_ref(), &a.value), ("method", SpanValue::String(v)) if v == "GET") { valid_user_fields += 1; }
            }
            for event in &s.events {
                if event.attributes.iter().any(|a| a.key == "ok" && matches!(a.value, SpanValue::Bool(true))) { valid_user_fields += 1; }
                if event.attributes.iter().any(|a| a.key == "message" && matches!(&a.value, SpanValue::String(v) if v == "checkpoint")) { valid_user_fields += 1; }
            }
            Shape { trace: s.trace_id.as_bytes().to_vec(), id: s.span_id.as_bytes().to_vec(), parent: s.parent_span_id.as_bytes().to_vec(), name: s.name.to_string(), events: s.events.len() as u64, valid_user_fields }
        }).collect(),
        Drained::Otel(spans) => spans.iter().map(|s| {
            let mut valid_user_fields = 0;
            for a in &s.attributes {
                if matches!((a.key.as_str(), &a.value), ("worker" | "iteration", opentelemetry::Value::I64(_)) | ("code", opentelemetry::Value::I64(200)))
                    || matches!((a.key.as_str(), &a.value), ("method", opentelemetry::Value::String(v)) if v.as_str() == "GET") { valid_user_fields += 1; }
            }
            for event in &s.events.events {
                if event.attributes.iter().any(|a| a.key.as_str() == "ok" && matches!(a.value, opentelemetry::Value::Bool(true))) { valid_user_fields += 1; }
                if event.name == "checkpoint" { valid_user_fields += 1; }
            }
            Shape { trace: s.span_context.trace_id().to_bytes().to_vec(), id: s.span_context.span_id().to_bytes().to_vec(), parent: s.parent_span_id.to_bytes().to_vec(), name: s.name.to_string(), events: s.events.events.len() as u64, valid_user_fields }
        }).collect(),
        Drained::Disabled => Vec::new(),
    };
    let work = if options.mode == Mode::Disabled {
        0
    } else {
        (options.threads * iterations) as u64
    };
    if shapes.len() as u64 != work * options.workload.spans() {
        return Err(format!(
            "sink count mismatch: completed {}, expected {}",
            shapes.len(),
            work * options.workload.spans()
        ));
    }
    let mut names: HashMap<&str, u64> = HashMap::new();
    for shape in &shapes {
        *names.entry(&shape.name).or_default() += 1;
        if shape.trace.iter().all(|b| *b == 0) || shape.id.iter().all(|b| *b == 0) {
            return Err("invalid trace/span identity".into());
        }
    }
    let expected_names: &[&str] = match options.workload {
        Workload::Root => &["root"],
        Workload::Attributes => &["root", "child"],
        Workload::Children => &["root", "child_a", "child_b", "child_c"],
    };
    if expected_names
        .iter()
        .any(|name| names.get(name).copied().unwrap_or(0) != work)
    {
        return Err("sink workload-name count mismatch".into());
    }
    if let Drained::Otel(spans) = drained {
        for span in spans {
            if span.dropped_attributes_count != 0
                || span.events.dropped_count != 0
                || span
                    .events
                    .events
                    .iter()
                    .any(|e| e.dropped_attributes_count != 0)
            {
                return Err("OTel payload limits dropped fields/events".into());
            }
        }
    }
    let mut counts = Counts::default();
    let mut ids = HashSet::new();
    let mut roots = HashMap::new();
    for shape in &shapes {
        if !ids.insert((&shape.trace, &shape.id)) {
            return Err("duplicate span ID".into());
        }
        if shape.parent.iter().all(|b| *b == 0) {
            if shape.name != "root" {
                return Err("unexpected root name".into());
            }
            if roots.insert(&shape.trace, &shape.id).is_some() {
                return Err("multiple roots in trace".into());
            }
            counts.roots += 1;
        }
    }
    for shape in &shapes {
        if shape.parent.iter().any(|b| *b != 0) {
            if roots.get(&shape.trace).copied() != Some(&shape.parent) {
                return Err("child has missing/mismatched root".into());
            }
            let allowed = if options.workload == Workload::Attributes {
                shape.name == "child"
            } else {
                ["child_a", "child_b", "child_c"].contains(&shape.name.as_str())
            };
            if !allowed {
                return Err("unexpected child name".into());
            }
            counts.children += 1;
        }
        counts.spans += 1;
        counts.events += shape.events;
        counts.user_fields += shape.valid_user_fields;
    }
    let expected = (
        work * options.workload.spans(),
        work * options.workload.events(),
        work,
        work * (options.workload.spans() - 1),
        if options.workload == Workload::Attributes {
            work * 6
        } else {
            0
        },
    );
    let actual = (
        counts.spans,
        counts.events,
        counts.roots,
        counts.children,
        counts.user_fields,
    );
    if actual != expected {
        return Err(format!(
            "sink count mismatch: actual {actual:?}, expected {expected:?}"
        ));
    }
    Ok(counts)
}

fn process_cpu_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: valid initialized writable timespec; no aliases cross this call.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) };
    assert_eq!(result, 0, "process CPU clock unavailable");
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

#[derive(Default)]
struct Timings {
    record_wall: u128,
    record_cpu: u64,
    drain_wall: u128,
    drain_cpu: u64,
    spans: u64,
    events: u64,
    verify_wall: u128,
    verify_cpu: u64,
}

fn measure(
    backend: &Backend,
    dispatch: &Dispatch,
    options: &Options,
    iterations: usize,
) -> Result<Timings, String> {
    let barrier = Barrier::new(options.threads + 1);
    let mut timings = Timings::default();
    let mut failure = None;
    let baseline = backend.counters();
    std::thread::scope(|scope| {
        for worker in 0..options.threads {
            let barrier = &barrier;
            let dispatch = dispatch.clone();
            scope.spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    for start in (0..iterations).step_by(options.batch) {
                        barrier.wait();
                        for iteration in start..(start + options.batch).min(iterations) {
                            instrument(options.workload, worker, iteration);
                        }
                        barrier.wait();
                    }
                })
            });
        }
        // Keep the barrier protocol running after a verification failure so
        // all workers finish and join, then fail the process with the evidence.
        for start in (0..iterations).step_by(options.batch) {
            let cpu = process_cpu_ns();
            let wall = Instant::now();
            barrier.wait();
            barrier.wait();
            timings.record_wall += wall.elapsed().as_nanos();
            timings.record_cpu += process_cpu_ns() - cpu;
            let cpu = process_cpu_ns();
            let wall = Instant::now();
            let drained = backend.drain();
            timings.drain_wall += wall.elapsed().as_nanos();
            timings.drain_cpu += process_cpu_ns() - cpu;
            let cpu = process_cpu_ns();
            let wall = Instant::now();
            match drained
                .and_then(|data| verify(&data, options, options.batch.min(iterations - start)))
            {
                Ok(counts) => {
                    timings.spans += counts.spans;
                    timings.events += counts.events;
                }
                Err(error) => {
                    failure.get_or_insert(error);
                }
            }
            timings.verify_wall += wall.elapsed().as_nanos();
            timings.verify_cpu += process_cpu_ns() - cpu;
        }
    });
    if let Some(error) = failure {
        return Err(error);
    }
    let (accepted, dropped) = backend.counters();
    if dropped != baseline.1 || accepted - baseline.0 != timings.spans {
        return Err(format!(
            "loss/count mismatch: accepted={} drained={} dropped={}",
            accepted - baseline.0,
            timings.spans,
            dropped - baseline.1
        ));
    }
    Ok(timings)
}

fn run() -> Result<(), String> {
    let options = Options::parse()?;
    let (backend, dispatch) = Backend::new(&options);
    // Warmup runs the same retention/verification policy, fully drains, and is
    // excluded from all reported intervals and measured counter deltas.
    if options.warmup > 0 {
        measure(&backend, &dispatch, &options, options.warmup)?;
    }
    let total = Instant::now();
    let timings = measure(&backend, &dispatch, &options, options.iterations)?;
    let total_wall = total.elapsed().as_nanos();
    let work = (options.threads * options.iterations) as u64;
    let per_span = if timings.spans == 0 {
        "NA".to_owned()
    } else {
        format!("{:.3}", timings.record_cpu as f64 / timings.spans as f64)
    };
    let pipeline_cpu = timings.record_cpu + timings.drain_cpu;
    let pipeline_per_span = if timings.spans == 0 {
        "NA".to_owned()
    } else {
        format!("{:.3}", pipeline_cpu as f64 / timings.spans as f64)
    };
    println!(
        "mode,workload,threads,iters_per_thread,warmup_per_thread,batch_per_thread,capacity,work_iterations,completed_spans,events,dropped,record_wall_ns,record_cpu_ns,drain_wall_ns,drain_cpu_ns,verify_wall_ns,verify_cpu_ns,total_wall_ns,record_cpu_ns_per_iteration,record_cpu_ns_per_span,pipeline_cpu_ns_per_iteration,pipeline_cpu_ns_per_span"
    );
    println!(
        "{},{},{},{},{},{},{},{},{},{},0,{},{},{},{},{},{},{},{:.3},{},{:.3},{}",
        options.mode_name,
        options.workload_name,
        options.threads,
        options.iterations,
        options.warmup,
        options.batch,
        options.capacity,
        work,
        timings.spans,
        timings.events,
        timings.record_wall,
        timings.record_cpu,
        timings.drain_wall,
        timings.drain_cpu,
        timings.verify_wall,
        timings.verify_cpu,
        total_wall,
        timings.record_cpu as f64 / work as f64,
        per_span,
        pipeline_cpu as f64 / work as f64,
        pipeline_per_span
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

//! In-memory retention benchmark with explicit native and tracing frontends.
//! No network or protobuf serialization is timed. See ../../README.md.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use fast_telemetry::{
    CompletedSpan, Span, SpanAttribute, SpanCollector, SpanKind, SpanStatus, SpanValue,
};
use fast_telemetry_tracing::{FastTelemetryLayer, LayerHandle};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::error::{OTelSdkError, OTelSdkResult};
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter};
use parking_lot::Mutex;
use tracing::{Dispatch, Metadata};
use tracing_subscriber::Registry;
use tracing_subscriber::layer::SubscriberExt;

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Native,
    NativeMatched,
    Fast,
    Otel,
    Disabled,
    Registry,
}

impl Mode {
    fn is_native(self) -> bool {
        matches!(self, Self::Native | Self::NativeMatched)
    }

    fn retains(self) -> bool {
        !matches!(self, Self::Disabled | Self::Registry)
    }
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
                        "native" => Mode::Native,
                        "native-matched" => Mode::NativeMatched,
                        "fast" => Mode::Fast,
                        "otel" => Mode::Otel,
                        "disabled" => Mode::Disabled,
                        "registry" => Mode::Registry,
                        _ => return Err("mode must be native, native-matched, fast, otel, disabled, or registry".into()),
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
        // Native outboxes are fixed at 4096 spans PER WORKER, independently of
        // --capacity. Stay below their first adaptive-pressure boundary (1024)
        // for both measured and warmup batches. Every batch is then flushed and
        // drained before recording resumes. Do not change the native sampler.
        if options.mode.is_native()
            && options.batch.min(options.iterations.max(options.warmup))
                * options.workload.spans() as usize
                > 1024
        {
            return Err("native batch exceeds 1024 completed spans per worker; reduce --batch to avoid adaptive sampling pressure".into());
        }
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

// These are the exact same callsites for every tracing mode. Worker defaults
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

// Capture the exact static metadata of the shared tracing callsites before any
// backend is constructed or timing begins. This layer records no payloads. The
// matched native mode borrows these same static strings; it does not run tracing
// on its timed path or manufacture allocation work for the native API.
#[derive(Clone, Default)]
struct WorkloadMetadata {
    spans: HashMap<&'static str, &'static Metadata<'static>>,
    event: Option<&'static Metadata<'static>>,
}

struct NativeMetadata {
    root: &'static Metadata<'static>,
    children: [Option<&'static Metadata<'static>>; 3],
    event: Option<&'static Metadata<'static>>,
}

struct CaptureMetadata(Arc<Mutex<WorkloadMetadata>>);

impl tracing_subscriber::Layer<Registry> for CaptureMetadata {
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _: &tracing::span::Id,
        _: tracing_subscriber::layer::Context<'_, Registry>,
    ) {
        let metadata = attrs.metadata();
        self.0.lock().spans.insert(metadata.name(), metadata);
    }

    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _: tracing_subscriber::layer::Context<'_, Registry>,
    ) {
        self.0.lock().event = Some(event.metadata());
    }
}

impl WorkloadMetadata {
    fn capture(workload: Workload) -> Self {
        let captured = Arc::new(Mutex::new(Self::default()));
        let dispatch = Dispatch::new(Registry::default().with(CaptureMetadata(captured.clone())));
        tracing::dispatcher::with_default(&dispatch, || instrument(workload, 0, 0));
        captured.lock().clone()
    }

    fn native(&self, workload: Workload) -> NativeMetadata {
        let names = if workload == Workload::Attributes {
            ["child", "", ""]
        } else {
            ["child_a", "child_b", "child_c"]
        };
        NativeMetadata {
            root: self.spans["root"],
            children: names.map(|name| self.spans.get(name).copied()),
            event: self.event,
        }
    }
}

fn native_metadata(span: &mut Span, metadata: &'static Metadata<'static>) {
    visit_metadata(metadata, |attribute| {
        span.set_attribute(attribute.key, attribute.value);
    });
}

fn visit_metadata(metadata: &'static Metadata<'static>, mut emit: impl FnMut(SpanAttribute)) {
    emit(SpanAttribute::new(
        "fast_telemetry.tracing.level",
        metadata.level().as_str(),
    ));
    emit(SpanAttribute::new(
        "fast_telemetry.tracing.target",
        metadata.target(),
    ));
    if let Some(module) = metadata.module_path() {
        emit(SpanAttribute::new("fast_telemetry.code.module", module));
    }
    if let Some(file) = metadata.file() {
        emit(SpanAttribute::new("fast_telemetry.code.file", file));
    }
    if let Some(line) = metadata.line() {
        emit(SpanAttribute::new(
            "fast_telemetry.code.line",
            i64::from(line),
        ));
    }
}

#[inline(never)]
fn instrument_native(
    collector: &Arc<SpanCollector>,
    metadata: Option<&NativeMetadata>,
    workload: Workload,
    worker: usize,
    iteration: usize,
) {
    let mut root = collector.start_span("root", SpanKind::Internal);
    if workload == Workload::Attributes {
        root.set_attribute("worker", worker as i64);
        root.set_attribute("iteration", iteration as i64);
    }
    if let Some(metadata) = metadata {
        native_metadata(&mut root, metadata.root);
    }
    root.enter();
    match workload {
        Workload::Root => {
            std::hint::black_box(&root);
        }
        Workload::Attributes => {
            let mut child = root.child("child", SpanKind::Internal);
            child.set_attribute("method", "GET");
            child.set_attribute("code", 200i64);
            if let Some(metadata) = metadata {
                native_metadata(&mut child, metadata.children[0].expect("child callsite"));
            }
            child.enter();
            let mut attributes = vec![
                SpanAttribute::new("ok", true),
                SpanAttribute::new("message", "checkpoint"),
            ];
            let name = if let Some(metadata) = metadata {
                let event = metadata.event.expect("attributes callsite has an event");
                visit_metadata(event, |attribute| attributes.push(attribute));
                event.name()
            } else {
                "checkpoint"
            };
            child.add_event(name, attributes);
            std::hint::black_box(&child);
        }
        Workload::Children => {
            for (index, name) in ["child_a", "child_b", "child_c"].into_iter().enumerate() {
                let mut child = root.child(name, SpanKind::Internal);
                if let Some(metadata) = metadata {
                    native_metadata(
                        &mut child,
                        metadata.children[index].expect("child callsite"),
                    );
                }
                child.enter();
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
    Native {
        collector: Arc<SpanCollector>,
    },
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

#[derive(Default)]
struct Counters {
    accepted: u64,
    dropped: u64,
    sampled_out: u64,
    recorded_roots: u64,
}

impl Backend {
    fn new(options: &Options) -> (Self, Dispatch) {
        match options.mode {
            Mode::Native | Mode::NativeMatched => (
                Self::Native {
                    collector: Arc::new(SpanCollector::new(1, 1)),
                },
                Dispatch::new(tracing::subscriber::NoSubscriber::default()),
            ),
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
            Mode::Registry => (Self::Disabled, Dispatch::new(Registry::default())),
        }
    }

    fn counters(&self) -> Counters {
        match self {
            // Native recorded_count counts start_span roots, not Span::child.
            // Native dropped_count is always zero, so actual drains prove loss.
            Self::Native { collector } => Counters {
                recorded_roots: collector.recorded_count(),
                sampled_out: collector.sampled_out_count(),
                ..Counters::default()
            },
            Self::Fast { handle, .. } => Counters {
                accepted: handle.accepted_span_count(),
                dropped: handle.dropped_span_count() + handle.dropped_event_count(),
                ..Counters::default()
            },
            Self::Otel { exporter, .. } => Counters {
                accepted: exporter.0.accepted.load(Ordering::Relaxed),
                dropped: exporter.0.dropped.load(Ordering::Relaxed),
                ..Counters::default()
            },
            Self::Disabled => Counters::default(),
        }
    }

    fn drain(&self) -> Result<Drained, String> {
        match self {
            Self::Native { collector } => {
                let mut spans = Vec::new();
                collector.drain_into(&mut spans);
                Ok(Drained::Fast(spans))
            }
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
    payload_attributes: u64,
}

struct Shape {
    trace: Vec<u8>,
    id: Vec<u8>,
    parent: Vec<u8>,
    name: String,
    events: u64,
    valid_user_fields: u64,
    payload_attributes: u64,
}

#[derive(Debug, PartialEq, Eq)]
enum PayloadValue {
    String(String),
    I64(i64),
    Bool(bool),
}

fn attributes_map(attributes: &[SpanAttribute]) -> Result<HashMap<String, PayloadValue>, String> {
    let mut values = HashMap::new();
    for attribute in attributes {
        let value = match &attribute.value {
            SpanValue::String(value) => PayloadValue::String(value.to_string()),
            SpanValue::I64(value) => PayloadValue::I64(*value),
            SpanValue::Bool(value) => PayloadValue::Bool(*value),
            _ => return Err("unexpected attribute type in benchmark payload".into()),
        };
        if values.insert(attribute.key.to_string(), value).is_some() {
            return Err("duplicate attribute in benchmark payload".into());
        }
    }
    Ok(values)
}

fn expected_metadata(metadata: &'static Metadata<'static>) -> HashMap<String, PayloadValue> {
    let mut attributes = Vec::new();
    visit_metadata(metadata, |attribute| attributes.push(attribute));
    attributes_map(&attributes).expect("metadata has unique supported attributes")
}

fn verify_native_payloads(
    spans: &[CompletedSpan],
    options: &Options,
    start: usize,
    iterations: usize,
    metadata: &WorkloadMetadata,
) -> Result<(), String> {
    let with_metadata = options.mode != Mode::Native;
    let mut root_work = HashSet::new();
    for span in spans {
        let callsite = metadata
            .spans
            .get(span.name.as_ref())
            .ok_or("unknown span name")?;
        let actual = attributes_map(&span.attributes)?;
        let mut expected = if with_metadata {
            expected_metadata(callsite)
        } else {
            HashMap::new()
        };
        if options.workload == Workload::Attributes {
            if span.name == "root" {
                let Some(PayloadValue::I64(worker)) = actual.get("worker") else {
                    return Err("root worker field missing or mistyped".into());
                };
                let Some(PayloadValue::I64(iteration)) = actual.get("iteration") else {
                    return Err("root iteration field missing or mistyped".into());
                };
                if *worker < 0
                    || *worker as usize >= options.threads
                    || *iteration < 0
                    || (*iteration as usize) < start
                    || *iteration as usize >= start + iterations
                    || !root_work.insert((*worker, *iteration))
                {
                    return Err("root worker/iteration payload mismatch".into());
                }
                expected.insert("worker".into(), PayloadValue::I64(*worker));
                expected.insert("iteration".into(), PayloadValue::I64(*iteration));
            } else if span.name == "child" {
                expected.insert("method".into(), PayloadValue::String("GET".into()));
                expected.insert("code".into(), PayloadValue::I64(200));
            }
        }
        if actual != expected {
            return Err(format!(
                "{} span attribute payload mismatch: actual {actual:?}, expected {expected:?}",
                span.name
            ));
        }
        if span.kind != SpanKind::Internal
            || span.status != SpanStatus::Unset
            || span.start_time_ns == 0
            || span.end_time_ns < span.start_time_ns
        {
            return Err("span lifecycle payload mismatch".into());
        }
        let expected_events =
            usize::from(options.workload == Workload::Attributes && span.name == "child");
        if span.events.len() != expected_events {
            return Err("span event placement/count mismatch".into());
        }
        for event in &span.events {
            let callsite = metadata.event.ok_or("unexpected event")?;
            let mut expected = if with_metadata {
                expected_metadata(callsite)
            } else {
                HashMap::new()
            };
            expected.insert("ok".into(), PayloadValue::Bool(true));
            expected.insert("message".into(), PayloadValue::String("checkpoint".into()));
            let name = if with_metadata {
                callsite.name()
            } else {
                "checkpoint"
            };
            if event.name != name
                || attributes_map(&event.attributes)? != expected
                || event.time_ns < span.start_time_ns
                || event.time_ns > span.end_time_ns
            {
                return Err("event attribute/name/timestamp payload mismatch".into());
            }
        }
    }
    Ok(())
}

fn verify(
    drained: &Drained,
    options: &Options,
    start: usize,
    iterations: usize,
    metadata: &WorkloadMetadata,
) -> Result<Counts, String> {
    if let Drained::Fast(spans) = drained {
        verify_native_payloads(spans, options, start, iterations, metadata)?;
    }
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
            Shape { trace: s.trace_id.as_bytes().to_vec(), id: s.span_id.as_bytes().to_vec(), parent: s.parent_span_id.as_bytes().to_vec(), name: s.name.to_string(), events: s.events.len() as u64, valid_user_fields, payload_attributes: (s.attributes.len() + s.events.iter().map(|e| e.attributes.len()).sum::<usize>()) as u64 }
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
            Shape { trace: s.span_context.trace_id().to_bytes().to_vec(), id: s.span_context.span_id().to_bytes().to_vec(), parent: s.parent_span_id.to_bytes().to_vec(), name: s.name.to_string(), events: s.events.events.len() as u64, valid_user_fields, payload_attributes: (s.attributes.len() + s.events.events.iter().map(|e| e.attributes.len()).sum::<usize>()) as u64 }
        }).collect(),
        Drained::Disabled => Vec::new(),
    };
    let work = if !options.mode.retains() {
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
        counts.payload_attributes += shape.payload_attributes;
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
    destroy_wall: u128,
    destroy_cpu: u64,
    total_wall: u128,
    sampled_out: u64,
    recorded_roots: u64,
    payload_attributes: u64,
}

fn worker_batches(
    options: &Options,
    barrier: &Barrier,
    mut record: impl FnMut(usize),
    flush: impl Fn(),
) {
    for iterations in [options.warmup, options.iterations] {
        for start in (0..iterations).step_by(options.batch) {
            barrier.wait();
            for iteration in start..(start + options.batch).min(iterations) {
                record(iteration);
            }
            // Native partial TLS batches must be published by their OWN worker
            // before the coordinator drains. This is inside record timing.
            flush();
            barrier.wait();
        }
    }
    // Keep workers alive through the final drain, verification and destruction.
    barrier.wait();
}

fn verify_counters(
    backend: &Backend,
    baseline: &Counters,
    timings: &mut Timings,
    roots: u64,
) -> Result<(), String> {
    let counters = backend.counters();
    timings.sampled_out = counters.sampled_out - baseline.sampled_out;
    timings.recorded_roots = counters.recorded_roots - baseline.recorded_roots;
    if counters.dropped != baseline.dropped || timings.sampled_out != 0 {
        return Err("sink rejected spans/events or native adaptive sampling omitted roots".into());
    }
    if let Backend::Native { .. } = backend {
        if timings.recorded_roots != roots {
            return Err("native root recording counter mismatch".into());
        }
    } else if counters.accepted - baseline.accepted != timings.spans {
        return Err("accepted counter differs from actual drained spans".into());
    }
    Ok(())
}

fn measure(
    backend: &Backend,
    dispatch: &Dispatch,
    options: &Options,
    metadata: &WorkloadMetadata,
) -> Result<Timings, String> {
    let barrier = Barrier::new(options.threads + 1);
    let native_metadata = metadata.native(options.workload);
    let mut timings = Timings::default();
    let mut failure = None;
    let mut measured_start = None;
    std::thread::scope(|scope| {
        for worker in 0..options.threads {
            let barrier = &barrier;
            let dispatch = dispatch.clone();
            let native_metadata = &native_metadata;
            scope.spawn(move || {
                if let Backend::Native { collector } = backend {
                    let matched = (options.mode == Mode::NativeMatched).then_some(native_metadata);
                    worker_batches(
                        options,
                        barrier,
                        |iteration| {
                            instrument_native(
                                collector,
                                matched,
                                options.workload,
                                worker,
                                iteration,
                            )
                        },
                        || collector.flush_local(),
                    );
                } else {
                    tracing::dispatcher::with_default(&dispatch, || {
                        worker_batches(
                            options,
                            barrier,
                            |iteration| instrument(options.workload, worker, iteration),
                            || {},
                        )
                    });
                }
            });
        }
        // Keep the barrier protocol running after a verification failure so
        // all workers finish and join, then fail the process with the evidence.
        // Resident workers warm their TLS, registry and queues, then continue
        // into measurement. No extra native warmup outboxes are registered.
        for (iterations, measured) in [(options.warmup, false), (options.iterations, true)] {
            let mut phase = Timings::default();
            let baseline = backend.counters();
            if measured {
                measured_start = Some(Instant::now());
            }
            for start in (0..iterations).step_by(options.batch) {
                let cpu = process_cpu_ns();
                let wall = Instant::now();
                barrier.wait();
                barrier.wait();
                phase.record_wall += wall.elapsed().as_nanos();
                phase.record_cpu += process_cpu_ns() - cpu;
                let cpu = process_cpu_ns();
                let wall = Instant::now();
                let drained = backend.drain();
                phase.drain_wall += wall.elapsed().as_nanos();
                phase.drain_cpu += process_cpu_ns() - cpu;
                let cpu = process_cpu_ns();
                let wall = Instant::now();
                match drained
                    .as_ref()
                    .map_err(|e| e.to_string())
                    .and_then(|data| {
                        verify(
                            data,
                            options,
                            start,
                            options.batch.min(iterations - start),
                            metadata,
                        )
                    }) {
                    Ok(counts) => {
                        phase.spans += counts.spans;
                        phase.events += counts.events;
                        phase.payload_attributes += counts.payload_attributes;
                    }
                    Err(error) => {
                        failure.get_or_insert(error);
                    }
                }
                phase.verify_wall += wall.elapsed().as_nanos();
                phase.verify_cpu += process_cpu_ns() - cpu;
                let cpu = process_cpu_ns();
                let wall = Instant::now();
                drop(drained);
                phase.destroy_wall += wall.elapsed().as_nanos();
                phase.destroy_cpu += process_cpu_ns() - cpu;
            }
            let roots = if options.mode.retains() {
                (options.threads * iterations) as u64
            } else {
                0
            };
            if let Err(error) = verify_counters(backend, &baseline, &mut phase, roots) {
                failure.get_or_insert(error);
            }
            if measured {
                timings = phase;
            }
        }
        barrier.wait();
    });
    timings.total_wall = measured_start
        .expect("measured phase always runs")
        .elapsed()
        .as_nanos();
    if let Some(error) = failure {
        return Err(error);
    }
    Ok(timings)
}

fn run() -> Result<(), String> {
    let options = Options::parse()?;
    let metadata = WorkloadMetadata::capture(options.workload);
    let (backend, dispatch) = Backend::new(&options);
    // Warmup runs the same retention/verification policy, fully drains, and is
    // excluded from all reported intervals and measured counter deltas.
    let timings = measure(&backend, &dispatch, &options, &metadata)?;
    let work = (options.threads * options.iterations) as u64;
    let per_span = if timings.spans == 0 {
        "NA".to_owned()
    } else {
        format!("{:.3}", timings.record_cpu as f64 / timings.spans as f64)
    };
    let pipeline_cpu = timings.record_cpu + timings.drain_cpu;
    let pipeline_with_destroy_cpu = pipeline_cpu + timings.destroy_cpu;
    let pipeline_per_span = if timings.spans == 0 {
        "NA".to_owned()
    } else {
        format!("{:.3}", pipeline_cpu as f64 / timings.spans as f64)
    };
    let pipeline_with_destroy_per_span = if timings.spans == 0 {
        "NA".to_owned()
    } else {
        format!(
            "{:.3}",
            pipeline_with_destroy_cpu as f64 / timings.spans as f64
        )
    };
    println!(
        "mode,workload,threads,iters_per_thread,warmup_per_thread,batch_per_thread,capacity,work_iterations,completed_spans,events,dropped,record_wall_ns,record_cpu_ns,drain_wall_ns,drain_cpu_ns,verify_wall_ns,verify_cpu_ns,total_wall_ns,record_cpu_ns_per_iteration,record_cpu_ns_per_span,pipeline_cpu_ns_per_iteration,pipeline_cpu_ns_per_span,sampled_out,recorded_roots,payload_attributes,destroy_wall_ns,destroy_cpu_ns,pipeline_with_destroy_cpu_ns_per_iteration,pipeline_with_destroy_cpu_ns_per_span,attempted_spans"
    );
    println!(
        "{},{},{},{},{},{},{},{},{},{},0,{},{},{},{},{},{},{},{:.3},{},{:.3},{},{},{},{},{},{},{:.3},{},{}",
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
        timings.total_wall,
        timings.record_cpu as f64 / work as f64,
        per_span,
        pipeline_cpu as f64 / work as f64,
        pipeline_per_span,
        timings.sampled_out,
        timings.recorded_roots,
        timings.payload_attributes,
        timings.destroy_wall,
        timings.destroy_cpu,
        pipeline_with_destroy_cpu as f64 / work as f64,
        pipeline_with_destroy_per_span,
        work * options.workload.spans()
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

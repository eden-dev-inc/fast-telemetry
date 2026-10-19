//! Lossless recording through real OTLP encoding, with a common transport.
//! Capture stops at the HTTP client boundary; HTTP includes acknowledgement.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use fast_telemetry::otlp::{build_resource, encode_native_trace_export_request_batches, pb};
use fast_telemetry::{SpanAttribute, SpanBatch, SpanCollector, SpanKind};
use fast_telemetry_tracing::{FastTelemetrySubscriber, LayerHandle, MetadataPolicy};
use http::{Request, Response};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_http::{HttpClient, HttpError};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{
    BatchConfigBuilder, BatchSpanProcessor, Sampler, SdkTracerProvider,
};
use parking_lot::Mutex;
use prost::Message;
use tracing_subscriber::layer::SubscriberExt;

const SERVICE: &str = "tracing-end-to-end";
const SCOPE: &str = "end-to-end";

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Native,
    DropIn,
    Otel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Root,
    Children,
    Attributes,
}

impl Workload {
    fn spans(self) -> usize {
        match self {
            Self::Root => 1,
            Self::Children => 4,
            Self::Attributes => 2,
        }
    }
}

struct Options {
    mode: Mode,
    workload: Workload,
    threads: usize,
    batch: usize,
    cycles: usize,
    warmup: usize,
    endpoint: String,
    http: bool,
}

impl Options {
    fn parse() -> Result<Self> {
        let args: Vec<_> = std::env::args().skip(1).collect();
        Self::from_args(&args)
    }

    fn from_args(args: &[String]) -> Result<Self> {
        if args.len() != 7 {
            return Err("usage: tracing-end-to-end native|drop-in|otel root|children|attributes THREADS SPANS_PER_THREAD_BATCH CYCLES WARMUP capture|http://HOST:PORT".into());
        }
        let mode = match args[0].as_str() {
            "native" => Mode::Native,
            "drop-in" => Mode::DropIn,
            "otel" => Mode::Otel,
            _ => return Err("unknown backend".into()),
        };
        let workload = match args[1].as_str() {
            "root" => Workload::Root,
            "children" => Workload::Children,
            "attributes" => Workload::Attributes,
            _ => return Err("unknown workload".into()),
        };
        let threads = args[2].parse::<usize>()?;
        let batch = args[3].parse::<usize>()?;
        let cycles = args[4].parse::<usize>()?;
        let warmup = args[5].parse::<usize>()?;
        if threads == 0 || threads > 128 || batch == 0 || batch > 1024 || cycles == 0 {
            return Err(
                "require 1..=128 threads, 1..=1024 spans per batch, and positive cycles".into(),
            );
        }
        if !batch.is_multiple_of(workload.spans()) {
            return Err("batch must contain a whole number of traces".into());
        }
        threads
            .checked_mul(batch)
            .and_then(|n| n.checked_mul(cycles.checked_add(warmup)?))
            .filter(|&n| n <= i64::MAX as usize)
            .ok_or("work count overflow")?;
        let http = args[6] != "capture";
        let endpoint = if http {
            let url = reqwest::Url::parse(&args[6])?;
            if url.scheme() != "http"
                || url.host_str().is_none()
                || !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
                || !matches!(url.path(), "" | "/")
            {
                return Err("HTTP mode requires an http://HOST:PORT base endpoint".into());
            }
            args[6].trim_end_matches('/').to_owned()
        } else {
            "http://127.0.0.1:4318".to_owned()
        };
        Ok(Self {
            mode,
            workload,
            threads,
            batch,
            cycles,
            warmup,
            endpoint,
            http,
        })
    }
}

/// Both backends transfer the final encoded allocation here without copying it.
#[derive(Clone, Debug)]
struct WireClient {
    bodies: Arc<Mutex<Vec<Bytes>>>,
    http: Option<reqwest::blocking::Client>,
}

#[async_trait]
impl HttpClient for WireClient {
    async fn send_bytes(
        &self,
        request: Request<Bytes>,
    ) -> std::result::Result<Response<Bytes>, HttpError> {
        if request.method() != http::Method::POST
            || request.uri().path() != "/v1/traces"
            || request
                .headers()
                .get(http::header::CONTENT_TYPE)
                .map(|v| v.as_bytes())
                != Some(b"application/x-protobuf".as_slice())
            || request
                .headers()
                .contains_key(http::header::CONTENT_ENCODING)
        {
            return Err("unexpected OTLP HTTP envelope".into());
        }
        // One Bytes clone in every mode keeps the actual wire allocation alive
        // for verification outside timing. Its final release is timed separately.
        let body = request.body().clone();
        let response = if let Some(client) = &self.http {
            let response = client.send_bytes(request).await?;
            if !response.status().is_success() {
                return Err(format!("collector returned {}", response.status()).into());
            }
            let ack = pb::ExportTraceServiceResponse::decode(response.body().as_ref())?;
            if ack.partial_success.is_some_and(|p| p.rejected_spans != 0) {
                return Err("collector rejected spans".into());
            }
            response
        } else {
            Response::builder().status(200).body(Bytes::new())?
        };
        self.bodies.lock().push(body);
        Ok(response)
    }
}

fn native_trace(
    collector: &Arc<SpanCollector>,
    workload: Workload,
    worker: usize,
    iteration: usize,
) {
    let mut root = collector.start_span("root", SpanKind::Internal);
    if workload == Workload::Attributes {
        root.set_attribute("worker", worker as i64);
        root.set_attribute("iteration", iteration as i64);
    }
    root.enter();
    match workload {
        Workload::Root => {}
        Workload::Children => {
            for name in ["child_a", "child_b", "child_c"] {
                let mut child = root.child(name, SpanKind::Internal);
                child.enter();
                std::hint::black_box(&child);
            }
        }
        Workload::Attributes => {
            let mut child = root.child("child", SpanKind::Internal);
            child.set_attribute("method", "GET");
            child.set_attribute("code", 200i64);
            child.enter();
            // tracing-opentelemetry always emits event level/target. Retain the
            // same fields explicitly in both FT frontends for output parity.
            child.add_event(
                "checkpoint",
                vec![
                    SpanAttribute::new("ok", true),
                    SpanAttribute::new("level", "INFO"),
                    SpanAttribute::new("target", "e2e"),
                ],
            );
        }
    }
    std::hint::black_box(&root);
}

fn tracing_trace(mode: Mode, workload: Workload, worker: usize, iteration: usize) {
    match workload {
        Workload::Root => {
            let root = tracing::info_span!(parent: None, "root");
            let _entered = root.enter();
            std::hint::black_box(&root);
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
        Workload::Attributes => {
            let root = tracing::info_span!(parent: None, "root", worker = worker as i64, iteration = iteration as i64);
            let _root = root.enter();
            let child = tracing::info_span!("child", method = "GET", code = 200i64);
            let _child = child.enter();
            if mode == Mode::Otel {
                tracing::event!(name: "checkpoint", target: "e2e", tracing::Level::INFO, ok = true);
            } else {
                tracing::event!(name: "checkpoint", target: "e2e", tracing::Level::INFO, ok = true, level = "INFO", target = "e2e");
            }
            std::hint::black_box(&child);
        }
    }
}

fn record_batch(
    collector: &Arc<SpanCollector>,
    mode: Mode,
    workload: Workload,
    worker: usize,
    traces: usize,
    cycle: usize,
) {
    if mode == Mode::Native {
        for index in 0..traces {
            native_trace(collector, workload, worker, cycle * traces + index);
        }
        collector.flush_local();
    } else {
        for index in 0..traces {
            tracing_trace(mode, workload, worker, cycle * traces + index);
        }
    }
}

fn setup(
    options: &Options,
    collector: &Arc<SpanCollector>,
    wire: &WireClient,
) -> Result<(Option<SdkTracerProvider>, Option<LayerHandle>)> {
    match options.mode {
        Mode::Native => Ok((None, None)),
        Mode::DropIn => {
            let subscriber = FastTelemetrySubscriber::with_capacity(
                collector.clone(),
                options.threads * 4096,
                options.threads * 4096,
            )
            .with_metadata_policy(MetadataPolicy::UserFields);
            let handle = subscriber.handle();
            tracing::subscriber::set_global_default(subscriber)?;
            Ok((None, Some(handle)))
        }
        Mode::Otel => {
            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .with_endpoint(format!("{}/v1/traces", options.endpoint))
                .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
                .with_http_client(wire.clone())
                .build()?;
            let processor = BatchSpanProcessor::builder(exporter)
                .with_batch_config(
                    BatchConfigBuilder::default()
                        .with_max_queue_size(options.threads * 4096)
                        .with_max_export_batch_size(options.threads * options.batch)
                        .with_scheduled_delay(Duration::from_secs(3600))
                        .build(),
                )
                .build();
            let provider = SdkTracerProvider::builder()
                .with_sampler(Sampler::AlwaysOn)
                .with_resource(
                    Resource::builder_empty()
                        .with_attribute(KeyValue::new("service.name", SERVICE))
                        .build(),
                )
                .with_span_processor(processor)
                .build();
            let layer = tracing_opentelemetry::layer()
                .with_tracer(provider.tracer(SCOPE))
                .with_location(false)
                .with_level(false)
                .with_threads(false)
                .with_tracked_inactivity(false);
            tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer))?;
            Ok((Some(provider), None))
        }
    }
}

fn cpu_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: time points to initialized, exclusively borrowed writable storage.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut time) };
    assert_eq!(result, 0, "process CPU accounting failed");
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}

#[derive(Debug, PartialEq, Eq)]
enum Value {
    Integer(i64),
    String(String),
    Boolean(bool),
}

fn attributes(values: &[pb::KeyValue]) -> Result<BTreeMap<&str, Value>> {
    let mut output = BTreeMap::new();
    for kv in values {
        let value = match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
            Some(pb::any_value::Value::IntValue(v)) => Value::Integer(*v),
            Some(pb::any_value::Value::StringValue(v)) => Value::String(v.clone()),
            Some(pb::any_value::Value::BoolValue(v)) => Value::Boolean(*v),
            _ => return Err("unexpected attribute type".into()),
        };
        if output.insert(kv.key.as_str(), value).is_some() {
            return Err("duplicate attribute key".into());
        }
    }
    Ok(output)
}

fn verify(bodies: &[Bytes], options: &Options, cycle: usize) -> Result<(usize, usize)> {
    let mut spans = Vec::new();
    for body in bodies {
        let request = pb::ExportTraceServiceRequest::decode(body.as_ref())?;
        if request.resource_spans.len() != 1 {
            return Err("expected one resource per request".into());
        }
        let mut resources = request.resource_spans.into_iter();
        let resource = resources.next().ok_or("missing resource")?;
        if !resource.schema_url.is_empty() || resource.scope_spans.len() != 1 {
            return Err("unexpected resource schema or scope count".into());
        }
        let expected = BTreeMap::from([("service.name", Value::String(SERVICE.into()))]);
        if attributes(
            &resource
                .resource
                .ok_or("missing resource attributes")?
                .attributes,
        )? != expected
        {
            return Err("resource attributes differ".into());
        }
        let scope = resource
            .scope_spans
            .into_iter()
            .next()
            .ok_or("missing scope")?;
        let metadata = scope.scope.ok_or("missing instrumentation scope")?;
        if metadata.name != SCOPE
            || !metadata.version.is_empty()
            || !metadata.attributes.is_empty()
            || !scope.schema_url.is_empty()
            || scope.spans.len() != options.threads * options.batch
        {
            return Err("scope metadata or export batch size differs".into());
        }
        spans.extend(scope.spans);
    }
    let expected_spans = options.threads * options.batch;
    if spans.len() != expected_spans || bodies.len() != 1 {
        return Err(format!(
            "incomplete export: {} spans in {} requests, expected {expected_spans} in one request",
            spans.len(),
            bodies.len()
        )
        .into());
    }
    let mut ids = HashSet::new();
    let mut roots = HashMap::new();
    let mut seen_iterations = HashSet::new();
    let trace_count = options.threads * options.batch / options.workload.spans();
    let traces_per_worker = options.batch / options.workload.spans();
    for span in &spans {
        if span.trace_id.len() != 16
            || span.trace_id.iter().all(|&b| b == 0)
            || span.span_id.len() != 8
            || span.span_id.iter().all(|&b| b == 0)
            || !ids.insert(span.span_id.as_slice())
            || span.start_time_unix_nano == 0
            || span.end_time_unix_nano < span.start_time_unix_nano
            || span.kind != pb::OtlpSpanKind::Internal as i32
            || !span.trace_state.is_empty()
            || !span.links.is_empty()
            || span.dropped_attributes_count != 0
            || span.dropped_events_count != 0
            || span.dropped_links_count != 0
            || span
                .status
                .as_ref()
                .is_some_and(|s| s.code != 0 || !s.message.is_empty())
        {
            return Err("invalid span identity, timestamps, limits, kind or status".into());
        }
        if span.name == "root" {
            if !span.parent_span_id.is_empty()
                || !span.events.is_empty()
                || roots.insert(span.trace_id.as_slice(), span).is_some()
            {
                return Err("invalid or duplicate root".into());
            }
            let attrs = attributes(&span.attributes)?;
            if options.workload == Workload::Attributes {
                let (Some(Value::Integer(worker)), Some(Value::Integer(iteration))) =
                    (attrs.get("worker"), attrs.get("iteration"))
                else {
                    return Err("missing root attributes".into());
                };
                if attrs.len() != 2
                    || *worker < 0
                    || *worker >= options.threads as i64
                    || *iteration < (cycle * traces_per_worker) as i64
                    || *iteration >= ((cycle + 1) * traces_per_worker) as i64
                    || !seen_iterations.insert((*worker, *iteration))
                {
                    return Err("root attribute payload differs".into());
                }
            } else if !attrs.is_empty() {
                return Err("unexpected root attributes".into());
            }
        }
    }
    if roots.len() != trace_count {
        return Err("missing root traces".into());
    }
    let mut children = HashSet::new();
    let mut events = 0;
    for span in &spans {
        if span.name == "root" {
            continue;
        }
        let root = roots.get(span.trace_id.as_slice()).ok_or("orphan child")?;
        if span.parent_span_id != root.span_id
            || span.start_time_unix_nano < root.start_time_unix_nano
            || span.end_time_unix_nano > root.end_time_unix_nano
            || !children.insert((span.trace_id.as_slice(), span.name.as_str()))
        {
            return Err("invalid child graph or timestamps".into());
        }
        if options.workload == Workload::Attributes {
            if span.name != "child"
                || attributes(&span.attributes)?
                    != BTreeMap::from([
                        ("method", Value::String("GET".into())),
                        ("code", Value::Integer(200)),
                    ])
                || span.events.len() != 1
            {
                return Err("child payload differs".into());
            }
            let event = &span.events[0];
            if event.name != "checkpoint"
                || event.time_unix_nano < span.start_time_unix_nano
                || event.time_unix_nano > span.end_time_unix_nano
                || event.dropped_attributes_count != 0
                || attributes(&event.attributes)?
                    != BTreeMap::from([
                        ("ok", Value::Boolean(true)),
                        ("level", Value::String("INFO".into())),
                        ("target", Value::String("e2e".into())),
                    ])
            {
                return Err("event payload differs".into());
            }
            events += 1;
        } else if options.workload != Workload::Children
            || !matches!(span.name.as_str(), "child_a" | "child_b" | "child_c")
            || !span.attributes.is_empty()
            || !span.events.is_empty()
        {
            return Err("unexpected child payload".into());
        }
    }
    let expected_events = if options.workload == Workload::Attributes {
        trace_count
    } else {
        0
    };
    if events != expected_events {
        return Err("event count differs".into());
    }
    Ok((spans.len(), events))
}

fn run() -> Result<()> {
    let options = Options::parse()?;
    let wire = WireClient {
        bodies: Arc::new(Mutex::new(Vec::new())),
        http: if options.http {
            Some(
                reqwest::blocking::Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(10))
                    .build()?,
            )
        } else {
            None
        },
    };
    let collector = Arc::new(SpanCollector::new(options.threads, 4096));
    let (provider, adapter) = setup(&options, &collector, &wire)?;
    let worker_count = if options.threads == 1 {
        0
    } else {
        options.threads
    };
    let start = Arc::new(Barrier::new(worker_count + 1));
    let done = Arc::new(Barrier::new(worker_count + 1));
    let mut workers = Vec::new();
    for worker in 0..worker_count {
        let (start, done, collector) = (start.clone(), done.clone(), collector.clone());
        let (mode, workload, traces, cycles) = (
            options.mode,
            options.workload,
            options.batch / options.workload.spans(),
            options.cycles + options.warmup,
        );
        workers.push(std::thread::spawn(move || {
            for cycle in 0..cycles {
                start.wait();
                record_batch(&collector, mode, workload, worker, traces, cycle);
                done.wait();
            }
            // Keep producer TLS and empty storage alive through final cleanup.
            start.wait();
        }));
    }
    let resource = build_resource(SERVICE, &[]);
    let uri = format!("{}/v1/traces", options.endpoint);
    let mut batches = SpanBatch::new();
    let (mut total_cpu, mut total_wall, mut bytes, mut requests, mut exported, mut events) =
        (0u64, 0u128, 0usize, 0usize, 0usize, 0usize);
    let mut seen_measured_ids = HashSet::new();
    for cycle in 0..options.warmup + options.cycles {
        let sampled = collector.sampled_out_count();
        let recorded = collector.recorded_count();
        let cpu = cpu_ns();
        let wall = Instant::now();
        start.wait();
        if worker_count == 0 {
            record_batch(
                &collector,
                options.mode,
                options.workload,
                0,
                options.batch / options.workload.spans(),
                cycle,
            );
        }
        done.wait();
        if let Some(provider) = &provider {
            provider.force_flush()?;
        } else {
            collector.drain_batches(&mut batches);
            let encoded =
                encode_native_trace_export_request_batches(&resource, SCOPE, &batches, usize::MAX);
            let request = Request::builder()
                .method("POST")
                .uri(&uri)
                .header(http::header::CONTENT_TYPE, "application/x-protobuf")
                .body(Bytes::from(encoded.into_body()))?;
            futures_executor::block_on(wire.send_bytes(request))?;
        }
        let mut bodies = std::mem::take(&mut *wire.bodies.lock());
        let mut elapsed_wall = wall.elapsed().as_nanos();
        let mut elapsed_cpu = cpu_ns() - cpu;
        let (count, event_count) = verify(&bodies, &options, cycle)?;
        if collector.sampled_out_count() != sampled {
            return Err("native sampled a root".into());
        }
        if options.mode == Mode::Native
            && collector.recorded_count() - recorded
                != (options.threads * options.batch / options.workload.spans()) as u64
        {
            return Err("native root accounting differs".into());
        }
        if let Some(handle) = &adapter
            && (handle.dropped_span_count() != 0 || handle.dropped_event_count() != 0)
        {
            return Err("adapter rejected telemetry".into());
        }
        if cycle >= options.warmup {
            for body in &bodies {
                let decoded = pb::ExportTraceServiceRequest::decode(body.as_ref())?;
                for resource in decoded.resource_spans {
                    for scope in resource.scope_spans {
                        for span in scope.spans {
                            if !seen_measured_ids.insert(span.span_id) {
                                return Err("span id reused across measured batches".into());
                            }
                        }
                    }
                }
            }
            bytes += bodies.iter().map(Bytes::len).sum::<usize>();
            requests += bodies.len();
            exported += count;
            events += event_count;
        }
        let cpu = cpu_ns();
        let wall = Instant::now();
        // Actual native payload and wire-body releases are part of the result.
        // Verification's decoded protobuf objects were already dropped above.
        bodies.clear();
        batches.clear();
        *wire.bodies.lock() = bodies;
        elapsed_wall += wall.elapsed().as_nanos();
        elapsed_cpu += cpu_ns() - cpu;
        if cycle >= options.warmup {
            total_cpu += elapsed_cpu;
            total_wall += elapsed_wall;
        }
    }
    start.wait();
    for worker in workers {
        worker.join().map_err(|_| "recording worker panicked")?;
    }
    if let Some(provider) = provider {
        provider.shutdown()?;
    }
    if !wire.bodies.lock().is_empty() {
        return Err("unaccounted export after measurement".into());
    }
    let traces = exported / options.workload.spans();
    println!(
        "mode,workload,transport,threads,batch_spans_per_thread,cycles,warmup,verified_traces,verified_spans,verified_events,wire_requests,wire_bytes,sampled_out,dropped,pipeline_cpu_ns_per_trace,pipeline_cpu_ns_per_span,pipeline_wall_ns_per_trace,pipeline_traces_per_sec,pipeline_spans_per_sec,process_cpu_ns,wall_ns"
    );
    println!(
        "{:?},{:?},{},{},{},{},{},{traces},{exported},{events},{requests},{bytes},0,0,{:.3},{:.3},{:.3},{:.3},{:.3},{total_cpu},{total_wall}",
        options.mode,
        options.workload,
        if options.http { "http" } else { "capture" },
        options.threads,
        options.batch,
        options.cycles,
        options.warmup,
        total_cpu as f64 / traces as f64,
        total_cpu as f64 / exported as f64,
        total_wall as f64 / traces as f64,
        traces as f64 * 1e9 / total_wall as f64,
        exported as f64 * 1e9 / total_wall as f64
    );
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(workload: Workload) -> Options {
        Options {
            mode: Mode::Native,
            workload,
            threads: 1,
            batch: 4,
            cycles: 1,
            warmup: 0,
            endpoint: "http://127.0.0.1:4318".into(),
            http: false,
        }
    }

    fn fixture(options: &Options) -> pb::ExportTraceServiceRequest {
        let collector = Arc::new(SpanCollector::new(1, 4096));
        for index in 0..options.batch / options.workload.spans() {
            native_trace(&collector, options.workload, 0, index);
        }
        collector.flush_local();
        let mut batch = SpanBatch::new();
        collector.drain_batches(&mut batch);
        let encoded = encode_native_trace_export_request_batches(
            &build_resource(SERVICE, &[]),
            SCOPE,
            &batch,
            usize::MAX,
        );
        pb::ExportTraceServiceRequest::decode(encoded.as_bytes()).expect("fixture must decode")
    }

    fn check(request: pb::ExportTraceServiceRequest, options: &Options) -> Result<(usize, usize)> {
        verify(&[Bytes::from(request.encode_to_vec())], options, 0)
    }

    #[test]
    fn verifies_every_fixture() {
        for workload in [Workload::Root, Workload::Children, Workload::Attributes] {
            let options = options(workload);
            let (_, events) =
                check(fixture(&options), &options).expect("native fixture must verify");
            assert_eq!(
                events,
                if workload == Workload::Attributes {
                    2
                } else {
                    0
                }
            );
        }
    }

    #[test]
    fn rejects_missing_or_duplicate_spans() {
        let options = options(Workload::Children);
        let mut missing = fixture(&options);
        missing.resource_spans[0].scope_spans[0].spans.pop();
        assert!(check(missing, &options).is_err());
        let mut duplicate = fixture(&options);
        let spans = &mut duplicate.resource_spans[0].scope_spans[0].spans;
        spans[1].span_id = spans[0].span_id.clone();
        assert!(check(duplicate, &options).is_err());
    }

    #[test]
    fn rejects_corrupted_relationships() {
        let options = options(Workload::Children);
        for corrupt_time in [false, true] {
            let mut request = fixture(&options);
            let child = request.resource_spans[0].scope_spans[0]
                .spans
                .iter_mut()
                .find(|s| s.name == "child_a")
                .expect("child exists");
            if corrupt_time {
                child.start_time_unix_nano = 1;
            } else {
                child.parent_span_id = vec![42; 8];
            }
            assert!(check(request, &options).is_err());
        }
    }

    #[test]
    fn rejects_changed_attribute_types_and_event_payloads() {
        let options = options(Workload::Attributes);
        let mut request = fixture(&options);
        let root = request.resource_spans[0].scope_spans[0]
            .spans
            .iter_mut()
            .find(|s| s.name == "root")
            .expect("root exists");
        root.attributes[0].value = Some(pb::AnyValue {
            value: Some(pb::any_value::Value::StringValue("wrong type".into())),
        });
        assert!(check(request, &options).is_err());
        let mut request = fixture(&options);
        request.resource_spans[0].scope_spans[0]
            .spans
            .iter_mut()
            .find(|s| s.name == "child")
            .expect("child exists")
            .events[0]
            .name = "wrong event".into();
        assert!(check(request, &options).is_err());
    }

    #[test]
    fn rejects_unexpected_scope_and_resource_metadata() {
        let options = options(Workload::Root);
        let mut request = fixture(&options);
        request.resource_spans[0].scope_spans[0]
            .scope
            .as_mut()
            .expect("scope exists")
            .name = "wrong scope".into();
        assert!(check(request, &options).is_err());
        let mut request = fixture(&options);
        request.resource_spans[0]
            .resource
            .as_mut()
            .expect("resource exists")
            .attributes
            .clear();
        assert!(check(request, &options).is_err());
    }

    #[test]
    fn rejects_missing_or_malformed_requests() {
        let options = options(Workload::Root);
        assert!(verify(&[], &options, 0).is_err());
        assert!(verify(&[Bytes::from_static(&[0xff])], &options, 0).is_err());
    }

    #[test]
    fn validates_cli_limits_and_whole_traces() {
        let valid = ["native", "children", "1", "256", "2", "1", "capture"].map(str::to_owned);
        assert!(Options::from_args(&valid).is_ok());
        for (index, value) in [
            (0, "registry"),
            (2, "0"),
            (2, "129"),
            (3, "1025"),
            (3, "255"),
            (4, "0"),
            (6, "http://localhost:4318/v1/traces"),
            (6, "http://user:password@localhost:4318"),
            (6, "https://localhost:4318"),
        ] {
            let mut args = valid.clone();
            args[index] = value.into();
            assert!(Options::from_args(&args).is_err());
        }
    }

    #[test]
    fn capture_client_checks_the_http_envelope() {
        let wire = WireClient {
            bodies: Arc::new(Mutex::new(Vec::new())),
            http: None,
        };
        let request = Request::builder()
            .method("GET")
            .uri("http://localhost/v1/traces")
            .body(Bytes::new())
            .expect("test request");
        assert!(futures_executor::block_on(wire.send_bytes(request)).is_err());
        assert!(wire.bodies.lock().is_empty());
        let request = Request::builder()
            .method("POST")
            .uri("http://localhost/v1/traces")
            .header(http::header::CONTENT_TYPE, "application/x-protobuf")
            .body(Bytes::from_static(b"test body"))
            .expect("test request");
        let response = futures_executor::block_on(wire.send_bytes(request))
            .expect("capture accepts valid envelope");
        assert!(response.status().is_success());
        assert_eq!(wire.bodies.lock().len(), 1);
    }
}

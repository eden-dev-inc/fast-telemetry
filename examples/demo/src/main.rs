//! Demo application showing fast-telemetry metrics collection and export.
//!
//! Run with:
//!   cargo run -p fast-telemetry-demo
//!
//! To see log output:
//!   RUST_LOG=info cargo run -p fast-telemetry-demo

use std::sync::Arc;
use std::time::Duration;

use fast_telemetry::{
    Counter, DeriveLabel, Distribution, DynamicCounter, ExportMetrics, Gauge, Histogram,
    LabeledCounter, LabeledHistogram, MaxGauge, MinGauge, SpanCollector, SpanKind, SpanStatus,
};
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// 1. Define label enums (compile-time, O(1) lookup)
// ---------------------------------------------------------------------------

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "method"]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "status"]
pub enum StatusClass {
    Success2xx,
    ClientError4xx,
    ServerError5xx,
}

// ---------------------------------------------------------------------------
// 2. Define metrics structs with derive macros
// ---------------------------------------------------------------------------

#[derive(ExportMetrics)]
#[metric_prefix = "demo"]
#[otlp]
pub struct AppMetrics {
    #[help = "Total HTTP requests"]
    pub requests: Counter,

    #[help = "HTTP requests by method"]
    pub requests_by_method: LabeledCounter<HttpMethod>,

    #[help = "HTTP requests by status class"]
    pub requests_by_status: LabeledCounter<StatusClass>,

    #[help = "Request latency in microseconds"]
    pub latency: Histogram,

    #[help = "Request latency by method"]
    pub latency_by_method: LabeledHistogram<HttpMethod>,

    #[help = "Response size distribution"]
    pub response_bytes: Distribution,

    #[help = "Current in-flight requests"]
    pub in_flight: Gauge,

    #[help = "Peak in-flight requests seen during the current run"]
    pub in_flight_peak: MaxGauge,

    #[help = "Smallest response size seen during the current run"]
    pub min_response_bytes: MinGauge,

    #[help = "Requests by endpoint (runtime labels)"]
    pub requests_by_endpoint: DynamicCounter,
}

impl Default for AppMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl AppMetrics {
    pub fn new() -> Self {
        let shards = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        Self {
            requests: Counter::new(shards),
            requests_by_method: LabeledCounter::new(shards),
            requests_by_status: LabeledCounter::new(shards),
            latency: Histogram::with_latency_buckets(shards),
            latency_by_method: LabeledHistogram::with_latency_buckets(shards),
            response_bytes: Distribution::new(shards),
            in_flight: Gauge::new(),
            in_flight_peak: MaxGauge::new(shards),
            min_response_bytes: MinGauge::with_value(shards, i64::MAX),
            requests_by_endpoint: DynamicCounter::new(shards),
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Simulate some work
// ---------------------------------------------------------------------------

fn simulate_requests(metrics: &AppMetrics) {
    // Simulate a GET request
    metrics.requests.inc();
    metrics.requests_by_method.inc(HttpMethod::Get);
    metrics.requests_by_status.inc(StatusClass::Success2xx);
    metrics.latency.record(150); // 150us
    metrics.latency_by_method.record(HttpMethod::Get, 150);
    metrics.response_bytes.record(4096);
    metrics.in_flight.set(3);
    metrics.in_flight_peak.observe(3);
    metrics.min_response_bytes.observe(4096);

    // Dynamic labels -- runtime endpoint tracking
    metrics
        .requests_by_endpoint
        .inc(&[("endpoint", "/api/users"), ("org", "acme")]);
    metrics
        .requests_by_endpoint
        .inc(&[("endpoint", "/api/orders"), ("org", "acme")]);

    // Hot-path optimization: resolve the series handle once
    let users_series = metrics
        .requests_by_endpoint
        .series(&[("endpoint", "/api/users"), ("org", "acme")]);
    for _ in 0..100 {
        users_series.inc();
    }

    // Simulate a POST that's slower
    metrics.requests.inc();
    metrics.requests_by_method.inc(HttpMethod::Post);
    metrics.requests_by_status.inc(StatusClass::Success2xx);
    metrics.latency.record(2500); // 2.5ms
    metrics.latency_by_method.record(HttpMethod::Post, 2500);
    metrics.response_bytes.record(128);
    metrics.in_flight_peak.observe(8);
    metrics.min_response_bytes.observe(128);

    // Simulate a failed request
    metrics.requests.inc();
    metrics.requests_by_method.inc(HttpMethod::Get);
    metrics.requests_by_status.inc(StatusClass::ServerError5xx);
    metrics.latency.record(50_000); // 50ms timeout
    metrics.in_flight_peak.observe(11);
}

fn simulate_spans(collector: Arc<SpanCollector>) {
    let mut root = collector.start_span("handle_request", SpanKind::Server);
    root.enter();
    root.set_attribute("http.method", "GET");
    root.set_attribute("http.url", "/api/users");

    {
        let mut auth = root.child("authenticate", SpanKind::Internal);
        auth.set_attribute("auth.method", "jwt");
        auth.set_status(SpanStatus::Ok);
    }

    {
        let mut db = root.child("db_query", SpanKind::Client);
        db.set_attribute("db.system", "postgres");
        db.set_attribute("db.statement", "SELECT * FROM users WHERE org_id = $1");
        db.set_status(SpanStatus::Ok);
    }

    root.set_status(SpanStatus::Ok);
    // spans submit to collector on drop
}

// ---------------------------------------------------------------------------
// 4. Main -- wire up metrics, recording, and export
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    env_logger::init();

    let metrics = Arc::new(AppMetrics::new());
    let collector = Arc::new(SpanCollector::new(4, 4096));
    let cancel = CancellationToken::new();

    // Simulate some application work
    simulate_requests(&metrics);
    simulate_spans(collector.clone());

    // --- Prometheus export (inline, for /metrics endpoint) ---
    let mut prom_output = String::new();
    metrics.export_prometheus(&mut prom_output);
    println!("=== Prometheus Output ===\n{prom_output}");

    // --- DogStatsD export (cumulative snapshot) ---
    let mut statsd_output = String::new();
    metrics.export_dogstatsd(
        &mut statsd_output,
        &[("env", "demo"), ("service", "fast-telemetry-demo")],
    );
    println!("=== DogStatsD Output ===\n{statsd_output}");

    // --- Drain spans ---
    // flush_local moves thread-local span buffers to the shared outbox
    // (normally this happens automatically when the buffer hits the threshold)
    collector.flush_local();
    let mut completed_spans = Vec::new();
    collector.drain_into(&mut completed_spans);
    println!("=== Spans ===");
    for span in &completed_spans {
        println!(
            "  {} (kind={:?}, status={:?}, duration={:?})",
            span.name,
            span.kind,
            span.status,
            span.end_time_ns.saturating_sub(span.start_time_ns)
        );
    }

    // --- Background exporters (would run for the lifetime of the app) ---
    // Uncomment and configure to see background export in action:

    // DogStatsD exporter
    // let m = metrics.clone();
    // let mut state = AppMetricsExportState::new();
    // let tags: Vec<(&str, &str)> = vec![("service", "demo")];
    // let c = cancel.clone();
    // tokio::spawn(fast_telemetry_export::dogstatsd::run(
    //     fast_telemetry_export::dogstatsd::DogStatsDConfig::new("127.0.0.1:8125")
    //         .with_interval(Duration::from_secs(10)),
    //     c,
    //     move |output| {
    //         m.export_dogstatsd_delta(output, &tags, &mut state);
    //     },
    // ));

    // OTLP metrics exporter
    // let m = metrics.clone();
    // let c = cancel.clone();
    // tokio::spawn(fast_telemetry_export::otlp::run(
    //     fast_telemetry_export::otlp::OtlpConfig::new("http://localhost:4318")
    //         .with_service_name("fast-telemetry-demo")
    //         .with_interval(Duration::from_secs(60)),
    //     c,
    //     move |out| {
    //         m.export_otlp(out);
    //     },
    // ));

    // OTLP span exporter
    // fast_telemetry_export::spans::spawn(
    //     collector.clone(),
    //     fast_telemetry_export::spans::SpanExportConfig::new("http://localhost:4318")
    //         .with_service_name("fast-telemetry-demo"),
    //     cancel.clone(),
    // );

    // Stale series sweeper
    // let m = metrics.clone();
    // let c = cancel.clone();
    // tokio::spawn(fast_telemetry_export::sweeper::run(
    //     fast_telemetry_export::sweeper::SweepConfig::default(),
    //     c,
    //     move |threshold| m.evict_stale_series(threshold),
    // ));

    // In a real app, you'd wait for shutdown signal:
    // tokio::signal::ctrl_c().await.ok();
    // cancel.cancel();

    let _ = (cancel, Duration::from_secs(0)); // suppress unused warnings
}

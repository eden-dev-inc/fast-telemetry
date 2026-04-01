//! Labeled metrics example using enum-indexed counters and histograms.
//!
//! Run with: cargo run --example labeled

use ophanim::{
    DeriveLabel, ExportMetrics, LabelEnum, LabeledCounter, LabeledGauge, LabeledHistogram,
};

// Define label enums with the derive macro.
// Variant names are auto-converted to snake_case.

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "method"]
enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "status"]
enum StatusClass {
    Success2xx,
    Redirect3xx,
    ClientError4xx,
    ServerError5xx,
}

#[derive(ExportMetrics)]
#[metric_prefix = "http"]
struct HttpMetrics {
    #[help = "Total requests by method"]
    requests: LabeledCounter<HttpMethod>,

    #[help = "Response status distribution"]
    responses: LabeledCounter<StatusClass>,

    #[help = "Request latency by method (microseconds)"]
    latency: LabeledHistogram<HttpMethod>,

    #[help = "Active connections by method"]
    active: LabeledGauge<HttpMethod>,
}

impl HttpMetrics {
    fn new() -> Self {
        Self {
            requests: LabeledCounter::new(4),
            responses: LabeledCounter::new(4),
            latency: LabeledHistogram::with_latency_buckets(4),
            active: LabeledGauge::new(),
        }
    }
}

fn main() {
    let metrics = HttpMetrics::new();

    // Simulate traffic
    for _ in 0..100 {
        metrics.requests.inc(HttpMethod::Get);
        metrics.latency.record(HttpMethod::Get, 150);
        metrics.responses.inc(StatusClass::Success2xx);
    }

    for _ in 0..30 {
        metrics.requests.inc(HttpMethod::Post);
        metrics.latency.record(HttpMethod::Post, 500);
        metrics.responses.inc(StatusClass::Success2xx);
    }

    metrics.requests.add(HttpMethod::Delete, 5);
    metrics.responses.add(StatusClass::ClientError4xx, 3);

    metrics.active.set(HttpMethod::Get, 10);
    metrics.active.set(HttpMethod::Post, 2);

    // Export
    println!("=== Prometheus Format ===\n");
    let mut prom = String::new();
    metrics.export_prometheus(&mut prom);
    println!("{}", prom);

    println!("=== DogStatsD Format ===\n");
    let mut statsd = String::new();
    metrics.export_dogstatsd(&mut statsd, &[("service", "api")]);
    println!("{}", statsd);
}

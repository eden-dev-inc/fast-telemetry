//! Basic usage of fast-telemetry counters, gauges, and histograms.
//!
//! Run with: cargo run --example basic

use std::time::Duration;

use fast_telemetry::{Counter, ExportMetrics, Gauge, Histogram, SampledTimer};

#[derive(ExportMetrics)]
#[metric_prefix = "myapp"]
struct AppMetrics {
    #[help = "Total requests processed"]
    requests: Counter,

    #[help = "Request latency in microseconds"]
    latency: Histogram,

    #[help = "Hot path latency"]
    hot_path_latency: SampledTimer,

    #[help = "Current queue depth"]
    queue_depth: Gauge,
}

impl AppMetrics {
    fn new() -> Self {
        Self {
            // Use number of CPUs for shard count in production
            requests: Counter::new(4),
            latency: Histogram::with_latency_buckets(4),
            hot_path_latency: SampledTimer::with_latency_buckets(4, 64),
            queue_depth: Gauge::new(),
        }
    }
}

fn main() {
    let metrics = AppMetrics::new();

    // Simulate some work
    for i in 0..100 {
        metrics.requests.inc();
        metrics.latency.record(50 + (i % 200)); // 50-250µs
        metrics
            .hot_path_latency
            .record_elapsed(Duration::from_nanos(80_000 + (i % 200) * 1_000));
    }
    metrics.queue_depth.set(42);

    // Export to Prometheus format
    println!("=== Prometheus Format ===\n");
    let mut prom = String::new();
    metrics.export_prometheus(&mut prom);
    println!("{}", prom);

    // Export to DogStatsD format
    println!("=== DogStatsD Format ===\n");
    let mut statsd = String::new();
    metrics.export_dogstatsd(&mut statsd, &[("env", "dev")]);
    println!("{}", statsd);
}

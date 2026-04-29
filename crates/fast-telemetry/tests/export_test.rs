//! Integration test for ExportMetrics derive macro.

use fast_telemetry::{Counter, ExportMetrics, Gauge, Histogram};

#[derive(ExportMetrics)]
#[metric_prefix = "test"]
struct TestMetrics {
    #[help = "Total requests"]
    requests: Counter,

    #[help = "Current connections"]
    connections: Gauge,

    #[help = "Request latency in microseconds"]
    latency: Histogram,
}

impl TestMetrics {
    fn new() -> Self {
        Self {
            requests: Counter::new(4),
            connections: Gauge::new(),
            latency: Histogram::new(&[100, 500, 1000], 4),
        }
    }
}

#[test]
fn test_export_counter() {
    let metrics = TestMetrics::new();
    metrics.requests.inc();
    metrics.requests.inc();

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP test_requests Total requests"));
    assert!(output.contains("# TYPE test_requests counter"));
    assert!(output.contains("test_requests 2"));
}

#[test]
fn test_export_gauge() {
    let metrics = TestMetrics::new();
    metrics.connections.set(42);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP test_connections Current connections"));
    assert!(output.contains("# TYPE test_connections gauge"));
    assert!(output.contains("test_connections 42"));
}

#[test]
fn test_export_histogram() {
    let metrics = TestMetrics::new();
    metrics.latency.record(50);
    metrics.latency.record(200);
    metrics.latency.record(2000);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP test_latency Request latency in microseconds"));
    assert!(output.contains("# TYPE test_latency histogram"));
    assert!(output.contains("test_latency_bucket{le=\"100\"} 1"));
    assert!(output.contains("test_latency_bucket{le=\"500\"} 2"));
    assert!(output.contains("test_latency_bucket{le=\"1000\"} 2"));
    assert!(output.contains("test_latency_bucket{le=\"+Inf\"} 3"));
    assert!(output.contains("test_latency_count 3"));
}

#[cfg(feature = "clickhouse")]
#[derive(ExportMetrics)]
#[clickhouse]
#[metric_prefix = "test"]
struct ClickHouseMetrics {
    #[help = "Total requests"]
    requests: Counter,

    #[help = "Current connections"]
    connections: Gauge,
}

#[cfg(feature = "clickhouse")]
#[test]
fn test_export_clickhouse() {
    let metrics = ClickHouseMetrics {
        requests: Counter::new(4),
        connections: Gauge::new(),
    };
    metrics.requests.add(7);
    metrics.connections.set(3);

    let mut batch = fast_telemetry::clickhouse::ClickHouseMetricBatch::new("test");
    metrics.export_clickhouse(&mut batch, 123);

    assert_eq!(batch.sums.len(), 1);
    assert_eq!(batch.gauges.len(), 1);
    assert_eq!(batch.sums[0].MetricName, "test_requests");
    assert_eq!(batch.sums[0].Value, 7.0);
    assert_eq!(batch.gauges[0].MetricName, "test_connections");
    assert_eq!(batch.gauges[0].Value, 3.0);
}

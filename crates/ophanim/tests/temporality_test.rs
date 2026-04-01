//! Integration test for ExportMetrics derive macro with temporality.

use ophanim::{Counter, ExportMetrics, Gauge, Histogram, Temporality};

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
fn test_export_cumulative() {
    let metrics = TestMetrics::new();
    metrics.requests.inc();
    metrics.requests.inc();
    metrics.connections.set(42);
    metrics.latency.record(50);

    // Test Prometheus (always cumulative)
    let mut output = String::new();
    metrics.export_prometheus(&mut output);
    assert!(output.contains("test_requests 2"));
    assert!(output.contains("test_connections 42"));
    assert!(output.contains("test_latency_count 1"));

    // Test DogStatsD Cumulative (default export_dogstatsd)
    let mut output = String::new();
    metrics.export_dogstatsd(&mut output, &[]);
    assert!(output.contains("test.requests:2|c\n"));
    assert!(output.contains("test.connections:42|g\n"));
    assert!(output.contains("test.latency.count:1|c\n"));
}

#[test]
fn test_export_delta() {
    let metrics = TestMetrics::new();
    let mut state = TestMetricsDogStatsDState::new();

    // First interval: 2 requests
    metrics.requests.inc();
    metrics.requests.inc();
    metrics.connections.set(10);
    metrics.latency.record(50); // count=1, sum=50

    let mut output = String::new();
    metrics.export_dogstatsd_delta(&mut output, &[], &mut state);

    // First export sees full values as delta from 0
    assert!(output.contains("test.requests:2|c\n"));
    assert!(output.contains("test.connections:10|g\n"));
    assert!(output.contains("test.latency.count:1|c\n"));
    assert!(output.contains("test.latency.sum:50|c\n"));

    // Second interval: 3 more requests
    metrics.requests.inc();
    metrics.requests.inc();
    metrics.requests.inc(); // Total 5
    metrics.connections.set(20);
    metrics.latency.record(100); // Total count=2, sum=150

    let mut output = String::new();
    metrics.export_dogstatsd_delta(&mut output, &[], &mut state);

    // Second export should show ONLY the deltas
    assert!(output.contains("test.requests:3|c\n")); // 5 - 2 = 3
    assert!(output.contains("test.connections:20|g\n")); // Gauge always shows current
    assert!(output.contains("test.latency.count:1|c\n")); // 2 - 1 = 1
    assert!(output.contains("test.latency.sum:100|c\n")); // 150 - 50 = 100
}

#[test]
fn test_export_with_temporality() {
    let metrics = TestMetrics::new();
    let mut state = TestMetricsDogStatsDState::new();

    metrics.requests.inc();

    // 1. Export with Delta temporality
    let mut output = String::new();
    metrics.export_dogstatsd_with_temporality(&mut output, &[], Temporality::Delta, &mut state);
    assert!(output.contains("test.requests:1|c\n"));

    metrics.requests.inc();

    // 2. Export with Cumulative temporality (ignores state, shows total)
    let mut output = String::new();
    metrics.export_dogstatsd_with_temporality(
        &mut output,
        &[],
        Temporality::Cumulative,
        &mut state,
    );
    assert!(output.contains("test.requests:2|c\n"));
}

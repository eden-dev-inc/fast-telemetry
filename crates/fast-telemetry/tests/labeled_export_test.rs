//! Integration tests for ExportMetrics with labeled types.

use fast_telemetry::{
    Counter, ExportMetrics, LabelEnum, LabeledCounter, LabeledGauge, LabeledHistogram,
};

// Define a test label enum
#[derive(Copy, Clone, Debug, PartialEq)]
enum HttpMethod {
    Get,
    Post,
    Put,
}

impl LabelEnum for HttpMethod {
    const CARDINALITY: usize = 3;
    const LABEL_NAME: &'static str = "method";

    fn as_index(self) -> usize {
        self as usize
    }

    fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Get,
            1 => Self::Post,
            _ => Self::Put,
        }
    }

    fn variant_name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
            Self::Put => "put",
        }
    }
}

#[derive(ExportMetrics)]
#[metric_prefix = "api"]
struct ApiMetrics {
    /// Total requests (unlabeled)
    #[help = "Total API requests"]
    requests_total: Counter,

    /// Requests by method (labeled)
    #[help = "API requests by HTTP method"]
    requests_by_method: LabeledCounter<HttpMethod>,

    /// Queue depth by method
    #[help = "Queue depth per method"]
    queue_depth: LabeledGauge<HttpMethod>,

    /// Latency by method
    #[help = "Request latency by method"]
    latency_by_method: LabeledHistogram<HttpMethod>,
}

impl ApiMetrics {
    fn new() -> Self {
        Self {
            requests_total: Counter::new(4),
            requests_by_method: LabeledCounter::new(4),
            queue_depth: LabeledGauge::new(),
            latency_by_method: LabeledHistogram::new(&[100, 1000], 4),
        }
    }
}

#[test]
fn test_labeled_counter_export() {
    let metrics = ApiMetrics::new();

    metrics.requests_by_method.inc(HttpMethod::Get);
    metrics.requests_by_method.inc(HttpMethod::Get);
    metrics.requests_by_method.add(HttpMethod::Post, 5);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP api_requests_by_method API requests by HTTP method"));
    assert!(output.contains("# TYPE api_requests_by_method counter"));
    assert!(output.contains("api_requests_by_method{method=\"get\"} 2"));
    assert!(output.contains("api_requests_by_method{method=\"post\"} 5"));
    assert!(output.contains("api_requests_by_method{method=\"put\"} 0"));
}

#[test]
fn test_labeled_gauge_export() {
    let metrics = ApiMetrics::new();

    metrics.queue_depth.set(HttpMethod::Get, 10);
    metrics.queue_depth.set(HttpMethod::Post, 25);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP api_queue_depth Queue depth per method"));
    assert!(output.contains("# TYPE api_queue_depth gauge"));
    assert!(output.contains("api_queue_depth{method=\"get\"} 10"));
    assert!(output.contains("api_queue_depth{method=\"post\"} 25"));
    assert!(output.contains("api_queue_depth{method=\"put\"} 0"));
}

#[test]
fn test_labeled_histogram_export() {
    let metrics = ApiMetrics::new();

    // Record some values: 50µs (bucket 0), 500µs (bucket 1), 2000µs (+Inf)
    metrics.latency_by_method.record(HttpMethod::Get, 50);
    metrics.latency_by_method.record(HttpMethod::Get, 500);
    metrics.latency_by_method.record(HttpMethod::Get, 2000);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    assert!(output.contains("# HELP api_latency_by_method Request latency by method"));
    assert!(output.contains("# TYPE api_latency_by_method histogram"));

    // GET buckets (cumulative)
    assert!(output.contains("api_latency_by_method_bucket{method=\"get\",le=\"100\"} 1"));
    assert!(output.contains("api_latency_by_method_bucket{method=\"get\",le=\"1000\"} 2"));
    assert!(output.contains("api_latency_by_method_bucket{method=\"get\",le=\"+Inf\"} 3"));
    assert!(output.contains("api_latency_by_method_count{method=\"get\"} 3"));

    // POST should have zero counts
    assert!(output.contains("api_latency_by_method_count{method=\"post\"} 0"));
}

#[test]
fn test_mixed_labeled_and_unlabeled() {
    let metrics = ApiMetrics::new();

    // Unlabeled counter
    metrics.requests_total.inc();
    metrics.requests_total.inc();

    // Labeled counter
    metrics.requests_by_method.inc(HttpMethod::Get);

    let mut output = String::new();
    metrics.export_prometheus(&mut output);

    // Should have both unlabeled and labeled output
    assert!(output.contains("api_requests_total 2"));
    assert!(output.contains("api_requests_by_method{method=\"get\"} 1"));
}

// ============================================================================
// DogStatsD Export Tests
// ============================================================================

#[test]
fn test_dogstatsd_export() {
    let metrics = ApiMetrics::new();

    metrics.requests_total.inc();
    metrics.requests_total.inc();
    metrics.requests_by_method.inc(HttpMethod::Get);
    metrics.requests_by_method.add(HttpMethod::Post, 5);
    metrics.queue_depth.set(HttpMethod::Get, 10);

    let mut output = String::new();
    metrics.export_dogstatsd(&mut output, &[]);

    // Unlabeled counter (note: dots instead of underscores)
    assert!(output.contains("api.requests_total:2|c\n"));

    // Labeled counter
    assert!(output.contains("api.requests_by_method:1|c|#method:get\n"));
    assert!(output.contains("api.requests_by_method:5|c|#method:post\n"));

    // Labeled gauge
    assert!(output.contains("api.queue_depth:10|g|#method:get\n"));
}

#[test]
fn test_dogstatsd_export_with_tags() {
    let metrics = ApiMetrics::new();

    metrics.requests_total.add(100);

    let mut output = String::new();
    metrics.export_dogstatsd(&mut output, &[("env", "prod"), ("region", "us-east")]);

    assert!(output.contains("api.requests_total:100|c|#env:prod,region:us-east\n"));
}

#[test]
fn test_dogstatsd_labeled_histogram() {
    let metrics = ApiMetrics::new();

    metrics.latency_by_method.record(HttpMethod::Get, 100);
    metrics.latency_by_method.record(HttpMethod::Get, 200);

    let mut output = String::new();
    metrics.export_dogstatsd(&mut output, &[]);

    // Histogram exports count and sum
    assert!(output.contains("api.latency_by_method.count:2|c|#method:get\n"));
    assert!(output.contains("api.latency_by_method.sum:300|c|#method:get\n"));
}

use std::time::Duration;

use fast_telemetry::{
    Counter, DeriveLabel, Distribution, DistributionSnapshot, DynamicCounter, DynamicDistribution,
    DynamicHistogram, ExportMetrics, Gauge, Histogram, HistogramSnapshot, LabeledCounter,
    LabeledSampledTimer, MetricLabels, MetricMeta, MetricVisitor,
};

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "method"]
enum Method {
    Get,
    Post,
}

#[derive(ExportMetrics)]
#[metric_prefix = "visitor"]
struct VisitorMetrics {
    #[help = "Total requests"]
    requests: Counter,

    #[help = "Active requests"]
    active: Gauge,

    #[help = "Requests by method"]
    requests_by_method: LabeledCounter<Method>,

    #[help = "Request latency"]
    latency: Histogram,

    #[help = "Request size"]
    request_size: Distribution,

    #[help = "Method latency"]
    method_latency: LabeledSampledTimer<Method>,

    #[help = "Dynamic requests"]
    dynamic_requests: DynamicCounter,

    #[help = "Dynamic latency"]
    dynamic_latency: DynamicHistogram,

    #[help = "Dynamic size"]
    dynamic_size: DynamicDistribution,
}

impl VisitorMetrics {
    fn new() -> Self {
        Self {
            requests: Counter::new(4),
            active: Gauge::new(),
            requests_by_method: LabeledCounter::new(4),
            latency: Histogram::new(&[100, 500], 4),
            request_size: Distribution::new(4),
            method_latency: LabeledSampledTimer::with_latency_buckets(4, 1),
            dynamic_requests: DynamicCounter::new(4),
            dynamic_latency: DynamicHistogram::new(&[10, 100], 4),
            dynamic_size: DynamicDistribution::new(4),
        }
    }
}

#[derive(Debug, PartialEq)]
enum Point {
    Counter {
        name: String,
        help: String,
        labels: Vec<(String, String)>,
        value: i64,
    },
    GaugeI64 {
        name: String,
        labels: Vec<(String, String)>,
        value: i64,
    },
    Histogram {
        name: String,
        labels: Vec<(String, String)>,
        count: u64,
        sum: u64,
        buckets: Vec<(u64, u64)>,
    },
    Distribution {
        name: String,
        labels: Vec<(String, String)>,
        count: u64,
        sum: u64,
        min: Option<u64>,
        max: Option<u64>,
        zero_count: u64,
        positive_buckets: Vec<(i32, u64)>,
    },
}

#[derive(Default)]
struct CollectVisitor {
    points: Vec<Point>,
}

impl CollectVisitor {
    fn labels(labels: MetricLabels<'_>) -> Vec<(String, String)> {
        labels
            .iter()
            .map(|label| (label.name.to_string(), label.value.to_string()))
            .collect()
    }
}

impl MetricVisitor for CollectVisitor {
    fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        self.points.push(Point::Counter {
            name: meta.name.to_string(),
            help: meta.help.to_string(),
            labels: Self::labels(labels),
            value,
        });
    }

    fn gauge_i64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        self.points.push(Point::GaugeI64 {
            name: meta.name.to_string(),
            labels: Self::labels(labels),
            value,
        });
    }

    fn gauge_f64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: f64) {}

    fn histogram(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        histogram: &dyn HistogramSnapshot,
    ) {
        let mut buckets = Vec::new();
        histogram.visit_buckets(&mut |upper_bound, cumulative_count| {
            buckets.push((upper_bound, cumulative_count));
        });

        self.points.push(Point::Histogram {
            name: meta.name.to_string(),
            labels: Self::labels(labels),
            count: histogram.count(),
            sum: histogram.sum(),
            buckets,
        });
    }

    fn distribution(
        &mut self,
        meta: MetricMeta<'_>,
        labels: MetricLabels<'_>,
        distribution: &dyn DistributionSnapshot,
    ) {
        let mut positive_buckets = Vec::new();
        distribution.visit_positive_buckets(&mut |bucket_index, count| {
            positive_buckets.push((bucket_index, count));
        });

        self.points.push(Point::Distribution {
            name: meta.name.to_string(),
            labels: Self::labels(labels),
            count: distribution.count(),
            sum: distribution.sum(),
            min: distribution.min(),
            max: distribution.max(),
            zero_count: distribution.zero_count(),
            positive_buckets,
        });
    }
}

#[test]
fn visit_metrics_emits_structured_observations() {
    let metrics = VisitorMetrics::new();
    metrics.requests.add(2);
    metrics.active.set(7);
    metrics.requests_by_method.inc(Method::Get);
    metrics.latency.record(50);
    metrics.latency.record(200);
    metrics.request_size.record(0);
    metrics.request_size.record(7);
    metrics
        .method_latency
        .record_elapsed(Method::Post, Duration::from_nanos(10_000));
    metrics
        .dynamic_requests
        .add(&[("org_id", "org-a"), ("endpoint_uuid", "ep-1")], 3);
    metrics
        .dynamic_latency
        .record(&[("org_id", "org-b"), ("endpoint_uuid", "ep-2")], 5);
    metrics
        .dynamic_latency
        .record(&[("endpoint_uuid", "ep-2"), ("org_id", "org-b")], 50);
    metrics
        .dynamic_size
        .record(&[("org_id", "org-c"), ("endpoint_uuid", "ep-3")], 0);
    metrics
        .dynamic_size
        .record(&[("endpoint_uuid", "ep-3"), ("org_id", "org-c")], 9);

    let mut visitor = CollectVisitor::default();
    metrics.visit_metrics(&mut visitor);

    assert!(visitor.points.contains(&Point::Counter {
        name: "visitor_requests".to_string(),
        help: "Total requests".to_string(),
        labels: Vec::new(),
        value: 2,
    }));
    assert!(visitor.points.contains(&Point::GaugeI64 {
        name: "visitor_active".to_string(),
        labels: Vec::new(),
        value: 7,
    }));
    assert!(visitor.points.contains(&Point::Counter {
        name: "visitor_requests_by_method".to_string(),
        help: "Requests by method".to_string(),
        labels: vec![("method".to_string(), "get".to_string())],
        value: 1,
    }));
    assert!(visitor.points.contains(&Point::Counter {
        name: "visitor_dynamic_requests".to_string(),
        help: "Dynamic requests".to_string(),
        labels: vec![
            ("endpoint_uuid".to_string(), "ep-1".to_string()),
            ("org_id".to_string(), "org-a".to_string()),
        ],
        value: 3,
    }));
    assert!(visitor.points.contains(&Point::Histogram {
        name: "visitor_latency".to_string(),
        labels: Vec::new(),
        count: 2,
        sum: 250,
        buckets: vec![(100, 1), (500, 2), (u64::MAX, 2)],
    }));
    assert!(visitor.points.contains(&Point::Distribution {
        name: "visitor_request_size".to_string(),
        labels: Vec::new(),
        count: 2,
        sum: 7,
        min: Some(0),
        max: Some(7),
        zero_count: 1,
        positive_buckets: vec![(2, 1)],
    }));
    assert!(visitor.points.contains(&Point::Counter {
        name: "visitor_method_latency_calls".to_string(),
        help: "Method latency total calls".to_string(),
        labels: vec![("method".to_string(), "post".to_string())],
        value: 1,
    }));
    assert!(visitor.points.contains(&Point::Histogram {
        name: "visitor_method_latency_samples".to_string(),
        labels: vec![("method".to_string(), "post".to_string())],
        count: 1,
        sum: 10_000,
        buckets: vec![
            (10_000, 1),
            (50_000, 1),
            (100_000, 1),
            (500_000, 1),
            (1_000_000, 1),
            (5_000_000, 1),
            (10_000_000, 1),
            (50_000_000, 1),
            (100_000_000, 1),
            (500_000_000, 1),
            (1_000_000_000, 1),
            (5_000_000_000, 1),
            (10_000_000_000, 1),
            (u64::MAX, 1),
        ],
    }));
    assert!(visitor.points.contains(&Point::Histogram {
        name: "visitor_dynamic_latency".to_string(),
        labels: vec![
            ("endpoint_uuid".to_string(), "ep-2".to_string()),
            ("org_id".to_string(), "org-b".to_string()),
        ],
        count: 2,
        sum: 55,
        buckets: vec![(10, 1), (100, 2), (u64::MAX, 2)],
    }));
    assert!(visitor.points.contains(&Point::Distribution {
        name: "visitor_dynamic_size".to_string(),
        labels: vec![
            ("endpoint_uuid".to_string(), "ep-3".to_string()),
            ("org_id".to_string(), "org-c".to_string()),
        ],
        count: 2,
        sum: 9,
        min: Some(0),
        max: Some(15),
        zero_count: 1,
        positive_buckets: vec![(3, 1)],
    }));
}

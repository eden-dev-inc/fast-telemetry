use std::time::Duration;

use fast_telemetry::{
    Counter, DeriveLabel, Distribution, DistributionSnapshot, DynamicCounter, DynamicDistribution,
    DynamicGauge, DynamicGaugeI64, DynamicHistogram, ExportMetrics, Gauge, GaugeF64, Histogram,
    HistogramSnapshot, LabeledCounter, LabeledGauge, LabeledHistogram, LabeledSampledTimer,
    MaxGauge, MaxGaugeF64, MetricKind, MetricLabels, MetricMeta, MetricVisitor, MinGauge,
    MinGaugeF64, SampledTimer,
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

#[derive(ExportMetrics)]
#[metric_prefix = "all"]
struct FullVisitorMetrics {
    #[help = "Float gauge"]
    float_gauge: GaugeF64,

    #[help = "Max queue depth"]
    max_queue_depth: MaxGauge,

    #[help = "Peak CPU"]
    peak_cpu: MaxGaugeF64,

    #[help = "Minimum free slots"]
    min_free_slots: MinGauge,

    #[help = "Best latency"]
    best_latency: MinGaugeF64,

    #[help = "Unlabeled timer"]
    timer: SampledTimer,

    #[help = "Active by method"]
    active_by_method: LabeledGauge<Method>,

    #[help = "Latency by method"]
    latency_by_method: LabeledHistogram<Method>,

    #[help = "Dynamic float gauge"]
    dynamic_float_gauge: DynamicGauge,

    #[help = "Dynamic integer gauge"]
    dynamic_integer_gauge: DynamicGaugeI64,

    #[help = "Overflow requests"]
    overflow_requests: DynamicCounter,

    #[help = "Overflow latency"]
    overflow_latency: DynamicHistogram,

    #[help = "Overflow size"]
    overflow_size: DynamicDistribution,
}

impl FullVisitorMetrics {
    fn new() -> Self {
        Self {
            float_gauge: GaugeF64::new(),
            max_queue_depth: MaxGauge::new(4),
            peak_cpu: MaxGaugeF64::new(4),
            min_free_slots: MinGauge::new(4),
            best_latency: MinGaugeF64::new(4),
            timer: SampledTimer::new(&[25, 100], 4, 1),
            active_by_method: LabeledGauge::new(),
            latency_by_method: LabeledHistogram::new(&[10, 100], 4),
            dynamic_float_gauge: DynamicGauge::with_max_series(4, 1),
            dynamic_integer_gauge: DynamicGaugeI64::with_max_series(4, 1),
            overflow_requests: DynamicCounter::with_max_series(4, 1),
            overflow_latency: DynamicHistogram::with_limits(&[10, 100], 4, 1),
            overflow_size: DynamicDistribution::with_max_series(4, 1),
        }
    }
}

#[derive(Debug, PartialEq)]
struct SeenMeta {
    name: String,
    help: String,
    kind: MetricKind,
    unit: Option<String>,
}

impl SeenMeta {
    fn new(name: &str, help: &str, kind: MetricKind) -> Self {
        Self {
            name: name.to_string(),
            help: help.to_string(),
            kind,
            unit: None,
        }
    }

    fn from_meta(meta: MetricMeta<'_>) -> Self {
        Self {
            name: meta.name.to_string(),
            help: meta.help.to_string(),
            kind: meta.kind,
            unit: meta.unit.map(str::to_string),
        }
    }
}

#[derive(Debug, PartialEq)]
enum VisitEvent {
    Counter {
        meta: SeenMeta,
        labels: Vec<(String, String)>,
        value: i64,
    },
    GaugeI64 {
        meta: SeenMeta,
        labels: Vec<(String, String)>,
        value: i64,
    },
    GaugeF64 {
        meta: SeenMeta,
        labels: Vec<(String, String)>,
        value: f64,
    },
    Histogram {
        meta: SeenMeta,
        labels: Vec<(String, String)>,
        count: u64,
        sum: u64,
        buckets: Vec<(u64, u64)>,
    },
    Distribution {
        meta: SeenMeta,
        labels: Vec<(String, String)>,
        count: u64,
        sum: u64,
        min: Option<u64>,
        max: Option<u64>,
        zero_count: u64,
        positive_buckets: Vec<(i32, u64)>,
    },
    Overflow {
        meta: SeenMeta,
        overflow_count: u64,
    },
}

#[derive(Default)]
struct RecordingVisitor {
    events: Vec<VisitEvent>,
}

impl RecordingVisitor {
    fn labels(labels: MetricLabels<'_>) -> Vec<(String, String)> {
        labels
            .iter()
            .map(|label| (label.name.to_string(), label.value.to_string()))
            .collect()
    }
}

impl MetricVisitor for RecordingVisitor {
    fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        self.events.push(VisitEvent::Counter {
            meta: SeenMeta::from_meta(meta),
            labels: Self::labels(labels),
            value,
        });
    }

    fn gauge_i64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        self.events.push(VisitEvent::GaugeI64 {
            meta: SeenMeta::from_meta(meta),
            labels: Self::labels(labels),
            value,
        });
    }

    fn gauge_f64(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: f64) {
        self.events.push(VisitEvent::GaugeF64 {
            meta: SeenMeta::from_meta(meta),
            labels: Self::labels(labels),
            value,
        });
    }

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

        self.events.push(VisitEvent::Histogram {
            meta: SeenMeta::from_meta(meta),
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

        self.events.push(VisitEvent::Distribution {
            meta: SeenMeta::from_meta(meta),
            labels: Self::labels(labels),
            count: distribution.count(),
            sum: distribution.sum(),
            min: distribution.min(),
            max: distribution.max(),
            zero_count: distribution.zero_count(),
            positive_buckets,
        });
    }

    fn dynamic_overflow(&mut self, meta: MetricMeta<'_>, overflow_count: u64) {
        self.events.push(VisitEvent::Overflow {
            meta: SeenMeta::from_meta(meta),
            overflow_count,
        });
    }
}

fn assert_seen(events: &[VisitEvent], expected: VisitEvent) {
    assert!(
        events.contains(&expected),
        "missing event: {expected:?}\nseen events: {events:#?}"
    );
}

#[test]
fn visit_metrics_covers_all_generated_visitor_branches() {
    let metrics = FullVisitorMetrics::new();

    metrics.float_gauge.set(1.5);
    metrics.max_queue_depth.observe(12);
    metrics.max_queue_depth.observe(7);
    metrics.peak_cpu.observe(8.5);
    metrics.peak_cpu.observe(3.25);
    metrics.min_free_slots.observe(4);
    metrics.min_free_slots.observe(-2);
    metrics.best_latency.observe(5.5);
    metrics.best_latency.observe(1.25);
    metrics.timer.record_elapsed(Duration::from_nanos(25));
    metrics.active_by_method.set(Method::Get, 8);
    metrics.latency_by_method.record(Method::Get, 9);
    metrics.latency_by_method.record(Method::Post, 99);

    metrics
        .dynamic_float_gauge
        .set(&[("tenant", "primary")], 1.5);
    metrics
        .dynamic_float_gauge
        .set(&[("tenant", "overflowed")], 2.5);
    metrics
        .dynamic_integer_gauge
        .set(&[("tenant", "primary")], 11);
    metrics
        .dynamic_integer_gauge
        .set(&[("tenant", "overflowed")], 22);
    metrics.overflow_requests.add(&[("tenant", "primary")], 4);
    metrics
        .overflow_requests
        .add(&[("tenant", "overflowed")], 6);
    metrics.overflow_latency.record(&[("tenant", "primary")], 5);
    metrics
        .overflow_latency
        .record(&[("tenant", "overflowed")], 70);
    metrics.overflow_size.record(&[("tenant", "primary")], 2);
    metrics.overflow_size.record(&[("tenant", "overflowed")], 9);

    let mut visitor = RecordingVisitor::default();
    let visitor_obj: &mut dyn MetricVisitor = &mut visitor;
    metrics.visit_metrics(visitor_obj);

    assert_seen(
        &visitor.events,
        VisitEvent::GaugeF64 {
            meta: SeenMeta::new("all_float_gauge", "Float gauge", MetricKind::Gauge),
            labels: Vec::new(),
            value: 1.5,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeI64 {
            meta: SeenMeta::new("all_max_queue_depth", "Max queue depth", MetricKind::Gauge),
            labels: Vec::new(),
            value: 12,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeF64 {
            meta: SeenMeta::new("all_peak_cpu", "Peak CPU", MetricKind::Gauge),
            labels: Vec::new(),
            value: 8.5,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeI64 {
            meta: SeenMeta::new(
                "all_min_free_slots",
                "Minimum free slots",
                MetricKind::Gauge,
            ),
            labels: Vec::new(),
            value: -2,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeF64 {
            meta: SeenMeta::new("all_best_latency", "Best latency", MetricKind::Gauge),
            labels: Vec::new(),
            value: 1.25,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Counter {
            meta: SeenMeta::new(
                "all_timer_calls",
                "Unlabeled timer total calls",
                MetricKind::Counter,
            ),
            labels: Vec::new(),
            value: 1,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Histogram {
            meta: SeenMeta::new(
                "all_timer_samples",
                "Unlabeled timer sampled latency in nanoseconds",
                MetricKind::Histogram,
            ),
            labels: Vec::new(),
            count: 1,
            sum: 25,
            buckets: vec![(25, 1), (100, 1), (u64::MAX, 1)],
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeI64 {
            meta: SeenMeta::new(
                "all_active_by_method",
                "Active by method",
                MetricKind::Gauge,
            ),
            labels: vec![("method".to_string(), "get".to_string())],
            value: 8,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Histogram {
            meta: SeenMeta::new(
                "all_latency_by_method",
                "Latency by method",
                MetricKind::Histogram,
            ),
            labels: vec![("method".to_string(), "get".to_string())],
            count: 1,
            sum: 9,
            buckets: vec![(10, 1), (100, 1), (u64::MAX, 1)],
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeF64 {
            meta: SeenMeta::new(
                "all_dynamic_float_gauge",
                "Dynamic float gauge",
                MetricKind::Gauge,
            ),
            labels: vec![("tenant".to_string(), "primary".to_string())],
            value: 1.5,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeF64 {
            meta: SeenMeta::new(
                "all_dynamic_float_gauge",
                "Dynamic float gauge",
                MetricKind::Gauge,
            ),
            labels: vec![("__ft_overflow".to_string(), "true".to_string())],
            value: 2.5,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeI64 {
            meta: SeenMeta::new(
                "all_dynamic_integer_gauge",
                "Dynamic integer gauge",
                MetricKind::Gauge,
            ),
            labels: vec![("tenant".to_string(), "primary".to_string())],
            value: 11,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::GaugeI64 {
            meta: SeenMeta::new(
                "all_dynamic_integer_gauge",
                "Dynamic integer gauge",
                MetricKind::Gauge,
            ),
            labels: vec![("__ft_overflow".to_string(), "true".to_string())],
            value: 22,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Counter {
            meta: SeenMeta::new(
                "all_overflow_requests",
                "Overflow requests",
                MetricKind::Counter,
            ),
            labels: vec![("tenant".to_string(), "primary".to_string())],
            value: 4,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Counter {
            meta: SeenMeta::new(
                "all_overflow_requests",
                "Overflow requests",
                MetricKind::Counter,
            ),
            labels: vec![("__ft_overflow".to_string(), "true".to_string())],
            value: 6,
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Histogram {
            meta: SeenMeta::new(
                "all_overflow_latency",
                "Overflow latency",
                MetricKind::Histogram,
            ),
            labels: vec![("__ft_overflow".to_string(), "true".to_string())],
            count: 1,
            sum: 70,
            buckets: vec![(10, 0), (100, 1), (u64::MAX, 1)],
        },
    );
    assert_seen(
        &visitor.events,
        VisitEvent::Distribution {
            meta: SeenMeta::new(
                "all_overflow_size",
                "Overflow size",
                MetricKind::Distribution,
            ),
            labels: vec![("__ft_overflow".to_string(), "true".to_string())],
            count: 1,
            sum: 9,
            min: Some(8),
            max: Some(15),
            zero_count: 0,
            positive_buckets: vec![(3, 1)],
        },
    );

    for (name, help, kind) in [
        (
            "all_dynamic_float_gauge",
            "Dynamic float gauge",
            MetricKind::Gauge,
        ),
        (
            "all_dynamic_integer_gauge",
            "Dynamic integer gauge",
            MetricKind::Gauge,
        ),
        (
            "all_overflow_requests",
            "Overflow requests",
            MetricKind::Counter,
        ),
        (
            "all_overflow_latency",
            "Overflow latency",
            MetricKind::Histogram,
        ),
        (
            "all_overflow_size",
            "Overflow size",
            MetricKind::Distribution,
        ),
    ] {
        assert_seen(
            &visitor.events,
            VisitEvent::Overflow {
                meta: SeenMeta::new(name, help, kind),
                overflow_count: 1,
            },
        );
    }
}

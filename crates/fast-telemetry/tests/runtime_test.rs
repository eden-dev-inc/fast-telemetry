#![cfg(all(feature = "runtime", feature = "macros"))]

use fast_telemetry::{
    Counter, ExportMetrics, HistogramSnapshot, MetricKind, MetricLabels, MetricMeta, MetricScope,
    MetricVisitor, Runtime, RuntimeConfig,
};

#[derive(ExportMetrics)]
#[metric_prefix = "cache"]
struct CacheTelemetry {
    #[help = "Cache hits"]
    hits: Counter,
}

impl CacheTelemetry {
    fn new() -> Self {
        Self {
            hits: Counter::new(4),
        }
    }
}

#[derive(Default)]
struct CounterVisitor {
    counters: Vec<(String, i64)>,
}

impl MetricVisitor for CounterVisitor {
    fn counter(&mut self, meta: MetricMeta<'_>, labels: MetricLabels<'_>, value: i64) {
        assert_eq!(meta.kind, MetricKind::Counter);
        assert_eq!(labels.iter().len(), 0);
        self.counters.push((meta.name.to_string(), value));
    }

    fn gauge_i64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: i64) {}

    fn gauge_f64(&mut self, _meta: MetricMeta<'_>, _labels: MetricLabels<'_>, _value: f64) {}

    fn histogram(
        &mut self,
        _meta: MetricMeta<'_>,
        _labels: MetricLabels<'_>,
        _histogram: &dyn HistogramSnapshot,
    ) {
    }
}

fn assert_export_metrics<T: fast_telemetry::ExportMetrics>() {}

#[test]
fn runtime_registers_metrics_and_returns_direct_handles() {
    assert_export_metrics::<CacheTelemetry>();

    let runtime = Runtime::new(RuntimeConfig::default());
    let registered =
        runtime.register_metrics(MetricScope::new("shardmap.cache"), CacheTelemetry::new());

    registered.hits.inc();
    registered.hits.add(2);

    assert_eq!(registered.scope().name(), "shardmap.cache");
    assert_eq!(registered.hits.sum(), 3);
    assert_eq!(runtime.registered_metrics_len(), 1);
    assert_eq!(runtime.scopes()[0].name(), "shardmap.cache");

    let mut visitor = CounterVisitor::default();
    runtime.visit_metrics(&mut visitor);
    assert_eq!(visitor.counters, vec![("cache_hits".to_string(), 3)]);

    let mut scoped_visitor = CounterVisitor::default();
    runtime.visit_metrics_for_scope(&MetricScope::new("shardmap.cache"), &mut scoped_visitor);
    assert_eq!(scoped_visitor.counters, vec![("cache_hits".to_string(), 3)]);

    let mut missing_scope_visitor = CounterVisitor::default();
    runtime.visit_metrics_for_scope(&MetricScope::new("other"), &mut missing_scope_visitor);
    assert!(missing_scope_visitor.counters.is_empty());
}

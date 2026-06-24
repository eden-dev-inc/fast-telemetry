#![cfg(all(feature = "runtime", feature = "macros"))]

use fast_telemetry::{
    Counter, ExportMetrics, HistogramSnapshot, MetricKind, MetricLabels, MetricMeta, MetricScope,
    MetricVisitor, Runtime, RuntimeConfig, SpanKind,
};
use std::sync::Arc;

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

#[derive(ExportMetrics)]
#[metric_prefix = "queue"]
struct QueueTelemetry {
    #[help = "Queue pushes"]
    pushes: Counter,
}

impl QueueTelemetry {
    fn new() -> Self {
        Self {
            pushes: Counter::new(4),
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

#[test]
fn runtime_filters_multiple_metric_scopes() {
    let runtime = Runtime::new(RuntimeConfig::default());
    let cache = runtime.register_metrics(MetricScope::new("shardmap.cache"), CacheTelemetry::new());
    let queue = runtime.register_metrics(MetricScope::new("shardmap.queue"), QueueTelemetry::new());

    cache.hits.add(3);
    queue.pushes.add(5);

    assert_eq!(runtime.registered_metrics_len(), 2);
    assert_eq!(
        runtime
            .scopes()
            .iter()
            .map(|scope| scope.name())
            .collect::<Vec<_>>(),
        vec!["shardmap.cache", "shardmap.queue"]
    );

    let mut all_visitor = CounterVisitor::default();
    runtime.visit_metrics(&mut all_visitor);
    assert_eq!(
        all_visitor.counters,
        vec![
            ("cache_hits".to_string(), 3),
            ("queue_pushes".to_string(), 5),
        ]
    );

    let mut queue_visitor = CounterVisitor::default();
    runtime.visit_metrics_for_scope(&MetricScope::new("shardmap.queue"), &mut queue_visitor);
    assert_eq!(
        queue_visitor.counters,
        vec![("queue_pushes".to_string(), 5)]
    );
}

#[test]
fn runtime_owns_shared_span_collector() {
    let runtime = Runtime::new(RuntimeConfig::default());
    let exporter_collector = Arc::clone(runtime.span_collector());

    {
        let mut span = runtime.start_span("cache_lookup", SpanKind::Internal);
        span.set_attribute("component", "shardmap");
    }

    {
        let mut span = runtime.start_span("index_probe", SpanKind::Internal);
        span.set_attribute("hit", true);
    }

    runtime.flush_local_spans();

    let mut spans = Vec::new();
    exporter_collector.drain_into(&mut spans);

    assert!(Arc::ptr_eq(runtime.span_collector(), &exporter_collector));
    assert_eq!(runtime.span_collector().recorded_count(), 2);
    assert_eq!(spans.len(), 2);
    assert!(spans.iter().any(|span| span.name == "cache_lookup"));
    assert!(spans.iter().any(|span| span.name == "index_probe"));
}

#[test]
fn runtime_forwards_traceparent_and_drains_spans() {
    let runtime = Runtime::new(RuntimeConfig::default());
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    {
        let mut span =
            runtime.start_span_from_traceparent(Some(traceparent), "inbound", SpanKind::Server);
        span.set_attribute("component", "shardmap");
    }

    runtime.flush_local_spans();

    let mut spans = Vec::new();
    runtime.drain_spans_into(&mut spans);

    assert_eq!(spans.len(), 1);
    let span = &spans[0];
    assert_eq!(span.name, "inbound");
    assert_eq!(
        span.trace_id.to_string(),
        "4bf92f3577b34da6a3ce929d0e0e4736"
    );
    assert_eq!(span.parent_span_id.to_string(), "00f067aa0ba902b7");
}

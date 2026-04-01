use opentelemetry::metrics::MeterProvider;
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use ophanim::{LabelEnum, LabeledCounter, LabeledGauge, LabeledHistogram};
use std::collections::BTreeMap;

#[derive(Copy, Clone, Debug, PartialEq)]
enum HttpMethod {
    Get,
    Post,
}

impl LabelEnum for HttpMethod {
    const CARDINALITY: usize = 2;
    const LABEL_NAME: &'static str = "method";

    fn as_index(self) -> usize {
        self as usize
    }

    fn from_index(index: usize) -> Self {
        match index {
            0 => Self::Get,
            _ => Self::Post,
        }
    }

    fn variant_name(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Post => "post",
        }
    }
}

fn attr_method<'a>(attrs: impl Iterator<Item = &'a KeyValue>) -> Option<String> {
    for kv in attrs {
        if kv.key.as_str() == "method" {
            return match &kv.value {
                Value::String(s) => Some(s.as_str().to_string()),
                _ => None,
            };
        }
    }
    None
}

#[test]
fn test_ophanim_and_otel_produce_equivalent_label_values() {
    let fast_counter = LabeledCounter::<HttpMethod>::new(4);
    let fast_gauge = LabeledGauge::<HttpMethod>::new();
    let fast_hist = LabeledHistogram::<HttpMethod>::new(&[100, 1000], 4);

    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    let meter = provider.meter("parity");
    let otel_counter = meter.u64_counter("reqs").build();
    let otel_gauge = meter.i64_gauge("queue").build();
    let otel_hist = meter.u64_histogram("latency").build();

    // Deterministic event stream
    fast_counter.inc(HttpMethod::Get);
    otel_counter.add(1, &[KeyValue::new("method", "get")]);

    fast_counter.inc(HttpMethod::Get);
    otel_counter.add(1, &[KeyValue::new("method", "get")]);

    fast_counter.add(HttpMethod::Post, 5);
    otel_counter.add(5, &[KeyValue::new("method", "post")]);

    fast_gauge.set(HttpMethod::Get, 10);
    otel_gauge.record(10, &[KeyValue::new("method", "get")]);
    fast_gauge.set(HttpMethod::Post, 20);
    otel_gauge.record(20, &[KeyValue::new("method", "post")]);
    fast_gauge.set(HttpMethod::Get, 15);
    otel_gauge.record(15, &[KeyValue::new("method", "get")]);

    for value in [50_u64, 500, 5000] {
        fast_hist.record(HttpMethod::Get, value);
        otel_hist.record(value, &[KeyValue::new("method", "get")]);
    }
    fast_hist.record(HttpMethod::Post, 200);
    otel_hist.record(200, &[KeyValue::new("method", "post")]);

    let _ = provider.force_flush();
    let resource_metrics = exporter
        .get_finished_metrics()
        .expect("otel export snapshot");

    let mut otel_counter_values: BTreeMap<String, u64> = BTreeMap::new();
    let mut otel_gauge_values: BTreeMap<String, i64> = BTreeMap::new();
    let mut otel_hist_count: BTreeMap<String, u64> = BTreeMap::new();
    let mut otel_hist_sum: BTreeMap<String, u64> = BTreeMap::new();

    for rm in &resource_metrics {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                match (metric.name(), metric.data()) {
                    ("reqs", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                        for dp in sum.data_points() {
                            if let Some(label) = attr_method(dp.attributes()) {
                                otel_counter_values.insert(label, dp.value());
                            }
                        }
                    }
                    ("queue", AggregatedMetrics::I64(MetricData::Gauge(gauge))) => {
                        for dp in gauge.data_points() {
                            if let Some(label) = attr_method(dp.attributes()) {
                                otel_gauge_values.insert(label, dp.value());
                            }
                        }
                    }
                    ("latency", AggregatedMetrics::U64(MetricData::Histogram(hist))) => {
                        for dp in hist.data_points() {
                            if let Some(label) = attr_method(dp.attributes()) {
                                otel_hist_count.insert(label.clone(), dp.count());
                                otel_hist_sum.insert(label, dp.sum());
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    assert_eq!(
        fast_counter.get(HttpMethod::Get) as u64,
        *otel_counter_values.get("get").unwrap_or(&0)
    );
    assert_eq!(
        fast_counter.get(HttpMethod::Post) as u64,
        *otel_counter_values.get("post").unwrap_or(&0)
    );

    assert_eq!(
        fast_gauge.get(HttpMethod::Get),
        *otel_gauge_values.get("get").unwrap_or(&0)
    );
    assert_eq!(
        fast_gauge.get(HttpMethod::Post),
        *otel_gauge_values.get("post").unwrap_or(&0)
    );

    assert_eq!(
        fast_hist.get(HttpMethod::Get).count(),
        *otel_hist_count.get("get").unwrap_or(&0)
    );
    assert_eq!(
        fast_hist.get(HttpMethod::Post).count(),
        *otel_hist_count.get("post").unwrap_or(&0)
    );
    assert_eq!(
        fast_hist.get(HttpMethod::Get).sum(),
        *otel_hist_sum.get("get").unwrap_or(&0)
    );
    assert_eq!(
        fast_hist.get(HttpMethod::Post).sum(),
        *otel_hist_sum.get("post").unwrap_or(&0)
    );
}

//! OTLP (OpenTelemetry Protocol) export for fast-telemetry metrics.
//!
//! Converts fast-telemetry metric types into OTLP protobuf messages for export
//! via HTTP/protobuf to any OTLP-compatible collector.
//!
//! All exports use **cumulative temporality** — values represent running totals
//! since process start. No state tracking is required between export cycles.

use crate::exp_buckets::ExpBucketsSnapshot;
use crate::{
    Counter, Distribution, DynamicCounter, DynamicDistribution, DynamicGauge, DynamicGaugeI64,
    DynamicHistogram, Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter, LabeledGauge,
    LabeledHistogram, LabeledSampledTimer, MaxGauge, MaxGaugeF64, MinGauge, MinGaugeF64,
    SampledTimer,
};

/// Re-export proto types so downstream crates (and the derive macro) can reference them.
pub mod pb {
    pub use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    pub use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    pub use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    pub use opentelemetry_proto::tonic::metrics::v1::{
        self, AggregationTemporality, ExponentialHistogram as OtlpExpHistogram,
        ExponentialHistogramDataPoint, Gauge as OtlpGauge, Histogram as OtlpHistogram,
        HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        exponential_histogram_data_point, metric, number_data_point,
    };
    pub use opentelemetry_proto::tonic::resource::v1::Resource;
    pub use opentelemetry_proto::tonic::trace::v1::{
        ResourceSpans, ScopeSpans, Span as OtlpSpan, Status as OtlpStatus,
        span::{Event as OtlpEvent, SpanKind as OtlpSpanKind},
        status::StatusCode as OtlpStatusCode,
    };
}

/// Trait for exporting a metric as OTLP protobuf `Metric` messages.
///
/// Each implementation appends one or more `Metric` to the output vec.
/// Uses cumulative temporality — no state tracking needed.
///
/// `time_unix_nano` is a pre-computed timestamp (via [`now_nanos`]) shared
/// across all data points in one export cycle for consistency.
pub trait OtlpExport {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    );
}

// ============================================================================
// Helpers
// ============================================================================

fn make_kv(key: &str, value: &str) -> pb::KeyValue {
    pb::KeyValue {
        key: key.to_string(),
        value: Some(pb::AnyValue {
            value: Some(pb::any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn pairs_to_attributes(pairs: &[(String, String)]) -> Vec<pb::KeyValue> {
    pairs.iter().map(|(k, v)| make_kv(k, v)).collect()
}

fn label_to_attribute<L: LabelEnum>(label: L) -> pb::KeyValue {
    make_kv(L::LABEL_NAME, label.variant_name())
}

/// Returns the current time as nanoseconds since the Unix epoch.
///
/// Use this to compute a shared timestamp for a batch of OTLP exports.
pub fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn int_data_point(
    value: i64,
    attributes: Vec<pb::KeyValue>,
    time_unix_nano: u64,
) -> pb::NumberDataPoint {
    pb::NumberDataPoint {
        attributes,
        time_unix_nano,
        value: Some(pb::number_data_point::Value::AsInt(value)),
        ..Default::default()
    }
}

fn double_data_point(
    value: f64,
    attributes: Vec<pb::KeyValue>,
    time_unix_nano: u64,
) -> pb::NumberDataPoint {
    pb::NumberDataPoint {
        attributes,
        time_unix_nano,
        value: Some(pb::number_data_point::Value::AsDouble(value)),
        ..Default::default()
    }
}

/// Convert cumulative bucket counts (as returned by `buckets_cumulative()`) to
/// OTLP's per-bucket counts and explicit bounds.
///
/// OTLP expects non-cumulative bucket counts and omits the +Inf bound from
/// `explicit_bounds` (it's implied by the final bucket).
fn cumulative_to_otlp_buckets(cumulative: &[(u64, u64)]) -> (Vec<u64>, Vec<f64>) {
    cumulative_to_otlp_buckets_iter(cumulative.iter().copied())
}

fn cumulative_to_otlp_buckets_iter(
    cumulative: impl IntoIterator<Item = (u64, u64)>,
) -> (Vec<u64>, Vec<f64>) {
    let iter = cumulative.into_iter();
    let (lower, _) = iter.size_hint();
    let mut bucket_counts = Vec::with_capacity(lower);
    let mut explicit_bounds = Vec::with_capacity(lower.saturating_sub(1));
    let mut prev = 0u64;

    for (bound, cum_count) in iter {
        bucket_counts.push(cum_count.saturating_sub(prev));
        prev = cum_count;
        if bound != u64::MAX {
            explicit_bounds.push(bound as f64);
        }
    }

    (bucket_counts, explicit_bounds)
}

/// Build an OTLP `Resource` with a service name and optional extra attributes.
pub fn build_resource(service_name: &str, attrs: &[(&str, &str)]) -> pb::Resource {
    let mut attributes = vec![make_kv("service.name", service_name)];
    for (k, v) in attrs {
        attributes.push(make_kv(k, v));
    }
    pb::Resource {
        attributes,
        ..Default::default()
    }
}

/// Wrap a vec of `Metric` into a full `ExportMetricsServiceRequest`.
///
/// Takes the resource by reference and clones it into the request.
pub fn build_export_request(
    resource: &pb::Resource,
    scope_name: &str,
    metrics: Vec<pb::Metric>,
) -> pb::ExportMetricsServiceRequest {
    pb::ExportMetricsServiceRequest {
        resource_metrics: vec![pb::ResourceMetrics {
            resource: Some(resource.clone()),
            scope_metrics: vec![pb::ScopeMetrics {
                scope: Some(pb::InstrumentationScope {
                    name: scope_name.to_string(),
                    ..Default::default()
                }),
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

// ============================================================================
// OtlpExport implementations
// ============================================================================

impl OtlpExport for Counter {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let value = self.sum() as i64;
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Sum(pb::Sum {
                // Counter uses AtomicIsize — callers can add negative values,
                // so we cannot guarantee monotonicity.
                data_points: vec![int_data_point(value, Vec::new(), time_unix_nano)],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: false,
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for Gauge {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![int_data_point(self.get(), Vec::new(), time_unix_nano)],
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for GaugeF64 {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![double_data_point(self.get(), Vec::new(), time_unix_nano)],
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for MaxGauge {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![int_data_point(self.get(), Vec::new(), time_unix_nano)],
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for MaxGaugeF64 {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![double_data_point(self.get(), Vec::new(), time_unix_nano)],
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for MinGauge {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![int_data_point(self.get(), Vec::new(), time_unix_nano)],
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for MinGaugeF64 {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![double_data_point(self.get(), Vec::new(), time_unix_nano)],
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for Histogram {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let cumulative = self.buckets_cumulative();
        let count = self.count();
        let sum = self.sum();
        let (bucket_counts, explicit_bounds) = cumulative_to_otlp_buckets(&cumulative);

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                data_points: vec![pb::HistogramDataPoint {
                    time_unix_nano,
                    count,
                    sum: Some(sum as f64),
                    bucket_counts,
                    explicit_bounds,
                    ..Default::default()
                }],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for SampledTimer {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let calls_name = format!("{name}.calls");
        let samples_name = format!("{name}.samples");
        let calls_description = format!("{description} total calls");
        let samples_description = format!("{description} sampled latency in nanoseconds");
        self.calls_metric()
            .export_otlp(metrics, &calls_name, &calls_description, time_unix_nano);
        self.histogram()
            .export_otlp(metrics, &samples_name, &samples_description, time_unix_nano);
    }
}

/// Build an OTLP ExponentialHistogramDataPoint from an ExpBucketsSnapshot.
fn exp_histogram_data_point(
    snap: &ExpBucketsSnapshot,
    attributes: Vec<pb::KeyValue>,
    time_unix_nano: u64,
) -> pb::ExponentialHistogramDataPoint {
    // Find the range of non-zero positive buckets to compact the array.
    let mut first_nonzero: Option<usize> = None;
    let mut last_nonzero: Option<usize> = None;
    for (i, &c) in snap.positive.iter().enumerate() {
        if c > 0 {
            if first_nonzero.is_none() {
                first_nonzero = Some(i);
            }
            last_nonzero = Some(i);
        }
    }

    let positive = match (first_nonzero, last_nonzero) {
        (Some(first), Some(last)) => {
            let bucket_counts: Vec<u64> = snap.positive[first..=last].to_vec();
            Some(pb::exponential_histogram_data_point::Buckets {
                offset: first as i32,
                bucket_counts,
            })
        }
        _ => None,
    };

    pb::ExponentialHistogramDataPoint {
        attributes,
        time_unix_nano,
        count: snap.count,
        sum: Some(snap.sum as f64),
        scale: 0, // base-2
        zero_count: snap.zero_count,
        positive,
        negative: None, // u64 values are always non-negative
        min: snap.min().map(|v| v as f64),
        max: snap.max().map(|v| v as f64),
        ..Default::default()
    }
}

impl OtlpExport for Distribution {
    /// Distribution exports as a native OTLP ExponentialHistogram (scale 0, base-2).
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let snap = self.buckets_snapshot();
        let dp = exp_histogram_data_point(&snap, Vec::new(), time_unix_nano);

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::ExponentialHistogram(
                pb::OtlpExpHistogram {
                    data_points: vec![dp],
                    aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                },
            )),
            ..Default::default()
        });
    }
}

// ============================================================================
// Labeled metric implementations
// ============================================================================

impl<L: LabelEnum> OtlpExport for LabeledCounter<L> {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let data_points: Vec<_> = self
            .iter()
            .map(|(label, count)| {
                int_data_point(
                    count as i64,
                    vec![label_to_attribute(label)],
                    time_unix_nano,
                )
            })
            .collect();

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Sum(pb::Sum {
                data_points,
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: false,
            })),
            ..Default::default()
        });
    }
}

impl<L: LabelEnum> OtlpExport for LabeledGauge<L> {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let data_points: Vec<_> = self
            .iter()
            .map(|(label, value)| {
                int_data_point(value, vec![label_to_attribute(label)], time_unix_nano)
            })
            .collect();

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge { data_points })),
            ..Default::default()
        });
    }
}

impl<L: LabelEnum> OtlpExport for LabeledHistogram<L> {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let mut data_points = Vec::new();

        for (label, buckets, sum, count) in self.iter() {
            let attrs = vec![label_to_attribute(label)];
            let (bucket_counts, explicit_bounds) = cumulative_to_otlp_buckets(&buckets);

            data_points.push(pb::HistogramDataPoint {
                attributes: attrs,
                time_unix_nano,
                count,
                sum: Some(sum as f64),
                bucket_counts,
                explicit_bounds,
                ..Default::default()
            });
        }

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                data_points,
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });
    }
}

impl<L: LabelEnum> OtlpExport for LabeledSampledTimer<L> {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let calls_name = format!("{name}.calls");
        let samples_name = format!("{name}.samples");
        let calls_description = format!("{description} total calls");
        let samples_description = format!("{description} sampled latency in nanoseconds");

        let mut call_points = Vec::new();
        let mut sample_points = Vec::new();

        for (label, calls, histogram) in self.iter() {
            call_points.push(int_data_point(
                calls.sum() as i64,
                vec![label_to_attribute(label)],
                time_unix_nano,
            ));

            let (bucket_counts, explicit_bounds) =
                cumulative_to_otlp_buckets(&histogram.buckets_cumulative());
            sample_points.push(pb::HistogramDataPoint {
                attributes: vec![label_to_attribute(label)],
                time_unix_nano,
                count: histogram.count(),
                sum: Some(histogram.sum() as f64),
                bucket_counts,
                explicit_bounds,
                ..Default::default()
            });
        }

        metrics.push(pb::Metric {
            name: calls_name,
            description: calls_description,
            data: Some(pb::metric::Data::Sum(pb::Sum {
                data_points: call_points,
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: false,
            })),
            ..Default::default()
        });

        metrics.push(pb::Metric {
            name: samples_name,
            description: samples_description,
            data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                data_points: sample_points,
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });
    }
}

// ============================================================================
// Dynamic metric implementations
// ============================================================================

impl OtlpExport for DynamicCounter {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let mut data_points = Vec::new();
        self.visit_series(|pairs, count| {
            data_points.push(int_data_point(
                count as i64,
                pairs_to_attributes(pairs),
                time_unix_nano,
            ));
        });

        if data_points.is_empty() {
            return;
        }

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Sum(pb::Sum {
                data_points,
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: false,
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for DynamicGauge {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let mut data_points = Vec::new();
        self.visit_series(|pairs, value| {
            data_points.push(double_data_point(
                value,
                pairs_to_attributes(pairs),
                time_unix_nano,
            ));
        });

        if data_points.is_empty() {
            return;
        }

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge { data_points })),
            ..Default::default()
        });
    }
}

impl OtlpExport for DynamicGaugeI64 {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let mut data_points = Vec::new();
        self.visit_series(|pairs, value| {
            data_points.push(int_data_point(
                value,
                pairs_to_attributes(pairs),
                time_unix_nano,
            ));
        });

        if data_points.is_empty() {
            return;
        }

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge { data_points })),
            ..Default::default()
        });
    }
}

impl OtlpExport for DynamicHistogram {
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let mut data_points = Vec::new();

        self.visit_series(|pairs, series| {
            let (bucket_counts, explicit_bounds) =
                cumulative_to_otlp_buckets_iter(series.buckets_cumulative_iter());

            data_points.push(pb::HistogramDataPoint {
                attributes: pairs_to_attributes(pairs),
                time_unix_nano,
                count: series.count(),
                sum: Some(series.sum() as f64),
                bucket_counts,
                explicit_bounds,
                ..Default::default()
            });
        });

        if data_points.is_empty() {
            return;
        }

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                data_points,
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });
    }
}

impl OtlpExport for DynamicDistribution {
    /// Exports as native OTLP ExponentialHistogram (scale 0, base-2) per label set.
    fn export_otlp(
        &self,
        metrics: &mut Vec<pb::Metric>,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let mut data_points = Vec::new();

        self.visit_series(|pairs, _count, _sum, snap| {
            let attrs = pairs_to_attributes(pairs);
            data_points.push(exp_histogram_data_point(&snap, attrs, time_unix_nano));
        });

        if data_points.is_empty() {
            return;
        }

        metrics.push(pb::Metric {
            name: name.to_string(),
            description: description.to_string(),
            data: Some(pb::metric::Data::ExponentialHistogram(
                pb::OtlpExpHistogram {
                    data_points,
                    aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                },
            )),
            ..Default::default()
        });
    }
}

// ============================================================================
// Trace export
// ============================================================================

use crate::span::{CompletedSpan, SpanKind, SpanStatus, SpanValue};

impl CompletedSpan {
    /// Convert this completed span into an OTLP protobuf `Span`.
    pub fn to_otlp(&self) -> pb::OtlpSpan {
        let kind = match self.kind {
            SpanKind::Internal => pb::OtlpSpanKind::Internal,
            SpanKind::Server => pb::OtlpSpanKind::Server,
            SpanKind::Client => pb::OtlpSpanKind::Client,
            SpanKind::Producer => pb::OtlpSpanKind::Producer,
            SpanKind::Consumer => pb::OtlpSpanKind::Consumer,
        };

        let status = match &self.status {
            SpanStatus::Unset => None,
            SpanStatus::Ok => Some(pb::OtlpStatus {
                code: pb::OtlpStatusCode::Ok as i32,
                message: String::new(),
            }),
            SpanStatus::Error { message } => Some(pb::OtlpStatus {
                code: pb::OtlpStatusCode::Error as i32,
                message: message.to_string(),
            }),
        };

        let attributes: Vec<pb::KeyValue> = self
            .attributes
            .iter()
            .map(|attr| {
                let value = match &attr.value {
                    SpanValue::String(s) => pb::any_value::Value::StringValue(s.to_string()),
                    SpanValue::I64(v) => pb::any_value::Value::IntValue(*v),
                    SpanValue::F64(v) => pb::any_value::Value::DoubleValue(*v),
                    SpanValue::Bool(v) => pb::any_value::Value::BoolValue(*v),
                    SpanValue::Uuid(u) => pb::any_value::Value::StringValue(u.to_string()),
                };
                pb::KeyValue {
                    key: attr.key.to_string(),
                    value: Some(pb::AnyValue { value: Some(value) }),
                }
            })
            .collect();

        let events: Vec<pb::OtlpEvent> = self
            .events
            .iter()
            .map(|evt| {
                let attrs: Vec<pb::KeyValue> = evt
                    .attributes
                    .iter()
                    .map(|a| {
                        let v = match &a.value {
                            SpanValue::String(s) => {
                                pb::any_value::Value::StringValue(s.to_string())
                            }
                            SpanValue::I64(v) => pb::any_value::Value::IntValue(*v),
                            SpanValue::F64(v) => pb::any_value::Value::DoubleValue(*v),
                            SpanValue::Bool(v) => pb::any_value::Value::BoolValue(*v),
                            SpanValue::Uuid(u) => pb::any_value::Value::StringValue(u.to_string()),
                        };
                        pb::KeyValue {
                            key: a.key.to_string(),
                            value: Some(pb::AnyValue { value: Some(v) }),
                        }
                    })
                    .collect();
                pb::OtlpEvent {
                    time_unix_nano: evt.time_ns,
                    name: evt.name.to_string(),
                    attributes: attrs,
                    dropped_attributes_count: 0,
                }
            })
            .collect();

        pb::OtlpSpan {
            trace_id: self.trace_id.as_bytes().to_vec(),
            span_id: self.span_id.as_bytes().to_vec(),
            parent_span_id: if self.parent_span_id.is_invalid() {
                Vec::new()
            } else {
                self.parent_span_id.as_bytes().to_vec()
            },
            name: self.name.to_string(),
            kind: kind as i32,
            start_time_unix_nano: self.start_time_ns,
            end_time_unix_nano: self.end_time_ns,
            attributes,
            events,
            status,
            ..Default::default()
        }
    }
}

/// Wrap a vec of OTLP `Span` protos into a full `ExportTraceServiceRequest`.
///
/// Takes the resource by reference and clones it into the request.
pub fn build_trace_export_request(
    resource: &pb::Resource,
    scope_name: &str,
    spans: Vec<pb::OtlpSpan>,
) -> pb::ExportTraceServiceRequest {
    pb::ExportTraceServiceRequest {
        resource_spans: vec![pb::ResourceSpans {
            resource: Some(resource.clone()),
            scope_spans: vec![pb::ScopeSpans {
                scope: Some(pb::InstrumentationScope {
                    name: scope_name.to_string(),
                    ..Default::default()
                }),
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_timestamp() -> u64 {
        1_000_000_000 // fixed timestamp for deterministic tests
    }

    #[test]
    fn test_counter_otlp() {
        let counter = Counter::new(4);
        counter.add(42);

        let mut metrics = Vec::new();
        counter.export_otlp(
            &mut metrics,
            "test_counter",
            "A test counter",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "test_counter");
        assert_eq!(metrics[0].description, "A test counter");

        let data = metrics[0].data.as_ref().expect("missing data");
        match data {
            pb::metric::Data::Sum(sum) => {
                // Counter uses isize (can go negative), so is_monotonic must be false
                assert!(!sum.is_monotonic);
                assert_eq!(
                    sum.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert_eq!(sum.data_points.len(), 1);
                assert_eq!(
                    sum.data_points[0].value,
                    Some(pb::number_data_point::Value::AsInt(42))
                );
                assert_eq!(sum.data_points[0].time_unix_nano, test_timestamp());
            }
            _ => panic!("expected Sum, got {:?}", data),
        }
    }

    #[test]
    fn test_gauge_otlp() {
        let gauge = Gauge::new();
        gauge.set(-10);

        let mut metrics = Vec::new();
        gauge.export_otlp(&mut metrics, "test_gauge", "A test gauge", test_timestamp());

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Gauge(g) => {
                assert_eq!(g.data_points.len(), 1);
                assert_eq!(
                    g.data_points[0].value,
                    Some(pb::number_data_point::Value::AsInt(-10))
                );
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn test_gauge_f64_otlp() {
        let gauge = GaugeF64::new();
        gauge.set(3.125);

        let mut metrics = Vec::new();
        gauge.export_otlp(&mut metrics, "test_gauge_f64", "", test_timestamp());

        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Gauge(g) => {
                assert_eq!(g.data_points.len(), 1);
                match g.data_points[0].value {
                    Some(pb::number_data_point::Value::AsDouble(v)) => {
                        assert!((v - 3.125).abs() < 1e-10);
                    }
                    ref other => panic!("expected AsDouble, got {:?}", other),
                }
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn test_histogram_otlp() {
        let h = Histogram::new(&[10, 100], 4);
        h.record(5);
        h.record(50);
        h.record(500);

        let mut metrics = Vec::new();
        h.export_otlp(
            &mut metrics,
            "test_hist",
            "A test histogram",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Histogram(hist) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 1);

                let dp = &hist.data_points[0];
                assert_eq!(dp.count, 3);
                assert_eq!(dp.sum, Some(555.0));
                assert_eq!(dp.explicit_bounds, vec![10.0, 100.0]);
                assert_eq!(dp.bucket_counts, vec![1, 1, 1]);
                assert_eq!(dp.time_unix_nano, test_timestamp());
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    #[test]
    fn test_distribution_otlp() {
        let dist = Distribution::new(4);
        dist.record(100);
        dist.record(200);
        dist.record(300);

        let mut metrics = Vec::new();
        dist.export_otlp(
            &mut metrics,
            "test_dist",
            "A distribution",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "test_dist");

        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::ExponentialHistogram(hist) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 1);

                let dp = &hist.data_points[0];
                assert_eq!(dp.count, 3);
                assert_eq!(dp.sum, Some(600.0));
                assert_eq!(dp.scale, 0);
                assert_eq!(dp.zero_count, 0);
                assert_eq!(dp.time_unix_nano, test_timestamp());
                // positive buckets should be set
                assert!(dp.positive.is_some());
                let positive = dp.positive.as_ref().expect("positive buckets");
                // 100 -> bucket 6, 200 -> bucket 7, 300 -> bucket 8
                assert!(!positive.bucket_counts.is_empty());
            }
            other => panic!("expected ExponentialHistogram, got {:?}", other),
        }
    }

    #[test]
    fn test_dynamic_counter_otlp() {
        let counter = DynamicCounter::new(4);
        counter.add(&[("env", "prod"), ("region", "us")], 10);
        counter.add(&[("env", "staging"), ("region", "eu")], 5);

        let mut metrics = Vec::new();
        counter.export_otlp(&mut metrics, "requests", "Request count", test_timestamp());

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Sum(sum) => {
                assert!(!sum.is_monotonic);
                assert_eq!(sum.data_points.len(), 2);
                for dp in &sum.data_points {
                    assert_eq!(dp.attributes.len(), 2);
                }
            }
            other => panic!("expected Sum, got {:?}", other),
        }
    }

    #[test]
    fn test_build_export_request() {
        let resource = build_resource("test-service", &[("version", "1.0")]);
        let counter = Counter::new(4);
        counter.add(1);

        let mut metrics = Vec::new();
        counter.export_otlp(&mut metrics, "my_counter", "", test_timestamp());

        let request = build_export_request(&resource, "fast-telemetry", metrics);

        assert_eq!(request.resource_metrics.len(), 1);
        let rm = &request.resource_metrics[0];
        let res = rm.resource.as_ref().expect("missing resource");
        assert_eq!(res.attributes.len(), 2); // service.name + version
        assert_eq!(res.attributes[0].key, "service.name");

        assert_eq!(rm.scope_metrics.len(), 1);
        let sm = &rm.scope_metrics[0];
        let scope = sm.scope.as_ref().expect("missing scope");
        assert_eq!(scope.name, "fast-telemetry");
        assert_eq!(sm.metrics.len(), 1);
    }

    #[test]
    fn test_make_kv() {
        let kv = make_kv("foo", "bar");
        assert_eq!(kv.key, "foo");
        match kv
            .value
            .expect("missing value")
            .value
            .expect("missing inner")
        {
            pb::any_value::Value::StringValue(s) => assert_eq!(s, "bar"),
            other => panic!("expected StringValue, got {:?}", other),
        }
    }

    // -- Labeled metric tests --

    #[derive(Copy, Clone, Debug)]
    enum TestLabel {
        A,
        B,
        C,
    }

    impl crate::LabelEnum for TestLabel {
        const CARDINALITY: usize = 3;
        const LABEL_NAME: &'static str = "test";

        fn as_index(self) -> usize {
            self as usize
        }
        fn from_index(index: usize) -> Self {
            match index {
                0 => Self::A,
                1 => Self::B,
                _ => Self::C,
            }
        }
        fn variant_name(self) -> &'static str {
            match self {
                Self::A => "a",
                Self::B => "b",
                Self::C => "c",
            }
        }
    }

    #[test]
    fn test_labeled_counter_otlp() {
        let counter = LabeledCounter::<TestLabel>::new(4);
        counter.add(TestLabel::A, 10);
        counter.add(TestLabel::B, 20);

        let mut metrics = Vec::new();
        counter.export_otlp(
            &mut metrics,
            "labeled_counter",
            "By label",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Sum(sum) => {
                assert!(!sum.is_monotonic);
                assert_eq!(sum.data_points.len(), 3); // A, B, C (all variants exported)
                // Find the data point for label A
                let dp_a = sum.data_points.iter().find(|dp| {
                    dp.attributes.iter().any(|kv| kv.key == "test" && matches!(&kv.value, Some(v) if matches!(&v.value, Some(pb::any_value::Value::StringValue(s)) if s == "a")))
                }).expect("missing data point for label A");
                assert_eq!(dp_a.value, Some(pb::number_data_point::Value::AsInt(10)));
            }
            other => panic!("expected Sum, got {:?}", other),
        }
    }

    #[test]
    fn test_labeled_gauge_otlp() {
        let gauge = LabeledGauge::<TestLabel>::new();
        gauge.set(TestLabel::A, 42);
        gauge.set(TestLabel::C, -5);

        let mut metrics = Vec::new();
        gauge.export_otlp(&mut metrics, "labeled_gauge", "By label", test_timestamp());

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Gauge(g) => {
                assert_eq!(g.data_points.len(), 3);
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn test_labeled_histogram_otlp() {
        let h = LabeledHistogram::<TestLabel>::new(&[10, 100], 4);
        h.record(TestLabel::A, 5);
        h.record(TestLabel::A, 50);
        h.record(TestLabel::B, 500);

        let mut metrics = Vec::new();
        h.export_otlp(&mut metrics, "labeled_hist", "By label", test_timestamp());

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Histogram(hist) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 3); // all variants
                // Each data point should have a label attribute
                for dp in &hist.data_points {
                    assert_eq!(dp.attributes.len(), 1);
                    assert_eq!(dp.attributes[0].key, "test");
                    assert_eq!(dp.time_unix_nano, test_timestamp());
                }
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    // -- Dynamic metric tests --

    #[test]
    fn test_dynamic_gauge_otlp() {
        let gauge = DynamicGauge::new(4);
        gauge.set(&[("host", "node1")], 3.125);
        gauge.set(&[("host", "node2")], 2.72);

        let mut metrics = Vec::new();
        gauge.export_otlp(
            &mut metrics,
            "cpu_usage",
            "CPU percentage",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Gauge(g) => {
                assert_eq!(g.data_points.len(), 2);
                for dp in &g.data_points {
                    assert_eq!(dp.attributes.len(), 1);
                    assert!(matches!(
                        dp.value,
                        Some(pb::number_data_point::Value::AsDouble(_))
                    ));
                }
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn test_dynamic_gauge_i64_otlp() {
        let gauge = DynamicGaugeI64::new(4);
        gauge.set(&[("region", "us")], 100);
        gauge.set(&[("region", "eu")], 200);

        let mut metrics = Vec::new();
        gauge.export_otlp(
            &mut metrics,
            "connections",
            "Active connections",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Gauge(g) => {
                assert_eq!(g.data_points.len(), 2);
                for dp in &g.data_points {
                    assert_eq!(dp.attributes.len(), 1);
                    assert!(matches!(
                        dp.value,
                        Some(pb::number_data_point::Value::AsInt(_))
                    ));
                }
            }
            other => panic!("expected Gauge, got {:?}", other),
        }
    }

    #[test]
    fn test_dynamic_histogram_otlp() {
        let h = DynamicHistogram::new(&[10, 100, 1000], 4);
        h.record(&[("endpoint", "/api")], 5);
        h.record(&[("endpoint", "/api")], 50);
        h.record(&[("endpoint", "/health")], 500);

        let mut metrics = Vec::new();
        h.export_otlp(&mut metrics, "latency", "Request latency", test_timestamp());

        assert_eq!(metrics.len(), 1);
        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::Histogram(hist) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 2); // /api and /health
                for dp in &hist.data_points {
                    assert_eq!(dp.attributes.len(), 1);
                    assert_eq!(dp.attributes[0].key, "endpoint");
                    assert_eq!(dp.time_unix_nano, test_timestamp());
                    // explicit_bounds should not include +Inf
                    assert_eq!(dp.explicit_bounds, vec![10.0, 100.0, 1000.0]);
                }
            }
            other => panic!("expected Histogram, got {:?}", other),
        }
    }

    #[test]
    fn test_dynamic_distribution_otlp() {
        let dist = DynamicDistribution::new(4);
        dist.record(&[("method", "GET")], 100);
        dist.record(&[("method", "GET")], 200);
        dist.record(&[("method", "POST")], 300);

        let mut metrics = Vec::new();
        dist.export_otlp(
            &mut metrics,
            "response_size",
            "Size in bytes",
            test_timestamp(),
        );

        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "response_size");

        match metrics[0].data.as_ref().expect("missing data") {
            pb::metric::Data::ExponentialHistogram(hist) => {
                assert_eq!(
                    hist.aggregation_temporality,
                    pb::AggregationTemporality::Cumulative as i32
                );
                assert_eq!(hist.data_points.len(), 2); // GET and POST
                for dp in &hist.data_points {
                    assert_eq!(dp.attributes.len(), 1);
                    assert_eq!(dp.attributes[0].key, "method");
                    assert_eq!(dp.scale, 0);
                    assert!(dp.positive.is_some());
                }
            }
            other => panic!("expected ExponentialHistogram, got {:?}", other),
        }
    }

    #[test]
    fn test_empty_dynamic_metrics_produce_nothing() {
        let counter = DynamicCounter::new(4);
        let gauge = DynamicGauge::new(4);
        let gauge_i64 = DynamicGaugeI64::new(4);
        let hist = DynamicHistogram::new(&[10], 4);
        let dist = DynamicDistribution::new(4);

        let mut metrics = Vec::new();
        let ts = test_timestamp();
        counter.export_otlp(&mut metrics, "c", "", ts);
        gauge.export_otlp(&mut metrics, "g", "", ts);
        gauge_i64.export_otlp(&mut metrics, "gi", "", ts);
        hist.export_otlp(&mut metrics, "h", "", ts);
        dist.export_otlp(&mut metrics, "d", "", ts);

        assert!(
            metrics.is_empty(),
            "empty dynamic metrics should produce no output"
        );
    }

    #[test]
    fn test_cumulative_to_otlp_buckets_helper() {
        // Input: cumulative [(10, 1), (100, 3), (u64::MAX, 5)]
        // Expected per-bucket: [1, 2, 2], bounds: [10.0, 100.0]
        let cumulative = vec![(10, 1), (100, 3), (u64::MAX, 5)];
        let (counts, bounds) = cumulative_to_otlp_buckets(&cumulative);
        assert_eq!(counts, vec![1, 2, 2]);
        assert_eq!(bounds, vec![10.0, 100.0]);
    }

    // -- Trace export tests --

    #[test]
    fn test_completed_span_to_otlp() {
        use crate::span::{SpanAttribute, SpanEvent, SpanKind, SpanStatus};
        use crate::span::{SpanId, TraceId};

        let completed = CompletedSpan {
            trace_id: TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            span_id: SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            parent_span_id: SpanId::from_hex("1234567890abcdef").unwrap(),
            name: "test_operation".into(),
            kind: SpanKind::Server,
            start_time_ns: 1_000_000_000,
            end_time_ns: 2_000_000_000,
            status: SpanStatus::Ok,
            attributes: vec![
                SpanAttribute::new("http.method", "GET"),
                SpanAttribute::new("http.status_code", 200i64),
            ],
            events: vec![SpanEvent {
                name: "processing".into(),
                time_ns: 1_500_000_000,
                attributes: vec![SpanAttribute::new("step", "validate")],
            }],
        };

        let otlp = completed.to_otlp();

        assert_eq!(
            otlp.trace_id,
            &[
                0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e,
                0x47, 0x36
            ]
        );
        assert_eq!(
            otlp.span_id,
            &[0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]
        );
        assert_eq!(
            otlp.parent_span_id,
            &[0x12, 0x34, 0x56, 0x78, 0x90, 0xab, 0xcd, 0xef]
        );
        assert_eq!(otlp.name, "test_operation");
        assert_eq!(otlp.kind, pb::OtlpSpanKind::Server as i32);
        assert_eq!(otlp.start_time_unix_nano, 1_000_000_000);
        assert_eq!(otlp.end_time_unix_nano, 2_000_000_000);

        // Status
        let status = otlp.status.unwrap();
        assert_eq!(status.code, pb::OtlpStatusCode::Ok as i32);

        // Attributes
        assert_eq!(otlp.attributes.len(), 2);
        assert_eq!(otlp.attributes[0].key, "http.method");
        assert_eq!(otlp.attributes[1].key, "http.status_code");

        // Events
        assert_eq!(otlp.events.len(), 1);
        assert_eq!(otlp.events[0].name, "processing");
        assert_eq!(otlp.events[0].time_unix_nano, 1_500_000_000);
        assert_eq!(otlp.events[0].attributes.len(), 1);
    }

    #[test]
    fn test_completed_span_root_has_empty_parent() {
        use crate::span::{SpanId, TraceId};

        let completed = CompletedSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: "root".into(),
            kind: SpanKind::Server,
            start_time_ns: 1_000_000_000,
            end_time_ns: 2_000_000_000,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        };

        let otlp = completed.to_otlp();
        assert!(
            otlp.parent_span_id.is_empty(),
            "root span should have empty parent_span_id"
        );
        assert!(otlp.status.is_none(), "Unset status should map to None");
    }

    #[test]
    fn test_completed_span_error_status() {
        use crate::span::{SpanId, TraceId};

        let completed = CompletedSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: "failing_op".into(),
            kind: SpanKind::Internal,
            start_time_ns: 1_000_000_000,
            end_time_ns: 2_000_000_000,
            status: SpanStatus::Error {
                message: "connection refused".into(),
            },
            attributes: Vec::new(),
            events: Vec::new(),
        };

        let otlp = completed.to_otlp();
        let status = otlp.status.unwrap();
        assert_eq!(status.code, pb::OtlpStatusCode::Error as i32);
        assert_eq!(status.message, "connection refused");
    }

    #[test]
    fn test_build_trace_export_request() {
        use crate::span::{SpanId, TraceId};

        let resource = build_resource("test-service", &[("version", "1.0")]);
        let completed = CompletedSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: "test".into(),
            kind: SpanKind::Server,
            start_time_ns: 1_000_000_000,
            end_time_ns: 2_000_000_000,
            status: SpanStatus::Ok,
            attributes: Vec::new(),
            events: Vec::new(),
        };

        let otlp_span = completed.to_otlp();
        let request = build_trace_export_request(&resource, "fast-telemetry", vec![otlp_span]);

        assert_eq!(request.resource_spans.len(), 1);
        let rs = &request.resource_spans[0];
        let res = rs.resource.as_ref().unwrap();
        assert_eq!(res.attributes.len(), 2); // service.name + version

        assert_eq!(rs.scope_spans.len(), 1);
        let ss = &rs.scope_spans[0];
        let scope = ss.scope.as_ref().unwrap();
        assert_eq!(scope.name, "fast-telemetry");
        assert_eq!(ss.spans.len(), 1);
    }
}

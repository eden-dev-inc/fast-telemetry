//! First-party ClickHouse row export for metric primitives.
//!
//! This feature builds ClickHouse-native row batches directly from
//! `fast-telemetry` primitives. It avoids the `pb::Metric` intermediate used by
//! OTLP export while keeping transport and connection management in
//! `fast-telemetry-export`.

use crate::exp_buckets::ExpBucketsSnapshot;
use crate::{
    Counter, Distribution, DynamicCounter, DynamicDistribution, DynamicGauge, DynamicGaugeI64,
    DynamicHistogram, Gauge, GaugeF64, Histogram, LabelEnum, LabeledCounter, LabeledGauge,
    LabeledHistogram, LabeledSampledTimer, MaxGauge, MaxGaugeF64, MinGauge, MinGaugeF64,
    SampledTimer,
};
use indexmap::IndexMap;
use klickhouse::{DateTime64, Tz};

const AGGREGATION_TEMPORALITY_CUMULATIVE: i32 = 2;

/// Direct ClickHouse export trait for metric primitives.
pub trait ClickHouseExport {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    );
}

/// OTel-standard ClickHouse rows built directly from fast-telemetry primitives.
#[derive(Debug)]
pub struct ClickHouseMetricBatch {
    resource_attributes: IndexMap<String, String>,
    service_name: String,
    scope_name: String,
    pub sums: Vec<SumRow>,
    pub gauges: Vec<GaugeRow>,
    pub histograms: Vec<HistogramRow>,
    pub exp_histograms: Vec<ExpHistogramRow>,
}

impl ClickHouseMetricBatch {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self::with_scope(service_name, "fast-telemetry")
    }

    pub fn with_scope(service_name: impl Into<String>, scope_name: impl Into<String>) -> Self {
        let service_name = service_name.into();
        let mut resource_attributes = IndexMap::new();
        resource_attributes.insert("service.name".to_string(), service_name.clone());
        Self {
            resource_attributes,
            service_name,
            scope_name: scope_name.into(),
            sums: Vec::new(),
            gauges: Vec::new(),
            histograms: Vec::new(),
            exp_histograms: Vec::new(),
        }
    }

    pub fn with_resource_attribute(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.resource_attributes.insert(key.into(), value.into());
        self
    }

    pub fn clear(&mut self) {
        self.sums.clear();
        self.gauges.clear();
        self.histograms.clear();
        self.exp_histograms.clear();
    }

    pub fn total_rows(&self) -> usize {
        self.sums.len() + self.gauges.len() + self.histograms.len() + self.exp_histograms.len()
    }

    fn push_sum(
        &mut self,
        name: &str,
        description: &str,
        attrs: IndexMap<String, String>,
        value: f64,
        is_monotonic: bool,
        time_unix_nano: u64,
    ) {
        self.sums.push(SumRow {
            ResourceAttributes: self.resource_attributes.clone(),
            ResourceSchemaUrl: String::new(),
            ServiceName: self.service_name.clone(),
            ScopeName: self.scope_name.clone(),
            ScopeVersion: String::new(),
            ScopeAttributes: IndexMap::new(),
            ScopeDroppedAttrCount: 0,
            ScopeSchemaUrl: String::new(),
            MetricName: name.to_string(),
            MetricDescription: description.to_string(),
            MetricUnit: String::new(),
            Attributes: attrs,
            StartTimeUnix: DateTime64::<9>(Tz::UTC, 0),
            TimeUnix: DateTime64::<9>(Tz::UTC, time_unix_nano),
            Value: value,
            Flags: 0,
            AggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
            IsMonotonic: is_monotonic,
        });
    }

    fn push_gauge(
        &mut self,
        name: &str,
        description: &str,
        attrs: IndexMap<String, String>,
        value: f64,
        time_unix_nano: u64,
    ) {
        self.gauges.push(GaugeRow {
            ResourceAttributes: self.resource_attributes.clone(),
            ResourceSchemaUrl: String::new(),
            ServiceName: self.service_name.clone(),
            ScopeName: self.scope_name.clone(),
            ScopeVersion: String::new(),
            ScopeAttributes: IndexMap::new(),
            ScopeDroppedAttrCount: 0,
            ScopeSchemaUrl: String::new(),
            MetricName: name.to_string(),
            MetricDescription: description.to_string(),
            MetricUnit: String::new(),
            Attributes: attrs,
            StartTimeUnix: DateTime64::<9>(Tz::UTC, 0),
            TimeUnix: DateTime64::<9>(Tz::UTC, time_unix_nano),
            Value: value,
            Flags: 0,
        });
    }

    fn push_histogram(
        &mut self,
        name: &str,
        description: &str,
        attrs: IndexMap<String, String>,
        histogram: &Histogram,
        time_unix_nano: u64,
    ) {
        let (bucket_counts, explicit_bounds) =
            cumulative_to_delta_buckets(histogram.buckets_cumulative_iter());
        self.histograms.push(HistogramRow {
            ResourceAttributes: self.resource_attributes.clone(),
            ResourceSchemaUrl: String::new(),
            ServiceName: self.service_name.clone(),
            ScopeName: self.scope_name.clone(),
            ScopeVersion: String::new(),
            ScopeAttributes: IndexMap::new(),
            ScopeDroppedAttrCount: 0,
            ScopeSchemaUrl: String::new(),
            MetricName: name.to_string(),
            MetricDescription: description.to_string(),
            MetricUnit: String::new(),
            Attributes: attrs,
            StartTimeUnix: DateTime64::<9>(Tz::UTC, 0),
            TimeUnix: DateTime64::<9>(Tz::UTC, time_unix_nano),
            Count: histogram.count(),
            Sum: histogram.sum() as f64,
            BucketCounts: bucket_counts,
            ExplicitBounds: explicit_bounds,
            Flags: 0,
            Min: 0.0,
            Max: 0.0,
            AggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        });
    }

    fn push_histogram_parts<I>(
        &mut self,
        name: &str,
        description: &str,
        parts: HistogramParts<I>,
        time_unix_nano: u64,
    ) where
        I: IntoIterator<Item = (u64, u64)>,
    {
        let (bucket_counts, explicit_bounds) =
            cumulative_to_delta_buckets(parts.buckets_cumulative);
        self.histograms.push(HistogramRow {
            ResourceAttributes: self.resource_attributes.clone(),
            ResourceSchemaUrl: String::new(),
            ServiceName: self.service_name.clone(),
            ScopeName: self.scope_name.clone(),
            ScopeVersion: String::new(),
            ScopeAttributes: IndexMap::new(),
            ScopeDroppedAttrCount: 0,
            ScopeSchemaUrl: String::new(),
            MetricName: name.to_string(),
            MetricDescription: description.to_string(),
            MetricUnit: String::new(),
            Attributes: parts.attrs,
            StartTimeUnix: DateTime64::<9>(Tz::UTC, 0),
            TimeUnix: DateTime64::<9>(Tz::UTC, time_unix_nano),
            Count: parts.count,
            Sum: parts.sum as f64,
            BucketCounts: bucket_counts,
            ExplicitBounds: explicit_bounds,
            Flags: 0,
            Min: 0.0,
            Max: 0.0,
            AggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        });
    }

    fn push_distribution(
        &mut self,
        name: &str,
        description: &str,
        attrs: IndexMap<String, String>,
        snap: &ExpBucketsSnapshot,
        time_unix_nano: u64,
    ) {
        let (positive_offset, positive_counts) = compact_positive_buckets(snap);
        self.exp_histograms.push(ExpHistogramRow {
            ResourceAttributes: self.resource_attributes.clone(),
            ResourceSchemaUrl: String::new(),
            ServiceName: self.service_name.clone(),
            ScopeName: self.scope_name.clone(),
            ScopeVersion: String::new(),
            ScopeAttributes: IndexMap::new(),
            ScopeDroppedAttrCount: 0,
            ScopeSchemaUrl: String::new(),
            MetricName: name.to_string(),
            MetricDescription: description.to_string(),
            MetricUnit: String::new(),
            Attributes: attrs,
            StartTimeUnix: DateTime64::<9>(Tz::UTC, 0),
            TimeUnix: DateTime64::<9>(Tz::UTC, time_unix_nano),
            Count: snap.count,
            Sum: snap.sum as f64,
            Scale: 0,
            ZeroCount: snap.zero_count,
            PositiveOffset: positive_offset,
            PositiveBucketCounts: positive_counts,
            NegativeOffset: 0,
            NegativeBucketCounts: Vec::new(),
            Flags: 0,
            Min: snap.min().map(|v| v as f64).unwrap_or(0.0),
            Max: snap.max().map(|v| v as f64).unwrap_or(0.0),
            AggregationTemporality: AGGREGATION_TEMPORALITY_CUMULATIVE,
        });
    }
}

struct HistogramParts<I> {
    attrs: IndexMap<String, String>,
    buckets_cumulative: I,
    sum: u64,
    count: u64,
}

#[allow(non_snake_case)]
#[derive(klickhouse::Row, Debug)]
pub struct SumRow {
    pub ResourceAttributes: IndexMap<String, String>,
    pub ResourceSchemaUrl: String,
    pub ServiceName: String,
    pub ScopeName: String,
    pub ScopeVersion: String,
    pub ScopeAttributes: IndexMap<String, String>,
    pub ScopeDroppedAttrCount: u32,
    pub ScopeSchemaUrl: String,
    pub MetricName: String,
    pub MetricDescription: String,
    pub MetricUnit: String,
    pub Attributes: IndexMap<String, String>,
    pub StartTimeUnix: DateTime64<9>,
    pub TimeUnix: DateTime64<9>,
    pub Value: f64,
    pub Flags: u32,
    pub AggregationTemporality: i32,
    pub IsMonotonic: bool,
}

#[allow(non_snake_case)]
#[derive(klickhouse::Row, Debug)]
pub struct GaugeRow {
    pub ResourceAttributes: IndexMap<String, String>,
    pub ResourceSchemaUrl: String,
    pub ServiceName: String,
    pub ScopeName: String,
    pub ScopeVersion: String,
    pub ScopeAttributes: IndexMap<String, String>,
    pub ScopeDroppedAttrCount: u32,
    pub ScopeSchemaUrl: String,
    pub MetricName: String,
    pub MetricDescription: String,
    pub MetricUnit: String,
    pub Attributes: IndexMap<String, String>,
    pub StartTimeUnix: DateTime64<9>,
    pub TimeUnix: DateTime64<9>,
    pub Value: f64,
    pub Flags: u32,
}

#[allow(non_snake_case)]
#[derive(klickhouse::Row, Debug)]
pub struct HistogramRow {
    pub ResourceAttributes: IndexMap<String, String>,
    pub ResourceSchemaUrl: String,
    pub ServiceName: String,
    pub ScopeName: String,
    pub ScopeVersion: String,
    pub ScopeAttributes: IndexMap<String, String>,
    pub ScopeDroppedAttrCount: u32,
    pub ScopeSchemaUrl: String,
    pub MetricName: String,
    pub MetricDescription: String,
    pub MetricUnit: String,
    pub Attributes: IndexMap<String, String>,
    pub StartTimeUnix: DateTime64<9>,
    pub TimeUnix: DateTime64<9>,
    pub Count: u64,
    pub Sum: f64,
    pub BucketCounts: Vec<u64>,
    pub ExplicitBounds: Vec<f64>,
    pub Flags: u32,
    pub Min: f64,
    pub Max: f64,
    pub AggregationTemporality: i32,
}

#[allow(non_snake_case)]
#[derive(klickhouse::Row, Debug)]
pub struct ExpHistogramRow {
    pub ResourceAttributes: IndexMap<String, String>,
    pub ResourceSchemaUrl: String,
    pub ServiceName: String,
    pub ScopeName: String,
    pub ScopeVersion: String,
    pub ScopeAttributes: IndexMap<String, String>,
    pub ScopeDroppedAttrCount: u32,
    pub ScopeSchemaUrl: String,
    pub MetricName: String,
    pub MetricDescription: String,
    pub MetricUnit: String,
    pub Attributes: IndexMap<String, String>,
    pub StartTimeUnix: DateTime64<9>,
    pub TimeUnix: DateTime64<9>,
    pub Count: u64,
    pub Sum: f64,
    pub Scale: i32,
    pub ZeroCount: u64,
    pub PositiveOffset: i32,
    pub PositiveBucketCounts: Vec<u64>,
    pub NegativeOffset: i32,
    pub NegativeBucketCounts: Vec<u64>,
    pub Flags: u32,
    pub Min: f64,
    pub Max: f64,
    pub AggregationTemporality: i32,
}

fn attrs_from_pairs(pairs: &[(String, String)]) -> IndexMap<String, String> {
    pairs.iter().cloned().collect()
}

fn single_attr(key: &str, value: &str) -> IndexMap<String, String> {
    let mut attrs = IndexMap::new();
    attrs.insert(key.to_string(), value.to_string());
    attrs
}

fn cumulative_to_delta_buckets(
    cumulative: impl IntoIterator<Item = (u64, u64)>,
) -> (Vec<u64>, Vec<f64>) {
    let iter = cumulative.into_iter();
    let (lower, _) = iter.size_hint();
    let mut bucket_counts = Vec::with_capacity(lower);
    let mut explicit_bounds = Vec::with_capacity(lower.saturating_sub(1));
    let mut prev = 0u64;

    for (bound, cumulative_count) in iter {
        bucket_counts.push(cumulative_count.saturating_sub(prev));
        prev = cumulative_count;
        if bound != u64::MAX {
            explicit_bounds.push(bound as f64);
        }
    }

    (bucket_counts, explicit_bounds)
}

fn compact_positive_buckets(snap: &ExpBucketsSnapshot) -> (i32, Vec<u64>) {
    let first = snap.positive.iter().position(|&count| count > 0);
    let last = snap.positive.iter().rposition(|&count| count > 0);
    match (first, last) {
        (Some(first), Some(last)) => (first as i32, snap.positive[first..=last].to_vec()),
        _ => (0, Vec::new()),
    }
}

impl ClickHouseExport for Counter {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_sum(
            name,
            description,
            IndexMap::new(),
            self.sum() as f64,
            false,
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for Gauge {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_gauge(
            name,
            description,
            IndexMap::new(),
            self.get() as f64,
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for GaugeF64 {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_gauge(
            name,
            description,
            IndexMap::new(),
            self.get(),
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for MaxGauge {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_gauge(
            name,
            description,
            IndexMap::new(),
            self.get() as f64,
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for MaxGaugeF64 {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_gauge(
            name,
            description,
            IndexMap::new(),
            self.get(),
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for MinGauge {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_gauge(
            name,
            description,
            IndexMap::new(),
            self.get() as f64,
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for MinGaugeF64 {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_gauge(
            name,
            description,
            IndexMap::new(),
            self.get(),
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for Histogram {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        batch.push_histogram(name, description, IndexMap::new(), self, time_unix_nano);
    }
}

impl ClickHouseExport for Distribution {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let snap = self.buckets_snapshot();
        batch.push_distribution(name, description, IndexMap::new(), &snap, time_unix_nano);
    }
}

impl ClickHouseExport for SampledTimer {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let calls_name = format!("{name}_calls");
        let samples_name = format!("{name}_samples");
        batch.push_sum(
            &calls_name,
            description,
            IndexMap::new(),
            self.calls() as f64,
            false,
            time_unix_nano,
        );
        batch.push_histogram(
            &samples_name,
            description,
            IndexMap::new(),
            self.histogram(),
            time_unix_nano,
        );
    }
}

impl ClickHouseExport for DynamicCounter {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        self.visit_series(|labels, value| {
            batch.push_sum(
                name,
                description,
                attrs_from_pairs(labels),
                value as f64,
                false,
                time_unix_nano,
            );
        });
    }
}

impl ClickHouseExport for DynamicGauge {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        for (labels, value) in self.snapshot() {
            batch.push_gauge(
                name,
                description,
                attrs_from_pairs(labels.pairs()),
                value,
                time_unix_nano,
            );
        }
    }
}

impl ClickHouseExport for DynamicGaugeI64 {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        for (labels, value) in self.snapshot() {
            batch.push_gauge(
                name,
                description,
                attrs_from_pairs(labels.pairs()),
                value as f64,
                time_unix_nano,
            );
        }
    }
}

impl ClickHouseExport for DynamicHistogram {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        self.visit_series(|labels, series| {
            batch.push_histogram_parts(
                name,
                description,
                HistogramParts {
                    attrs: attrs_from_pairs(labels),
                    buckets_cumulative: series.buckets_cumulative_iter(),
                    sum: series.sum(),
                    count: series.count(),
                },
                time_unix_nano,
            );
        });
    }
}

impl ClickHouseExport for DynamicDistribution {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        self.visit_series(|labels, _count, _sum, snap| {
            batch.push_distribution(
                name,
                description,
                attrs_from_pairs(labels),
                &snap,
                time_unix_nano,
            );
        });
    }
}

impl<L: LabelEnum> ClickHouseExport for LabeledCounter<L> {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        for (label, value) in self.iter() {
            batch.push_sum(
                name,
                description,
                single_attr(L::LABEL_NAME, label.variant_name()),
                value as f64,
                false,
                time_unix_nano,
            );
        }
    }
}

impl<L: LabelEnum> ClickHouseExport for LabeledGauge<L> {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        for (label, value) in self.iter() {
            batch.push_gauge(
                name,
                description,
                single_attr(L::LABEL_NAME, label.variant_name()),
                value as f64,
                time_unix_nano,
            );
        }
    }
}

impl<L: LabelEnum> ClickHouseExport for LabeledHistogram<L> {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        for (label, histogram) in self.iter() {
            batch.push_histogram(
                name,
                description,
                single_attr(L::LABEL_NAME, label.variant_name()),
                histogram,
                time_unix_nano,
            );
        }
    }
}

impl<L: LabelEnum> ClickHouseExport for LabeledSampledTimer<L> {
    fn export_clickhouse(
        &self,
        batch: &mut ClickHouseMetricBatch,
        name: &str,
        description: &str,
        time_unix_nano: u64,
    ) {
        let calls_name = format!("{name}_calls");
        let samples_name = format!("{name}_samples");
        for (label, calls, samples) in self.iter() {
            let attrs = single_attr(L::LABEL_NAME, label.variant_name());
            batch.push_sum(
                &calls_name,
                description,
                attrs.clone(),
                calls.sum() as f64,
                false,
                time_unix_nano,
            );
            batch.push_histogram(&samples_name, description, attrs, samples, time_unix_nano);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exports_counter_without_otlp_metric() {
        let counter = Counter::new(4);
        counter.add(42);
        let mut batch = ClickHouseMetricBatch::new("test");
        counter.export_clickhouse(&mut batch, "requests", "request count", 123);

        assert_eq!(batch.total_rows(), 1);
        assert_eq!(batch.sums[0].MetricName, "requests");
        assert_eq!(batch.sums[0].Value, 42.0);
    }

    #[test]
    fn exports_distribution_as_exp_histogram() {
        let distribution = Distribution::new(4);
        distribution.record(0);
        distribution.record(10);
        let mut batch = ClickHouseMetricBatch::new("test");
        distribution.export_clickhouse(&mut batch, "sizes", "", 123);

        assert_eq!(batch.exp_histograms.len(), 1);
        assert_eq!(batch.exp_histograms[0].ZeroCount, 1);
        assert_eq!(batch.exp_histograms[0].Count, 2);
        assert!(!batch.exp_histograms[0].PositiveBucketCounts.is_empty());
    }
}

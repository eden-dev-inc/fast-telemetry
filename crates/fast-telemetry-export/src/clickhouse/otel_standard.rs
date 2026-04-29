//! Built-in exporter targeting the metric table layout used by the
//! [OpenTelemetry Collector ClickHouse exporter].
//!
//! Writes to four tables — `otel_metrics_sum`, `otel_metrics_gauge`,
//! `otel_metrics_histogram`, `otel_metrics_exponential_histogram` — using
//! `Map(LowCardinality(String), String)` for resource and metric attributes
//! so common dashboards / queries / materialized views can use the same column
//! names.
//!
//! The schema includes Collector compatibility columns for scope, schema URL,
//! and exemplars; flat `export_otlp()` metrics populate those with defaults.
//! Summary metrics are not emitted by this exporter.
//!
//! All four tables are inserted into via a single shared client connection
//! per cycle; this is the convenient default for users who don't have a
//! custom schema and want metrics in ClickHouse with minimal configuration.
//!
//! For custom schemas, see the parent module's [`run`](super::run) primitive.
//!
//! [OpenTelemetry Collector ClickHouse exporter]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter

use std::time::Duration;

use fast_telemetry::clickhouse::{
    ClickHouseMetricBatch, ExpHistogramRow, GaugeRow, HistogramRow, SumRow,
};
use fast_telemetry::otlp::pb;
use indexmap::IndexMap;
use klickhouse::{Client, DateTime64, Tz};
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;

use super::{
    ClickHouseConfig, backoff_with_jitter, connect, connect_with_database, qualified_table,
    quote_ident,
};

/// Configuration for the built-in OTel-standard schema exporter.
#[derive(Clone)]
pub struct OtelStandardConfig {
    /// Connection settings (endpoint, credentials, database, interval).
    pub clickhouse: ClickHouseConfig,
    /// `service.name` resource attribute, written into every row.
    pub service_name: String,
    /// Instrumentation scope name (default `fast-telemetry`).
    pub scope_name: String,
    /// Additional resource attributes attached to every row.
    pub resource_attributes: Vec<(String, String)>,
    /// Run `CREATE TABLE IF NOT EXISTS` for each metric table on startup
    /// (default true). Disable if schema is managed externally.
    pub auto_create_tables: bool,
    pub sum_table: String,
    pub gauge_table: String,
    pub histogram_table: String,
    pub exp_histogram_table: String,
}

impl Default for OtelStandardConfig {
    fn default() -> Self {
        Self {
            clickhouse: ClickHouseConfig::default(),
            service_name: "unknown_service".to_string(),
            scope_name: "fast-telemetry".to_string(),
            resource_attributes: Vec::new(),
            auto_create_tables: true,
            sum_table: "otel_metrics_sum".to_string(),
            gauge_table: "otel_metrics_gauge".to_string(),
            histogram_table: "otel_metrics_histogram".to_string(),
            exp_histogram_table: "otel_metrics_exponential_histogram".to_string(),
        }
    }
}

impl OtelStandardConfig {
    pub fn new(endpoint: impl Into<String>, service_name: impl Into<String>) -> Self {
        Self {
            clickhouse: ClickHouseConfig::new(endpoint),
            service_name: service_name.into(),
            ..Default::default()
        }
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.clickhouse = self.clickhouse.with_credentials(username, password);
        self
    }

    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.clickhouse = self.clickhouse.with_database(database);
        self
    }

    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.clickhouse = self.clickhouse.with_interval(interval);
        self
    }

    pub fn with_scope_name(mut self, name: impl Into<String>) -> Self {
        self.scope_name = name.into();
        self
    }

    pub fn with_attribute(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.resource_attributes.push((key.into(), value.into()));
        self
    }

    pub fn with_auto_create_tables(mut self, enabled: bool) -> Self {
        self.auto_create_tables = enabled;
        self
    }
}

// ----------------------------------------------------------------------------
// Schema
// ----------------------------------------------------------------------------

fn sum_table_ddl(db: &str, table: &str) -> String {
    let table_ref = qualified_table(db, table);
    format!(
        "CREATE TABLE IF NOT EXISTS {table_ref} (
            ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ResourceSchemaUrl String CODEC(ZSTD(1)),
            ServiceName LowCardinality(String),
            ScopeName String CODEC(ZSTD(1)),
            ScopeVersion String CODEC(ZSTD(1)),
            ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
            ScopeSchemaUrl String CODEC(ZSTD(1)),
            MetricName String,
            MetricDescription String,
            MetricUnit String CODEC(ZSTD(1)),
            Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
            TimeUnix DateTime64(9),
            Value Float64,
            Flags UInt32 CODEC(ZSTD(1)),
            Exemplars Nested (
                FilteredAttributes Map(LowCardinality(String), String),
                TimeUnix DateTime64(9),
                Value Float64,
                SpanId String,
                TraceId String
            ) CODEC(ZSTD(1)),
            AggregationTemporality Int32,
            IsMonotonic Bool
        ) ENGINE = MergeTree()
        PARTITION BY toDate(TimeUnix)
        ORDER BY (ServiceName, MetricName, TimeUnix)
        TTL toDateTime(TimeUnix) + toIntervalDay(30)
        SETTINGS index_granularity = 8192"
    )
}

fn gauge_table_ddl(db: &str, table: &str) -> String {
    let table_ref = qualified_table(db, table);
    format!(
        "CREATE TABLE IF NOT EXISTS {table_ref} (
            ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ResourceSchemaUrl String CODEC(ZSTD(1)),
            ServiceName LowCardinality(String),
            ScopeName String CODEC(ZSTD(1)),
            ScopeVersion String CODEC(ZSTD(1)),
            ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
            ScopeSchemaUrl String CODEC(ZSTD(1)),
            MetricName String,
            MetricDescription String,
            MetricUnit String CODEC(ZSTD(1)),
            Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
            TimeUnix DateTime64(9),
            Value Float64,
            Flags UInt32 CODEC(ZSTD(1)),
            Exemplars Nested (
                FilteredAttributes Map(LowCardinality(String), String),
                TimeUnix DateTime64(9),
                Value Float64,
                SpanId String,
                TraceId String
            ) CODEC(ZSTD(1))
        ) ENGINE = MergeTree()
        PARTITION BY toDate(TimeUnix)
        ORDER BY (ServiceName, MetricName, TimeUnix)
        TTL toDateTime(TimeUnix) + toIntervalDay(30)
        SETTINGS index_granularity = 8192"
    )
}

fn histogram_table_ddl(db: &str, table: &str) -> String {
    let table_ref = qualified_table(db, table);
    format!(
        "CREATE TABLE IF NOT EXISTS {table_ref} (
            ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ResourceSchemaUrl String CODEC(ZSTD(1)),
            ServiceName LowCardinality(String),
            ScopeName String CODEC(ZSTD(1)),
            ScopeVersion String CODEC(ZSTD(1)),
            ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
            ScopeSchemaUrl String CODEC(ZSTD(1)),
            MetricName String,
            MetricDescription String,
            MetricUnit String CODEC(ZSTD(1)),
            Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
            TimeUnix DateTime64(9),
            Count UInt64,
            Sum Float64,
            BucketCounts Array(UInt64),
            ExplicitBounds Array(Float64),
            Exemplars Nested (
                FilteredAttributes Map(LowCardinality(String), String),
                TimeUnix DateTime64(9),
                Value Float64,
                SpanId String,
                TraceId String
            ) CODEC(ZSTD(1)),
            Flags UInt32 CODEC(ZSTD(1)),
            Min Float64 CODEC(ZSTD(1)),
            Max Float64 CODEC(ZSTD(1)),
            AggregationTemporality Int32
        ) ENGINE = MergeTree()
        PARTITION BY toDate(TimeUnix)
        ORDER BY (ServiceName, MetricName, TimeUnix)
        TTL toDateTime(TimeUnix) + toIntervalDay(30)
        SETTINGS index_granularity = 8192"
    )
}

fn exp_histogram_table_ddl(db: &str, table: &str) -> String {
    let table_ref = qualified_table(db, table);
    format!(
        "CREATE TABLE IF NOT EXISTS {table_ref} (
            ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ResourceSchemaUrl String CODEC(ZSTD(1)),
            ServiceName LowCardinality(String),
            ScopeName String CODEC(ZSTD(1)),
            ScopeVersion String CODEC(ZSTD(1)),
            ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
            ScopeSchemaUrl String CODEC(ZSTD(1)),
            MetricName String,
            MetricDescription String,
            MetricUnit String CODEC(ZSTD(1)),
            Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
            StartTimeUnix DateTime64(9) CODEC(Delta, ZSTD(1)),
            TimeUnix DateTime64(9),
            Count UInt64,
            Sum Float64,
            Scale Int32,
            ZeroCount UInt64,
            PositiveOffset Int32,
            PositiveBucketCounts Array(UInt64),
            NegativeOffset Int32,
            NegativeBucketCounts Array(UInt64),
            Exemplars Nested (
                FilteredAttributes Map(LowCardinality(String), String),
                TimeUnix DateTime64(9),
                Value Float64,
                SpanId String,
                TraceId String
            ) CODEC(ZSTD(1)),
            Flags UInt32 CODEC(ZSTD(1)),
            Min Float64 CODEC(ZSTD(1)),
            Max Float64 CODEC(ZSTD(1)),
            AggregationTemporality Int32
        ) ENGINE = MergeTree()
        PARTITION BY toDate(TimeUnix)
        ORDER BY (ServiceName, MetricName, TimeUnix)
        TTL toDateTime(TimeUnix) + toIntervalDay(30)
        SETTINGS index_granularity = 8192"
    )
}

async fn ensure_schema(client: &Client, config: &OtelStandardConfig) -> klickhouse::Result<()> {
    client
        .execute(format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote_ident(&config.clickhouse.database)
        ))
        .await?;
    client
        .execute(sum_table_ddl(
            &config.clickhouse.database,
            &config.sum_table,
        ))
        .await?;
    client
        .execute(gauge_table_ddl(
            &config.clickhouse.database,
            &config.gauge_table,
        ))
        .await?;
    client
        .execute(histogram_table_ddl(
            &config.clickhouse.database,
            &config.histogram_table,
        ))
        .await?;
    client
        .execute(exp_histogram_table_ddl(
            &config.clickhouse.database,
            &config.exp_histogram_table,
        ))
        .await?;
    Ok(())
}

async fn connect_and_prepare(config: &OtelStandardConfig) -> klickhouse::Result<Client> {
    match connect(&config.clickhouse).await {
        Ok(client) => {
            if config.auto_create_tables {
                ensure_schema(&client, config).await?;
            }
            Ok(client)
        }
        Err(_) if config.auto_create_tables && config.clickhouse.database != "default" => {
            let bootstrap = connect_with_database(&config.clickhouse, "default").await?;
            ensure_schema(&bootstrap, config).await?;
            connect(&config.clickhouse).await
        }
        Err(e) => Err(e),
    }
}

// ----------------------------------------------------------------------------
// pb::Metric → rows translation
// ----------------------------------------------------------------------------

fn attrs_to_map(attrs: &[pb::KeyValue]) -> IndexMap<String, String> {
    let mut map = IndexMap::with_capacity(attrs.len());
    for kv in attrs {
        if let Some(any) = &kv.value
            && let Some(value) = &any.value
        {
            let s = match value {
                pb::any_value::Value::StringValue(s) => s.clone(),
                pb::any_value::Value::IntValue(i) => i.to_string(),
                pb::any_value::Value::DoubleValue(f) => f.to_string(),
                pb::any_value::Value::BoolValue(b) => b.to_string(),
                _ => continue,
            };
            map.insert(kv.key.clone(), s);
        }
    }
    map
}

fn number_value(dp: &pb::NumberDataPoint) -> f64 {
    match dp.value {
        Some(pb::number_data_point::Value::AsInt(i)) => i as f64,
        Some(pb::number_data_point::Value::AsDouble(f)) => f,
        None => 0.0,
    }
}

fn time_unix(time_unix_nano: u64) -> DateTime64<9> {
    DateTime64::<9>(Tz::UTC, time_unix_nano)
}

#[derive(Default)]
struct Batches {
    sums: Vec<SumRow>,
    gauges: Vec<GaugeRow>,
    histograms: Vec<HistogramRow>,
    exp_histograms: Vec<ExpHistogramRow>,
}

impl Batches {
    fn clear(&mut self) {
        self.sums.clear();
        self.gauges.clear();
        self.histograms.clear();
        self.exp_histograms.clear();
    }

    fn is_empty(&self) -> bool {
        self.sums.is_empty()
            && self.gauges.is_empty()
            && self.histograms.is_empty()
            && self.exp_histograms.is_empty()
    }

    fn total_rows(&self) -> usize {
        self.sums.len() + self.gauges.len() + self.histograms.len() + self.exp_histograms.len()
    }
}

fn translate_metrics(
    metrics: &[pb::Metric],
    resource_attrs: &IndexMap<String, String>,
    service_name: &str,
    scope_name: &str,
    out: &mut Batches,
) {
    for metric in metrics {
        let Some(data) = &metric.data else { continue };
        match data {
            pb::metric::Data::Sum(sum) => {
                for dp in &sum.data_points {
                    out.sums.push(SumRow {
                        ResourceAttributes: resource_attrs.clone(),
                        ResourceSchemaUrl: String::new(),
                        ServiceName: service_name.to_string(),
                        ScopeName: scope_name.to_string(),
                        ScopeVersion: String::new(),
                        ScopeAttributes: IndexMap::new(),
                        ScopeDroppedAttrCount: 0,
                        ScopeSchemaUrl: String::new(),
                        MetricName: metric.name.clone(),
                        MetricDescription: metric.description.clone(),
                        MetricUnit: metric.unit.clone(),
                        Attributes: attrs_to_map(&dp.attributes),
                        StartTimeUnix: time_unix(dp.start_time_unix_nano),
                        TimeUnix: time_unix(dp.time_unix_nano),
                        Value: number_value(dp),
                        Flags: dp.flags,
                        AggregationTemporality: sum.aggregation_temporality,
                        IsMonotonic: sum.is_monotonic,
                    });
                }
            }
            pb::metric::Data::Gauge(g) => {
                for dp in &g.data_points {
                    out.gauges.push(GaugeRow {
                        ResourceAttributes: resource_attrs.clone(),
                        ResourceSchemaUrl: String::new(),
                        ServiceName: service_name.to_string(),
                        ScopeName: scope_name.to_string(),
                        ScopeVersion: String::new(),
                        ScopeAttributes: IndexMap::new(),
                        ScopeDroppedAttrCount: 0,
                        ScopeSchemaUrl: String::new(),
                        MetricName: metric.name.clone(),
                        MetricDescription: metric.description.clone(),
                        MetricUnit: metric.unit.clone(),
                        Attributes: attrs_to_map(&dp.attributes),
                        StartTimeUnix: time_unix(dp.start_time_unix_nano),
                        TimeUnix: time_unix(dp.time_unix_nano),
                        Value: number_value(dp),
                        Flags: dp.flags,
                    });
                }
            }
            pb::metric::Data::Histogram(h) => {
                for dp in &h.data_points {
                    out.histograms.push(HistogramRow {
                        ResourceAttributes: resource_attrs.clone(),
                        ResourceSchemaUrl: String::new(),
                        ServiceName: service_name.to_string(),
                        ScopeName: scope_name.to_string(),
                        ScopeVersion: String::new(),
                        ScopeAttributes: IndexMap::new(),
                        ScopeDroppedAttrCount: 0,
                        ScopeSchemaUrl: String::new(),
                        MetricName: metric.name.clone(),
                        MetricDescription: metric.description.clone(),
                        MetricUnit: metric.unit.clone(),
                        Attributes: attrs_to_map(&dp.attributes),
                        StartTimeUnix: time_unix(dp.start_time_unix_nano),
                        TimeUnix: time_unix(dp.time_unix_nano),
                        Count: dp.count,
                        Sum: dp.sum.unwrap_or(0.0),
                        BucketCounts: dp.bucket_counts.clone(),
                        ExplicitBounds: dp.explicit_bounds.clone(),
                        Flags: dp.flags,
                        Min: dp.min.unwrap_or(0.0),
                        Max: dp.max.unwrap_or(0.0),
                        AggregationTemporality: h.aggregation_temporality,
                    });
                }
            }
            pb::metric::Data::ExponentialHistogram(eh) => {
                for dp in &eh.data_points {
                    let (positive_offset, positive_counts) = match &dp.positive {
                        Some(b) => (b.offset, b.bucket_counts.clone()),
                        None => (0, Vec::new()),
                    };
                    let (negative_offset, negative_counts) = match &dp.negative {
                        Some(b) => (b.offset, b.bucket_counts.clone()),
                        None => (0, Vec::new()),
                    };
                    out.exp_histograms.push(ExpHistogramRow {
                        ResourceAttributes: resource_attrs.clone(),
                        ResourceSchemaUrl: String::new(),
                        ServiceName: service_name.to_string(),
                        ScopeName: scope_name.to_string(),
                        ScopeVersion: String::new(),
                        ScopeAttributes: IndexMap::new(),
                        ScopeDroppedAttrCount: 0,
                        ScopeSchemaUrl: String::new(),
                        MetricName: metric.name.clone(),
                        MetricDescription: metric.description.clone(),
                        MetricUnit: metric.unit.clone(),
                        Attributes: attrs_to_map(&dp.attributes),
                        StartTimeUnix: time_unix(dp.start_time_unix_nano),
                        TimeUnix: time_unix(dp.time_unix_nano),
                        Count: dp.count,
                        Sum: dp.sum.unwrap_or(0.0),
                        Scale: dp.scale,
                        ZeroCount: dp.zero_count,
                        PositiveOffset: positive_offset,
                        PositiveBucketCounts: positive_counts,
                        NegativeOffset: negative_offset,
                        NegativeBucketCounts: negative_counts,
                        Flags: dp.flags,
                        Min: dp.min.unwrap_or(0.0),
                        Max: dp.max.unwrap_or(0.0),
                        AggregationTemporality: eh.aggregation_temporality,
                    });
                }
            }
            _ => {}
        }
    }
}

/// Benchmark-only hook for measuring the production OTLP-metric to ClickHouse-row
/// translation path without exposing the internal row types as public API.
#[doc(hidden)]
pub fn benchmark_translate_row_count(metrics: &[pb::Metric]) -> usize {
    let mut resource_attrs = IndexMap::new();
    resource_attrs.insert("service.name".to_string(), "bench".to_string());

    let mut batches = Batches::default();
    translate_metrics(
        metrics,
        &resource_attrs,
        "bench",
        "fast-telemetry",
        &mut batches,
    );
    batches.total_rows()
}

// ----------------------------------------------------------------------------
// Insert
// ----------------------------------------------------------------------------

const SUM_COLUMNS: &str = "ResourceAttributes, ResourceSchemaUrl, ServiceName, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, MetricName, MetricDescription, MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags, AggregationTemporality, IsMonotonic";
const GAUGE_COLUMNS: &str = "ResourceAttributes, ResourceSchemaUrl, ServiceName, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, MetricName, MetricDescription, MetricUnit, Attributes, StartTimeUnix, TimeUnix, Value, Flags";
const HISTOGRAM_COLUMNS: &str = "ResourceAttributes, ResourceSchemaUrl, ServiceName, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, MetricName, MetricDescription, MetricUnit, Attributes, StartTimeUnix, TimeUnix, Count, Sum, BucketCounts, ExplicitBounds, Flags, Min, Max, AggregationTemporality";
const EXP_HISTOGRAM_COLUMNS: &str = "ResourceAttributes, ResourceSchemaUrl, ServiceName, ScopeName, ScopeVersion, ScopeAttributes, ScopeDroppedAttrCount, ScopeSchemaUrl, MetricName, MetricDescription, MetricUnit, Attributes, StartTimeUnix, TimeUnix, Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts, NegativeOffset, NegativeBucketCounts, Flags, Min, Max, AggregationTemporality";

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

async fn insert_batches(
    client: &Client,
    config: &OtelStandardConfig,
    batches: &mut Batches,
) -> klickhouse::Result<usize> {
    let mut total = 0;
    let db = &config.clickhouse.database;

    if !batches.sums.is_empty() {
        let rows = std::mem::take(&mut batches.sums);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({SUM_COLUMNS}) FORMAT native",
            qualified_table(db, &config.sum_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    if !batches.gauges.is_empty() {
        let rows = std::mem::take(&mut batches.gauges);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({GAUGE_COLUMNS}) FORMAT native",
            qualified_table(db, &config.gauge_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    if !batches.histograms.is_empty() {
        let rows = std::mem::take(&mut batches.histograms);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({HISTOGRAM_COLUMNS}) FORMAT native",
            qualified_table(db, &config.histogram_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    if !batches.exp_histograms.is_empty() {
        let rows = std::mem::take(&mut batches.exp_histograms);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({EXP_HISTOGRAM_COLUMNS}) FORMAT native",
            qualified_table(db, &config.exp_histogram_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    Ok(total)
}

async fn insert_metric_batch(
    client: &Client,
    config: &OtelStandardConfig,
    batch: &mut ClickHouseMetricBatch,
) -> klickhouse::Result<usize> {
    let mut total = 0;
    let db = &config.clickhouse.database;

    if !batch.sums.is_empty() {
        let rows = std::mem::take(&mut batch.sums);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({SUM_COLUMNS}) FORMAT native",
            qualified_table(db, &config.sum_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    if !batch.gauges.is_empty() {
        let rows = std::mem::take(&mut batch.gauges);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({GAUGE_COLUMNS}) FORMAT native",
            qualified_table(db, &config.gauge_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    if !batch.histograms.is_empty() {
        let rows = std::mem::take(&mut batch.histograms);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({HISTOGRAM_COLUMNS}) FORMAT native",
            qualified_table(db, &config.histogram_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    if !batch.exp_histograms.is_empty() {
        let rows = std::mem::take(&mut batch.exp_histograms);
        total += rows.len();
        let q = format!(
            "INSERT INTO {} ({EXP_HISTOGRAM_COLUMNS}) FORMAT native",
            qualified_table(db, &config.exp_histogram_table)
        );
        client.insert_native_block(&q, rows).await?;
    }

    Ok(total)
}

// ----------------------------------------------------------------------------
// Run loop
// ----------------------------------------------------------------------------

/// Run the OTel-standard schema export loop.
///
/// `collect_fn` is called each cycle with a `&mut Vec<pb::Metric>` — typically
/// the same `export_otlp(out)` method you'd pass to the OTLP exporter. The
/// exporter handles connection setup, optional schema bootstrap, batched
/// native-protocol inserts to all four tables on a shared connection, and
/// exponential backoff on failures.
///
/// On cancellation, a final export is performed to flush pending metrics.
pub async fn run<F>(config: OtelStandardConfig, cancel: CancellationToken, mut collect_fn: F)
where
    F: FnMut(&mut Vec<pb::Metric>),
{
    log::info!(
        "Starting ClickHouse OTel-standard exporter, endpoint={}, database={}, interval={}s",
        config.clickhouse.endpoint,
        config.clickhouse.database,
        config.clickhouse.interval.as_secs()
    );

    let mut resource_attrs = IndexMap::with_capacity(config.resource_attributes.len() + 1);
    resource_attrs.insert("service.name".to_string(), config.service_name.clone());
    for (k, v) in &config.resource_attributes {
        resource_attrs.insert(k.clone(), v.clone());
    }

    let mut client = match connect_and_prepare(&config).await {
        Ok(c) => c,
        Err(e) => {
            log::error!(
                "Failed to connect to ClickHouse at {} or create ClickHouse metric tables: {e}",
                config.clickhouse.endpoint
            );
            return;
        }
    };

    let mut interval_timer = interval(config.clickhouse.interval);
    interval_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval_timer.tick().await;

    let mut consecutive_failures: u32 = 0;
    let mut metrics_buf: Vec<pb::Metric> = Vec::new();
    let mut batches = Batches::default();

    loop {
        tokio::select! {
            _ = interval_timer.tick() => {}
            _ = cancel.cancelled() => {
                log::info!("ClickHouse OTel-standard exporter shutting down, performing final export");
                let _ = export_once(
                    &client,
                    &config,
                    &resource_attrs,
                    &mut collect_fn,
                    &mut metrics_buf,
                    &mut batches,
                ).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            log::debug!(
                "ClickHouse export backing off {}ms (failures={consecutive_failures})",
                backoff.as_millis()
            );
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = cancel.cancelled() => {
                    let _ = export_once(
                        &client,
                        &config,
                        &resource_attrs,
                        &mut collect_fn,
                        &mut metrics_buf,
                        &mut batches,
                    ).await;
                    return;
                }
            }
        }

        if client.is_closed() {
            match connect(&config.clickhouse).await {
                Ok(c) => {
                    log::info!("Reconnected to ClickHouse");
                    client = c;
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    log::warn!("ClickHouse reconnect failed: {e}");
                    continue;
                }
            }
        }

        metrics_buf.clear();
        collect_fn(&mut metrics_buf);
        if metrics_buf.is_empty() {
            continue;
        }

        batches.clear();
        translate_metrics(
            &metrics_buf,
            &resource_attrs,
            &config.service_name,
            &config.scope_name,
            &mut batches,
        );

        if batches.is_empty() {
            continue;
        }

        let row_count = batches.total_rows();
        match insert_batches(&client, &config, &mut batches).await {
            Ok(_) => {
                consecutive_failures = 0;
                log::debug!("Exported {row_count} rows to ClickHouse");
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                log::warn!("ClickHouse insert failed: {e}");
            }
        }
    }
}

/// Run the first-party ClickHouse row export loop.
///
/// `collect_fn` is called each cycle with a reusable
/// [`ClickHouseMetricBatch`] and shared timestamp. Call derive-generated
/// `export_clickhouse(batch, time_unix_nano)` methods or individual
/// [`fast_telemetry::ClickHouseExport`] impls inside the closure.
///
/// This skips the `pb::Metric` intermediate used by [`run`] and writes the
/// same OTel-standard ClickHouse tables.
pub async fn run_first_party<F>(
    config: OtelStandardConfig,
    cancel: CancellationToken,
    mut collect_fn: F,
) where
    F: FnMut(&mut ClickHouseMetricBatch, u64),
{
    log::info!(
        "Starting first-party ClickHouse OTel-standard exporter, endpoint={}, database={}, interval={}s",
        config.clickhouse.endpoint,
        config.clickhouse.database,
        config.clickhouse.interval.as_secs()
    );

    let mut client = match connect_and_prepare(&config).await {
        Ok(c) => c,
        Err(e) => {
            log::error!(
                "Failed to connect to ClickHouse at {} or create ClickHouse metric tables: {e}",
                config.clickhouse.endpoint
            );
            return;
        }
    };

    let mut batch =
        ClickHouseMetricBatch::with_scope(config.service_name.clone(), config.scope_name.clone());
    for (key, value) in &config.resource_attributes {
        batch = batch.with_resource_attribute(key.clone(), value.clone());
    }

    let mut interval_timer = interval(config.clickhouse.interval);
    interval_timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval_timer.tick().await;

    let mut consecutive_failures: u32 = 0;

    loop {
        tokio::select! {
            _ = interval_timer.tick() => {}
            _ = cancel.cancelled() => {
                log::info!("First-party ClickHouse exporter shutting down, performing final export");
                let _ = export_first_party_once(
                    &client,
                    &config,
                    &mut collect_fn,
                    &mut batch,
                ).await;
                return;
            }
        }

        if consecutive_failures > 0 {
            let backoff = backoff_with_jitter(consecutive_failures);
            log::debug!(
                "ClickHouse export backing off {}ms (failures={consecutive_failures})",
                backoff.as_millis()
            );
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = cancel.cancelled() => {
                    let _ = export_first_party_once(
                        &client,
                        &config,
                        &mut collect_fn,
                        &mut batch,
                    ).await;
                    return;
                }
            }
        }

        if client.is_closed() {
            match connect(&config.clickhouse).await {
                Ok(c) => {
                    log::info!("Reconnected to ClickHouse");
                    client = c;
                }
                Err(e) => {
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    log::warn!("ClickHouse reconnect failed: {e}");
                    continue;
                }
            }
        }

        batch.clear();
        collect_fn(&mut batch, now_nanos());
        if batch.total_rows() == 0 {
            continue;
        }

        let row_count = batch.total_rows();
        match insert_metric_batch(&client, &config, &mut batch).await {
            Ok(_) => {
                consecutive_failures = 0;
                log::debug!("Exported {row_count} rows to ClickHouse");
            }
            Err(e) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                log::warn!("ClickHouse insert failed: {e}");
            }
        }
    }
}

async fn export_once<F>(
    client: &Client,
    config: &OtelStandardConfig,
    resource_attrs: &IndexMap<String, String>,
    collect_fn: &mut F,
    metrics_buf: &mut Vec<pb::Metric>,
    batches: &mut Batches,
) -> klickhouse::Result<()>
where
    F: FnMut(&mut Vec<pb::Metric>),
{
    metrics_buf.clear();
    collect_fn(metrics_buf);
    if metrics_buf.is_empty() {
        return Ok(());
    }

    batches.clear();
    translate_metrics(
        metrics_buf,
        resource_attrs,
        &config.service_name,
        &config.scope_name,
        batches,
    );

    if batches.is_empty() {
        return Ok(());
    }

    if let Err(e) = insert_batches(client, config, batches).await {
        log::warn!("Final ClickHouse insert failed: {e}");
        return Err(e);
    }
    Ok(())
}

async fn export_first_party_once<F>(
    client: &Client,
    config: &OtelStandardConfig,
    collect_fn: &mut F,
    batch: &mut ClickHouseMetricBatch,
) -> klickhouse::Result<()>
where
    F: FnMut(&mut ClickHouseMetricBatch, u64),
{
    batch.clear();
    collect_fn(batch, now_nanos());
    if batch.total_rows() == 0 {
        return Ok(());
    }

    if let Err(e) = insert_metric_batch(client, config, batch).await {
        log::warn!("Final ClickHouse insert failed: {e}");
        return Err(e);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> u64 {
        1_700_000_000_000_000_000
    }

    fn make_kv(k: &str, v: &str) -> pb::KeyValue {
        pb::KeyValue {
            key: k.to_string(),
            value: Some(pb::AnyValue {
                value: Some(pb::any_value::Value::StringValue(v.to_string())),
            }),
        }
    }

    fn resource_for_test() -> IndexMap<String, String> {
        let mut m = IndexMap::new();
        m.insert("service.name".to_string(), "test".to_string());
        m
    }

    #[test]
    fn translates_sum_to_sum_row() {
        let metric = pb::Metric {
            name: "requests_total".to_string(),
            description: "request count".to_string(),
            data: Some(pb::metric::Data::Sum(pb::Sum {
                data_points: vec![pb::NumberDataPoint {
                    attributes: vec![make_kv("route", "/api")],
                    time_unix_nano: ts(),
                    value: Some(pb::number_data_point::Value::AsInt(42)),
                    ..Default::default()
                }],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        };

        let mut batches = Batches::default();
        translate_metrics(
            &[metric],
            &resource_for_test(),
            "test",
            "fast-telemetry",
            &mut batches,
        );

        assert_eq!(batches.sums.len(), 1);
        let row = &batches.sums[0];
        assert_eq!(row.MetricName, "requests_total");
        assert_eq!(row.Value, 42.0);
        assert!(row.IsMonotonic);
        assert_eq!(row.Attributes.get("route"), Some(&"/api".to_string()));
    }

    #[test]
    fn translates_gauge() {
        let metric = pb::Metric {
            name: "cpu".to_string(),
            description: String::new(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![pb::NumberDataPoint {
                    time_unix_nano: ts(),
                    value: Some(pb::number_data_point::Value::AsDouble(0.75)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };

        let mut batches = Batches::default();
        translate_metrics(
            &[metric],
            &resource_for_test(),
            "test",
            "fast-telemetry",
            &mut batches,
        );

        assert_eq!(batches.gauges.len(), 1);
        assert!((batches.gauges[0].Value - 0.75).abs() < 1e-12);
    }

    #[test]
    fn translates_histogram() {
        let metric = pb::Metric {
            name: "lat".to_string(),
            description: String::new(),
            data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                data_points: vec![pb::HistogramDataPoint {
                    time_unix_nano: ts(),
                    count: 5,
                    sum: Some(123.0),
                    bucket_counts: vec![1, 2, 2],
                    explicit_bounds: vec![10.0, 100.0],
                    ..Default::default()
                }],
                aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        };

        let mut batches = Batches::default();
        translate_metrics(
            &[metric],
            &resource_for_test(),
            "test",
            "fast-telemetry",
            &mut batches,
        );

        assert_eq!(batches.histograms.len(), 1);
        let row = &batches.histograms[0];
        assert_eq!(row.Count, 5);
        assert_eq!(row.Sum, 123.0);
        assert_eq!(row.BucketCounts, vec![1, 2, 2]);
        assert_eq!(row.ExplicitBounds, vec![10.0, 100.0]);
    }

    #[test]
    fn translates_exponential_histogram() {
        let metric = pb::Metric {
            name: "dist".to_string(),
            description: String::new(),
            data: Some(pb::metric::Data::ExponentialHistogram(
                pb::OtlpExpHistogram {
                    data_points: vec![pb::ExponentialHistogramDataPoint {
                        time_unix_nano: ts(),
                        count: 3,
                        sum: Some(600.0),
                        scale: 0,
                        zero_count: 0,
                        positive: Some(pb::exponential_histogram_data_point::Buckets {
                            offset: 6,
                            bucket_counts: vec![1, 1, 1],
                        }),
                        negative: Some(pb::exponential_histogram_data_point::Buckets {
                            offset: -3,
                            bucket_counts: vec![2, 4],
                        }),
                        min: Some(-8.0),
                        max: Some(16.0),
                        ..Default::default()
                    }],
                    aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                },
            )),
            ..Default::default()
        };

        let mut batches = Batches::default();
        translate_metrics(
            &[metric],
            &resource_for_test(),
            "test",
            "fast-telemetry",
            &mut batches,
        );

        assert_eq!(batches.exp_histograms.len(), 1);
        let row = &batches.exp_histograms[0];
        assert_eq!(row.Count, 3);
        assert_eq!(row.PositiveOffset, 6);
        assert_eq!(row.PositiveBucketCounts, vec![1, 1, 1]);
        assert_eq!(row.NegativeOffset, -3);
        assert_eq!(row.NegativeBucketCounts, vec![2, 4]);
        assert_eq!(row.Min, -8.0);
        assert_eq!(row.Max, 16.0);
    }

    #[test]
    fn empty_metrics_yield_empty_batches() {
        let mut batches = Batches::default();
        translate_metrics(&[], &resource_for_test(), "test", "scope", &mut batches);
        assert!(batches.is_empty());
    }
}

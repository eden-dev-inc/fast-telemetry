//! Integration tests for the ClickHouse exporter.
//!
//! Each test spins up a real ClickHouse server in a Docker container via
//! [`testcontainers`], runs the exporter against it, and verifies the
//! inserted rows by issuing SELECT queries with a separate client.
//!
//! Requires Docker on the host. Run with:
//!
//! ```sh
//! cargo test -p fast-telemetry-export --features clickhouse \
//!     --no-default-features --test clickhouse_integration
//! ```

#![cfg(feature = "clickhouse")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use fast_telemetry::otlp::pb;
use fast_telemetry::{ClickHouseExport, Counter, Gauge};
use fast_telemetry_export::clickhouse::{ClickHouseConfig, otel_standard, run};
use klickhouse::{Client, ClientOptions, DateTime64, Tz};
use testcontainers_modules::testcontainers::core::wait::HttpWaitStrategy;
use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};
use tokio_util::sync::CancellationToken;

const HTTP_PORT: u16 = 8123;
const NATIVE_PORT: u16 = 9000;

async fn start_clickhouse() -> (ContainerAsync<GenericImage>, String) {
    let image = GenericImage::new("clickhouse/clickhouse-server", "23.3.8.21-alpine")
        .with_exposed_port(ContainerPort::Tcp(HTTP_PORT))
        .with_exposed_port(ContainerPort::Tcp(NATIVE_PORT))
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new("/")
                .with_port(ContainerPort::Tcp(HTTP_PORT))
                .with_expected_status_code(200_u16),
        ));
    let container = image.start().await.expect("start clickhouse container");
    let host = container.get_host().await.expect("container host");
    let port = container
        .get_host_port_ipv4(NATIVE_PORT)
        .await
        .expect("native port mapping");
    (container, format!("{host}:{port}"))
}

async fn verify_client(endpoint: &str) -> Client {
    Client::connect(
        endpoint,
        ClientOptions {
            username: "default".to_string(),
            password: String::new(),
            default_database: "default".to_string(),
            tcp_nodelay: true,
        },
    )
    .await
    .expect("verify client connect")
}

fn ts() -> u64 {
    // Use current wall-clock time so the OTel-standard tables' TTL clause
    // doesn't immediately expire freshly inserted test rows.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time")
        .as_nanos() as u64
}

fn make_kv(k: &str, v: &str) -> pb::KeyValue {
    pb::KeyValue {
        key: k.to_string(),
        value: Some(pb::AnyValue {
            value: Some(pb::any_value::Value::StringValue(v.to_string())),
        }),
    }
}

/// One metric of each kind so we exercise all four OTel-standard tables and
/// every translator branch.
fn sample_metrics() -> Vec<pb::Metric> {
    vec![
        pb::Metric {
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
        },
        pb::Metric {
            name: "cpu_usage".to_string(),
            description: "cpu fraction".to_string(),
            data: Some(pb::metric::Data::Gauge(pb::OtlpGauge {
                data_points: vec![pb::NumberDataPoint {
                    attributes: vec![make_kv("host", "node1")],
                    time_unix_nano: ts(),
                    value: Some(pb::number_data_point::Value::AsDouble(0.75)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        },
        pb::Metric {
            name: "request_latency".to_string(),
            description: "latency histogram".to_string(),
            data: Some(pb::metric::Data::Histogram(pb::OtlpHistogram {
                data_points: vec![pb::HistogramDataPoint {
                    attributes: vec![make_kv("endpoint", "/api")],
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
        },
        pb::Metric {
            name: "response_size".to_string(),
            description: "size distribution".to_string(),
            data: Some(pb::metric::Data::ExponentialHistogram(
                pb::OtlpExpHistogram {
                    data_points: vec![pb::ExponentialHistogramDataPoint {
                        attributes: vec![make_kv("method", "GET")],
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
                            offset: -2,
                            bucket_counts: vec![2, 1],
                        }),
                        min: Some(-32.0),
                        max: Some(256.0),
                        ..Default::default()
                    }],
                    aggregation_temporality: pb::AggregationTemporality::Cumulative as i32,
                },
            )),
            ..Default::default()
        },
    ]
}

#[derive(klickhouse::Row, Debug)]
struct CountRow {
    c: u64,
}

#[derive(klickhouse::Row, Debug)]
#[allow(non_snake_case)]
struct SumValueRow {
    MetricName: String,
    Value: f64,
    IsMonotonic: bool,
}

#[derive(klickhouse::Row, Debug)]
#[allow(non_snake_case)]
struct HistRow {
    Count: u64,
    Sum: f64,
    BucketCounts: Vec<u64>,
    ExplicitBounds: Vec<f64>,
}

#[derive(klickhouse::Row, Debug)]
#[allow(non_snake_case)]
struct ExpHistRow {
    Count: u64,
    Scale: i32,
    PositiveOffset: i32,
    PositiveBucketCounts: Vec<u64>,
    NegativeOffset: i32,
    NegativeBucketCounts: Vec<u64>,
    Min: f64,
    Max: f64,
}

#[tokio::test]
async fn otel_standard_round_trip() {
    let (_container, endpoint) = start_clickhouse().await;

    // Drive the exporter for one cycle's worth of inserts, then cancel.
    let metrics = Arc::new(sample_metrics());
    let consumed = Arc::new(AtomicU32::new(0));
    let cancel = CancellationToken::new();

    let config = otel_standard::OtelStandardConfig::new(&endpoint, "integration-test")
        .with_database("ft_clickhouse_integration")
        .with_interval(Duration::from_millis(50));

    let m = metrics.clone();
    let c = consumed.clone();
    let handle = tokio::spawn(otel_standard::run(config, cancel.clone(), move |out| {
        // Emit metrics on the first call only so row counts are deterministic.
        if c.fetch_add(1, Ordering::SeqCst) == 0 {
            out.extend((*m).clone());
        }
    }));

    // Wait for at least one full cycle (schema bootstrap + insert).
    tokio::time::sleep(Duration::from_millis(800)).await;
    cancel.cancel();
    handle.await.expect("exporter task");

    // Verify with a separate client.
    let client = verify_client(&endpoint).await;

    let sums: SumValueRow = client
        .query_one(
            "SELECT MetricName, Value, IsMonotonic
             FROM ft_clickhouse_integration.otel_metrics_sum
             WHERE ServiceName = 'integration-test'",
        )
        .await
        .expect("query sum row");
    assert_eq!(sums.MetricName, "requests_total");
    assert_eq!(sums.Value, 42.0);
    assert!(sums.IsMonotonic);

    let gauges: CountRow = client
        .query_one(
            "SELECT count() AS c FROM ft_clickhouse_integration.otel_metrics_gauge
             WHERE ServiceName = 'integration-test'",
        )
        .await
        .expect("query gauge count");
    assert!(gauges.c >= 1, "expected at least one gauge row");

    let hist: HistRow = client
        .query_one(
            "SELECT Count, Sum, BucketCounts, ExplicitBounds
             FROM ft_clickhouse_integration.otel_metrics_histogram
             WHERE ServiceName = 'integration-test'",
        )
        .await
        .expect("query histogram row");
    assert_eq!(hist.Count, 5);
    assert_eq!(hist.Sum, 123.0);
    assert_eq!(hist.BucketCounts, vec![1u64, 2, 2]);
    assert_eq!(hist.ExplicitBounds, vec![10.0, 100.0]);

    let exp: ExpHistRow = client
        .query_one(
            "SELECT Count, Scale, PositiveOffset, PositiveBucketCounts,
                    NegativeOffset, NegativeBucketCounts, Min, Max
             FROM ft_clickhouse_integration.otel_metrics_exponential_histogram
             WHERE ServiceName = 'integration-test'",
        )
        .await
        .expect("query exp histogram row");
    assert_eq!(exp.Count, 3);
    assert_eq!(exp.Scale, 0);
    assert_eq!(exp.PositiveOffset, 6);
    assert_eq!(exp.PositiveBucketCounts, vec![1u64, 1, 1]);
    assert_eq!(exp.NegativeOffset, -2);
    assert_eq!(exp.NegativeBucketCounts, vec![2u64, 1]);
    assert_eq!(exp.Min, -32.0);
    assert_eq!(exp.Max, 256.0);
}

#[derive(klickhouse::Row, Debug, Clone)]
#[allow(non_snake_case)]
struct CustomMetricRow {
    MetricName: String,
    MetricKind: String,
    TimeUnix: DateTime64<9>,
    Value: f64,
}

#[tokio::test]
async fn primitive_round_trip_custom_schema() {
    let (_container, endpoint) = start_clickhouse().await;

    // Caller is responsible for the schema in the primitive flow.
    let setup = verify_client(&endpoint).await;
    setup
        .execute(
            "CREATE TABLE default.custom_metrics (
                MetricName String,
                MetricKind LowCardinality(String),
                TimeUnix DateTime64(9),
                Value Float64
            ) ENGINE = MergeTree() ORDER BY (MetricName, TimeUnix)",
        )
        .await
        .expect("create custom_metrics");

    let metrics = Arc::new(sample_metrics());
    let consumed = Arc::new(AtomicU32::new(0));
    let cancel = CancellationToken::new();

    let config = ClickHouseConfig::new(&endpoint).with_interval(Duration::from_millis(50));

    let m = metrics.clone();
    let c = consumed.clone();
    let handle = tokio::spawn(run(
        config,
        "custom_metrics",
        cancel.clone(),
        move |out| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                out.extend((*m).clone());
            }
        },
        |metric: &pb::Metric| -> Vec<CustomMetricRow> {
            // Customer-defined translation: collapse every metric kind into
            // a single flat table with a `MetricKind` discriminator column.
            let mut rows = Vec::new();
            let name = metric.name.clone();
            match &metric.data {
                Some(pb::metric::Data::Sum(s)) => {
                    for dp in &s.data_points {
                        let v = match dp.value {
                            Some(pb::number_data_point::Value::AsInt(i)) => i as f64,
                            Some(pb::number_data_point::Value::AsDouble(f)) => f,
                            _ => 0.0,
                        };
                        rows.push(CustomMetricRow {
                            MetricName: name.clone(),
                            MetricKind: "sum".to_string(),
                            TimeUnix: DateTime64::<9>(Tz::UTC, dp.time_unix_nano),
                            Value: v,
                        });
                    }
                }
                Some(pb::metric::Data::Gauge(g)) => {
                    for dp in &g.data_points {
                        let v = match dp.value {
                            Some(pb::number_data_point::Value::AsInt(i)) => i as f64,
                            Some(pb::number_data_point::Value::AsDouble(f)) => f,
                            _ => 0.0,
                        };
                        rows.push(CustomMetricRow {
                            MetricName: name.clone(),
                            MetricKind: "gauge".to_string(),
                            TimeUnix: DateTime64::<9>(Tz::UTC, dp.time_unix_nano),
                            Value: v,
                        });
                    }
                }
                Some(pb::metric::Data::Histogram(h)) => {
                    for dp in &h.data_points {
                        rows.push(CustomMetricRow {
                            MetricName: name.clone(),
                            MetricKind: "histogram".to_string(),
                            TimeUnix: DateTime64::<9>(Tz::UTC, dp.time_unix_nano),
                            Value: dp.sum.unwrap_or(0.0),
                        });
                    }
                }
                Some(pb::metric::Data::ExponentialHistogram(eh)) => {
                    for dp in &eh.data_points {
                        rows.push(CustomMetricRow {
                            MetricName: name.clone(),
                            MetricKind: "exp_histogram".to_string(),
                            TimeUnix: DateTime64::<9>(Tz::UTC, dp.time_unix_nano),
                            Value: dp.sum.unwrap_or(0.0),
                        });
                    }
                }
                _ => {}
            }
            rows
        },
    ));

    tokio::time::sleep(Duration::from_millis(500)).await;
    cancel.cancel();
    handle.await.expect("primitive exporter task");

    // One row per metric kind from sample_metrics (4 metrics × 1 dp each).
    let count: CountRow = setup
        .query_one("SELECT count() AS c FROM default.custom_metrics")
        .await
        .expect("query count");
    assert_eq!(count.c, 4, "expected one row per sample metric");

    let sum_row: CustomMetricRow = setup
        .query_one(
            "SELECT MetricName, MetricKind, TimeUnix, Value
             FROM default.custom_metrics WHERE MetricKind = 'sum'",
        )
        .await
        .expect("query sum row");
    assert_eq!(sum_row.MetricName, "requests_total");
    assert_eq!(sum_row.Value, 42.0);

    let hist_row: CustomMetricRow = setup
        .query_one(
            "SELECT MetricName, MetricKind, TimeUnix, Value
             FROM default.custom_metrics WHERE MetricKind = 'histogram'",
        )
        .await
        .expect("query histogram row");
    assert_eq!(hist_row.MetricName, "request_latency");
    assert_eq!(hist_row.Value, 123.0);
}

#[tokio::test]
async fn first_party_round_trip() {
    let (_container, endpoint) = start_clickhouse().await;

    let counter = Arc::new(Counter::new(4));
    counter.add(99);
    let gauge = Arc::new(Gauge::new());
    gauge.set(17);

    let consumed = Arc::new(AtomicU32::new(0));
    let cancel = CancellationToken::new();

    let config = otel_standard::OtelStandardConfig::new(&endpoint, "first-party-test")
        .with_database("ft_clickhouse_first_party")
        .with_interval(Duration::from_millis(50));

    let export_counter = counter.clone();
    let export_gauge = gauge.clone();
    let c = consumed.clone();
    let handle = tokio::spawn(otel_standard::run_first_party(
        config,
        cancel.clone(),
        move |batch, ts| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                export_counter.export_clickhouse(batch, "requests_total", "request count", ts);
                export_gauge.export_clickhouse(batch, "queue_depth", "queue depth", ts);
            }
        },
    ));

    tokio::time::sleep(Duration::from_millis(800)).await;
    cancel.cancel();
    handle.await.expect("first-party exporter task");

    let client = verify_client(&endpoint).await;

    let sum: SumValueRow = client
        .query_one(
            "SELECT MetricName, Value, IsMonotonic
             FROM ft_clickhouse_first_party.otel_metrics_sum
             WHERE ServiceName = 'first-party-test'",
        )
        .await
        .expect("query first-party sum row");
    assert_eq!(sum.MetricName, "requests_total");
    assert_eq!(sum.Value, 99.0);

    let gauges: CountRow = client
        .query_one(
            "SELECT count() AS c FROM ft_clickhouse_first_party.otel_metrics_gauge
             WHERE ServiceName = 'first-party-test' AND MetricName = 'queue_depth'",
        )
        .await
        .expect("query first-party gauge count");
    assert_eq!(gauges.c, 1);
}

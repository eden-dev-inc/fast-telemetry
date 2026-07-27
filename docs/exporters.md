# Export Adapters

## Export Adapters (fast-telemetry-export)

For production use, `fast-telemetry-export` provides background export loops with
batching, compression, backoff, and graceful shutdown.

```toml
[dependencies]
fast-telemetry-export = "0.9"
```

### DogStatsD

```rust
use std::sync::Arc;
use fast_telemetry_export::dogstatsd::{DogStatsDConfig, run};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = DogStatsDConfig::new("127.0.0.1:8125");

let metrics = Arc::new(my_metrics);
let mut state = MyMetricsExportState::new();
let tags = vec![("service", "myapp"), ("env", "prod")];

tokio::spawn(run(config, cancel, move | output| {
  metrics.export_dogstatsd_delta(output, & tags, &mut state);
}));
```

`MyMetricsExportState` is the derive-generated per-sink state type from
`#[derive(ExportMetrics)]`. Keep one state value per DogStatsD export loop when
using delta temporality.

### OTLP Metrics

```rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry_export::otlp::{OtlpConfig, run};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = OtlpConfig::new("http://otel-collector:4318")
    .with_service_name("myapp")
    .with_scope_name("proxy")
    .with_attribute("service.version", "1.0")
    .with_header("Authorization", "Bearer <token>")
    .with_timeout(Duration::from_secs(5));

let metrics = Arc::new(my_metrics);

tokio::spawn(run(config, cancel, move | out| {
  metrics.export_otlp(out);
}));
```

The OTLP metrics exporter gzip-compresses larger payloads automatically and
applies exponential backoff on transport failures.

### OTLP Spans

```rust
use std::time::Duration;
use fast_telemetry_export::spans::{SpanExportConfig, spawn_on};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = SpanExportConfig::new("http://otel-collector:4318")
    .with_service_name("myapp")
    .with_scope_name("proxy")
    .with_header("Authorization", "Bearer <token>")
    .with_timeout(Duration::from_secs(5))
    .with_max_batch_size(1024);

let exporter = spawn_on(
    &tokio::runtime::Handle::current(),
    collector,
    config,
    cancel.clone(),
);

// During graceful shutdown:
cancel.cancel();
exporter.await?;
```

Applications without a parent Tokio executor, or those intentionally isolating
telemetry I/O, can use `spans::spawn_standalone(...)` instead.

### Acknowledged OTLP HTTP and Logs

Enable `otlp-logs` when a caller needs to submit OTLP log records directly or
share one transport policy across logs, metrics, and traces:

```toml
[dependencies]
fast-telemetry = { version = "0.9", features = ["otlp-logs"] }
fast-telemetry-export = { version = "0.9", default-features = false, features = ["otlp-logs"] }
```

```rust,no_run
use fast_telemetry::otlp::{build_log_export_request, build_resource, pb};
use fast_telemetry_export::otlp::{OtlpHttpClient, OtlpHttpConfig};

# async fn export() -> Result<(), Box<dyn std::error::Error>> {
let client = OtlpHttpClient::new(
    OtlpHttpConfig::new("https://otel-collector:4318")
        .with_header("Authorization", "Bearer <token>")
        .with_gzip_threshold(1024),
)?;
let resource = build_resource("checkout", &[("service.instance.id", "checkout-1")]);
let request = build_log_export_request(
    &resource,
    "checkout",
    vec![pb::LogRecord {
        body: Some(pb::AnyValue {
            value: Some(pb::any_value::Value::StringValue("ready".to_string())),
        }),
        ..Default::default()
    }],
);
let outcome = client.export_logs(&request).await?;
assert_eq!(outcome.accepted + outcome.rejected, 1);
# Ok(())
# }
```

The same client exposes `export_metrics` and `export_traces`. It validates its
endpoint, headers, and TLS/mTLS material at construction, then performs exactly
one request per call. It never retries or logs internally.

Use the returned classification to drive the caller's policy:

| Result | Caller action |
| --- | --- |
| `Ok` with `rejected == 0` | Advance the caller's checkpoint or mark the batch delivered |
| `Ok` with `rejected > 0` | Inspect `message`; isolate invalid records when ordering must continue |
| `error.is_retryable()` | Retry transport errors, 408, 425, 429, and 5xx; honor `retry_after` |
| `error.is_invalid_payload()` | Split or reject HTTP 400/413 payloads |
| `error.is_terminal()` | Stop and surface configuration, encoding, authentication, other permanent HTTP, or decode failures |

Additional CA bundles and mTLS identities are configured with
`with_ca_certificate_pem` and `with_client_identity_pem`. The client uses
at-least-once request semantics: a lost response can cause a caller to resend a
batch, so stable event identity remains the caller's responsibility.

### ClickHouse (native TCP)

Behind the `clickhouse` feature flag. Three layers are provided:

**First-party OTel-standard rows** — skips OTLP protobuf construction and writes
metrics directly into ClickHouse row batches. Enable the `clickhouse` feature on
`fast-telemetry`, add `#[clickhouse]` to the metrics struct, and use
`run_first_party`:

```rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry::{Counter, ExportMetrics, Gauge};
use fast_telemetry_export::clickhouse::otel_standard::{OtelStandardConfig, run_first_party};
use tokio_util::sync::CancellationToken;

#[derive(ExportMetrics)]
#[metric_prefix = "myapp"]
#[clickhouse]
pub struct AppMetrics {
    pub requests: Counter,
    pub queue_depth: Gauge,
}

let cancel = CancellationToken::new();
let config = OtelStandardConfig::new("clickhouse.internal:9000", "myapp")
    .with_credentials("metrics_writer", "<password>")
    .with_database("telemetry")
    .with_interval(Duration::from_secs(30));

let metrics = Arc::new(my_metrics);

tokio::spawn(run_first_party(config, cancel, move |batch, timestamp| {
    metrics.export_clickhouse(batch, timestamp);
}));
```

**Drop-in OTLP translation** — auto-creates the configured database and four
metric tables (`otel_metrics_sum`, `otel_metrics_gauge`,
`otel_metrics_histogram`, `otel_metrics_exponential_histogram`) compatible with
the [OpenTelemetry Collector ClickHouse exporter] layout, so common metric
queries and dashboards can use the same column names while reusing an existing
`export_otlp()` implementation:

```rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry_export::clickhouse::otel_standard::{OtelStandardConfig, run};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = OtelStandardConfig::new("clickhouse.internal:9000", "myapp")
    .with_credentials("metrics_writer", "<password>")
    .with_database("telemetry")
    .with_interval(Duration::from_secs(30));

let metrics = Arc::new(my_metrics);

tokio::spawn(run(config, cancel, move |out| {
    metrics.export_otlp(out);
}));
```

The built-in exporter writes sum, gauge, histogram, and exponential histogram
metrics. Collector compatibility columns for scope/schema/exemplar data are
created, but flat `export_otlp()` metrics populate them with defaults; summary
metrics are ignored.

**Generic primitive** — for custom schemas. Caller supplies a row type
deriving `klickhouse::Row` and a translator closure that turns each
`pb::Metric` into zero or more rows. The runtime handles connection,
ticking, batched native-protocol inserts, and exponential backoff;
schema and migrations are the caller's problem. Spawn one task per table
for multi-table layouts.

```rust
use fast_telemetry::otlp::pb;
use fast_telemetry_export::clickhouse::{ClickHouseConfig, run};
use klickhouse::{DateTime64, Tz};

#[derive(klickhouse::Row, Debug)]
#[allow(non_snake_case)]
struct MyRow {
    MetricName: String,
    TimeUnix: DateTime64<9>,
    Value: f64,
}

tokio::spawn(run(
    ClickHouseConfig::new("clickhouse.internal:9000").with_database("telemetry"),
    "my_metrics",
    cancel,
    move |out| metrics.export_otlp(out),
    |metric: &pb::Metric| match &metric.data {
        Some(pb::metric::Data::Sum(s)) => s.data_points.iter().map(|dp| MyRow {
            MetricName: metric.name.clone(),
            TimeUnix: DateTime64::<9>(Tz::UTC, dp.time_unix_nano),
            Value: 0.0,
        }).collect(),
        _ => Vec::new(),
    },
));
```

[OpenTelemetry Collector ClickHouse exporter]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter

### Stale Series Sweeper

Bounds memory from dynamic labels by evicting inactive series:

```rust
use std::sync::Arc;
use fast_telemetry::advance_cycle;
use fast_telemetry_export::sweeper::{SweepConfig, run};
use tokio_util::sync::CancellationToken;

let metrics = Arc::new(my_metrics);
let cancel = CancellationToken::new();

tokio::spawn(run(SweepConfig::default (), cancel, move | threshold| {
  advance_cycle();
  metrics.requests_by_endpoint.evict_stale(threshold)
    + metrics.latency_by_endpoint.evict_stale(threshold)
}));
```

If you wrap your metrics in a helper method, call `advance_cycle()` once per
sweep and then sum each dynamic metric's `evict_stale(...)` result.

### Monoio Exporters

`fast-telemetry` recording is runtime-agnostic. If your service runs monoio,
enable the export crate's `monoio` feature for monoio-native loops:

```toml
fast-telemetry-export = { version = "0.8", default-features = false, features = ["dogstatsd", "otlp", "monoio"] }
```

Use `dogstatsd::run_monoio(...)`, `otlp::run_monoio(...)`, and
`sweeper::run_monoio(...)` inside a monoio runtime with timers enabled.
`otlp::run_monoio(...)` currently supports plaintext `http://` collector
endpoints. For spans, run `spans::run_local_flusher_monoio(...)` on each monoio
worker that records spans so thread-local span buffers are published for the
exporter to drain.

## OTLP Protobuf (Manual)

For direct control over OTLP encoding without the export loop.
Add `#[otlp]` to your metrics struct to generate the `export_otlp()` method:

```rust
use fast_telemetry::otlp;

let mut metrics = Vec::new();
my_metrics.export_otlp(&mut metrics, fast_telemetry::otlp::now_nanos());

let resource = otlp::build_resource("myapp", &[("env", "prod")]);
let request = otlp::build_export_request(&resource, "fast-telemetry", metrics);
// Encode and send `request` with your own transport
```

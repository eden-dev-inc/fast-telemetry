# fast-telemetry-export

I/O adapters for the [`fast-telemetry`](https://crates.io/crates/fast-telemetry)
runtime crate.

This crate provides:

- DogStatsD export over UDP
- OTLP metrics export over HTTP/protobuf
- OTLP span export over HTTP/protobuf
- acknowledged OTLP log requests through a reusable HTTP client
- ClickHouse metrics export over the native TCP protocol (via [`klickhouse`])
- stale-series sweeping for dynamic metrics

## Features

| Feature      | Default | Description                                                                          |
| ------------ | ------- | ------------------------------------------------------------------------------------ |
| `dogstatsd`  | ✓       | DogStatsD UDP exporter                                                               |
| `otlp`       | ✓       | OTLP HTTP/protobuf metrics + span exporters                                          |
| `otlp-logs`  |         | OTLP Logs protobuf types and acknowledged `OtlpHttpClient::export_logs`              |
| `clickhouse` |         | Native-TCP ClickHouse exporter — first-party rows, generic primitive, and OTel schema |
| `monoio`     |         | Monoio-native DogStatsD, OTLP HTTP/protobuf, sweeper, and span-flush helper          |
| `compio`     |         | Compio-native DogStatsD and sweeper; span-flush helper with `otlp`                   |
| `logging`    |         | Internal exporter info, warning, and error diagnostics through `eden_logger`         |
| `logging-debug` |      | Adds debug diagnostics; also enables `logging`                                       |

Exporter logging is opt-in so the crate does not write to stderr or bypass the
application's logging setup by default. Enable `logging` to emit internal
diagnostics through `eden_logger`; add `logging-debug` when per-export debug
messages are useful. Runtime filtering follows `eden_logger`'s
`EDEN_LOG_LEVEL` configuration.

Tokio applications should run exporters on their parent-owned executor.
Metric exporters already expose async `run(...)` functions; the span exporter
provides `spans::spawn_on(&tokio::runtime::Handle, ...)` and returns a task that
can be awaited during shutdown. `spans::spawn_standalone(...)` remains
available when a dedicated telemetry thread and private Tokio runtime are an
explicit isolation choice.

The `otlp::OtlpHttpClient` is the shared, non-logging transport used by the
Tokio metric and span loops. With `otlp-logs`, callers can submit an
`ExportLogsServiceRequest` and receive a structured acknowledgement, partial
rejection, retry classification, and `Retry-After` value. The client validates
headers and TLS/mTLS material at construction, emits an OTLP exporter
`User-Agent`, bounds response bodies to 64 KiB by default, and never retries on
its own. `with_max_response_bytes` can lower or raise that response limit.

```rust,no_run
use fast_telemetry::otlp::{build_log_export_request, build_resource, pb};
use fast_telemetry_export::otlp::{OtlpHttpClient, OtlpHttpConfig};

# async fn export() -> Result<(), Box<dyn std::error::Error>> {
let client = OtlpHttpClient::new(
    OtlpHttpConfig::new("https://otel-collector:4318")
        .with_header("Authorization", "Bearer <token>"),
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

`is_retryable`, `is_invalid_payload`, and `is_terminal` form a complete,
non-overlapping policy. Following OTLP/HTTP, only transport failures and status
429, 502, 503, or 504 are retryable; 400/413 identify invalid payloads and all
other HTTP failures are terminal. Partial-success responses are successful
acknowledgements and must not be retried. A caller that retries after a lost
response must tolerate duplicate delivery.

The ClickHouse exporter ships two layers:

- `clickhouse::run<R, F, T>` — generic over a caller-supplied `klickhouse::Row`
  type and a `FnMut(&pb::Metric) -> Vec<R>` translator. Caller owns schema and
  migrations.
- `clickhouse::otel_standard::run_first_party` — writes
  `fast_telemetry::clickhouse::ClickHouseMetricBatch` rows directly, avoiding
  OTLP protobuf construction when the application enables the
  `fast-telemetry/clickhouse` feature and derives `#[clickhouse]`.
- `clickhouse::otel_standard::run` — drop-in OTLP translator writing to four metric
  tables compatible with the [OpenTelemetry Collector ClickHouse exporter] layout
  (`otel_metrics_sum`, `otel_metrics_gauge`, `otel_metrics_histogram`,
  `otel_metrics_exponential_histogram`). Auto-creates the configured database
  and tables on startup.

The OTel-standard exporter currently writes sum, gauge, histogram, and
exponential histogram metrics. It creates the Collector's compatibility columns
for scope/schema/exemplar data, but flat `export_otlp()` metrics populate those
columns with defaults. Summary metrics are ignored.

## Monoio

Enable the `monoio` feature to run exporter loops on a monoio runtime:

```toml
fast-telemetry-export = { version = "0.9", default-features = false, features = ["dogstatsd", "otlp", "monoio"] }
```

The monoio entry points mirror the Tokio ones:

- `dogstatsd::run_monoio(...)`
- `otlp::run_monoio(...)`
- `sweeper::run_monoio(...)`
- `spans::run_local_flusher_monoio(...)`

`otlp::run_monoio(...)` sends OTLP HTTP/protobuf over monoio TCP and currently
supports plaintext `http://` collector endpoints. Keep using the default Tokio
exporter for `https://` endpoints. Monoio timer APIs require a runtime with
timers enabled, and span-heavy applications should run one local flusher task on
each monoio worker that records spans so low-volume thread-local span buffers
are published for the exporter to drain.

## Compio

Enable the `compio` feature to run the DogStatsD UDP exporter and sweeper on a
compio runtime without pulling in Tokio:

```toml
fast-telemetry-export = { version = "0.9", default-features = false, features = ["compio"] }
```

The compio entry points are:

- `dogstatsd::run_compio(...)`
- `sweeper::run_compio(...)`
- `spans::run_local_flusher_compio(...)`, when the `otlp` feature is also enabled

`dogstatsd::run_compio(...)` mirrors `dogstatsd::run(...)`, including
newline-delimited batching and `DogStatsDConfig::max_packet_size` enforcement.
Compio entry points accept any cancellation future with `Output = ()`, so callers
can bridge their own shutdown model without constructing a runtime-local compio
cancel token.
As with the monoio span flusher, run one local flusher task on each compio worker
that records spans so low-volume thread-local span buffers are published for the
OTLP span exporter to drain.

Integration tests covering both layers run against a real ClickHouse via
`testcontainers`:

```sh
cargo test -p fast-telemetry-export --features clickhouse \
    --no-default-features --test clickhouse_integration
```

A `docker compose`-based ClickHouse benchmark/smoke harness lives at
[`crates/fast-telemetry/bench/run-clickhouse.sh`](../fast-telemetry/bench/run-clickhouse.sh)
for ad-hoc local ingest validation, row-count scraping, and repeatable server
setup beside the existing benchmark suite.

Run the export-format comparison with:

```sh
./crates/fast-telemetry/bench/run-clickhouse.sh bench
```

This compares Datadog-compatible DogStatsD text, OTLP build/encode, the current
ClickHouse `export_otlp()` → row translation path, and the first-party
`export_clickhouse()` row builder that skips `pb::Metric`.

See the workspace README at
[`eden-dev-inc/fast-telemetry`](https://github.com/eden-dev-inc/fast-telemetry)
for full examples and integration guidance.

[OpenTelemetry Collector ClickHouse exporter]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter
[`klickhouse`]: https://crates.io/crates/klickhouse

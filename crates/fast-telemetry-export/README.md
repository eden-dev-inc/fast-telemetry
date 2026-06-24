# fast-telemetry-export

I/O adapters for the [`fast-telemetry`](https://crates.io/crates/fast-telemetry)
runtime crate.

This crate provides:

- DogStatsD export over UDP
- OTLP metrics export over HTTP/protobuf
- OTLP span export over HTTP/protobuf
- ClickHouse metrics export over the native TCP protocol (via [`klickhouse`])
- stale-series sweeping for dynamic metrics

## Features

| Feature      | Default | Description                                                                          |
| ------------ | ------- | ------------------------------------------------------------------------------------ |
| `dogstatsd`  | ✓       | DogStatsD UDP exporter                                                               |
| `otlp`       | ✓       | OTLP HTTP/protobuf metrics + span exporters                                          |
| `clickhouse` |         | Native-TCP ClickHouse exporter — first-party rows, generic primitive, and OTel schema |
| `monoio`     |         | Monoio-native DogStatsD, OTLP HTTP/protobuf, sweeper, and span-flush helper          |
| `compio`     |         | Compio-native DogStatsD and sweeper; span-flush helper with `otlp`                   |

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
fast-telemetry-export = { version = "0.6", default-features = false, features = ["dogstatsd", "otlp", "monoio"] }
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
fast-telemetry-export = { version = "0.6", default-features = false, features = ["compio"] }
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

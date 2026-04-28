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

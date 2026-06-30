# API Reference

## API Reference

### Metric Types

| Type                 | Use Case                                 | Hot Path Cost             |
| -------------------- | ---------------------------------------- | ------------------------- |
| `Counter`            | Totals that only go up                   | ~2ns (thread-local write) |
| `Histogram`          | Latency distributions with fixed buckets | ~2ns + bucket lookup      |
| `Distribution`       | Exponential-bucket distributions         | ~2ns + bucket lookup      |
| `Gauge` / `GaugeF64` | Point-in-time values                     | ~2ns (single atomic)      |
| `MaxGauge` / `MinGauge` | Peak/trough of integer observations   | ~2ns (sharded min/max)    |
| `MaxGaugeF64` / `MinGaugeF64` | Peak/trough of float observations | ~2ns (sharded min/max)    |

### Labeled Variants

| Type                               | Label Resolution                    |
| ---------------------------------- | ----------------------------------- |
| `LabeledCounter<L>`                | Compile-time enum, O(1) array index |
| `LabeledHistogram<L>`              | Compile-time enum, O(1) array index |
| `LabeledGauge<L>`                  | Compile-time enum, O(1) array index |
| `DynamicCounter`                   | Runtime labels, HashMap lookup      |
| `DynamicHistogram`                 | Runtime labels, HashMap lookup      |
| `DynamicDistribution`              | Runtime labels, HashMap lookup      |
| `DynamicGauge` / `DynamicGaugeI64` | Runtime labels, HashMap lookup      |

### Derive Macros

| Macro                                      | Purpose                                                                                                                                     |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `#[derive(ExportMetrics)]`                 | Generate `visit_metrics`, `export_prometheus`, `export_dogstatsd`, `export_dogstatsd_delta`, `export_dogstatsd_with_temporality`, and optional `export_otlp` |
| `#[derive(LabelEnum)]` (via `DeriveLabel`) | Generate `LabelEnum` trait impl for enum labels                                                                                             |

### Export Formats

| Format          | Method                                                                                                          | Transport                                       |
| --------------- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Prometheus text | `export_prometheus()`                                                                                           | Serve at `/metrics`                             |
| DogStatsD       | `export_dogstatsd()`, `export_dogstatsd_delta()`, or `export_dogstatsd_with_temporality(..., Temporality, ...)` | UDP via `fast-telemetry-export`                 |
| OTLP protobuf   | `export_otlp()` (requires `#[otlp]` on struct)                                                                  | HTTP via `fast-telemetry-export`                |
| ClickHouse rows | `export_clickhouse()` (requires `#[clickhouse]` on struct) or `ClickHouseExport`; `export_otlp()` fallback also supported | Native TCP via `fast-telemetry-export[clickhouse]` |

## Shard Count

Pass the number of shards to `Counter::new(n)` and other constructors:

- **`std::thread::available_parallelism()`** (or equivalent) for production
- **`4`** for tests
- Must be >= 1, rounded up to power of two internally
- One cache line (128 bytes on x86-64) per shard

## Lineage

The `Counter` implementation originated from
[`JackThomson2/fast-counter`](https://github.com/JackThomson2/fast-counter).
This project has since evolved substantially.

## Scope

fast-telemetry is **metrics and lightweight spans**. It does not cover:

- Structured logging
- Distributed trace backends (ingestion, storage, query)
- Automatic cross-service context propagation
- Alerting or dashboarding

## Further Reading

- [Concurrency Costs](https://travisdowns.github.io/blog/2020/07/06/concurrency-costs.html) — why contended atomics are
  slow
- [crossbeam CachePadded](https://docs.rs/crossbeam-utils/latest/crossbeam_utils/struct.CachePadded.html) — the padding
  we use

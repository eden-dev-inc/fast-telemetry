# Release Notes

## 0.2.0 - 2026-04-29

Published crates: `fast-telemetry`, `fast-telemetry-macros`, `fast-telemetry-export`.

Highlights:

- ClickHouse export: added optional first-party ClickHouse row export support in `fast-telemetry` behind the `clickhouse` feature, including `ClickHouseExport`, `ClickHouseMetricBatch`, and OTel-standard row structs.
- ClickHouse export crate support: `fast-telemetry-export` now ships a native TCP ClickHouse exporter with three paths: custom `klickhouse::Row` schemas, OTel-standard OTLP-to-row translation, and first-party `export_clickhouse()` row batches via `otel_standard::run_first_party`.
- derive macros: `#[derive(ExportMetrics)]` now accepts `#[clickhouse]` and generates `export_clickhouse(...)` methods when the runtime `clickhouse` feature is enabled.
- export performance: histogram and sampled-timer export paths avoid several intermediate allocations. `Histogram::buckets_cumulative_iter()` is a new compatible public API for allocation-free bucket export.
- labeled histograms: `LabeledHistogram::iter()` now yields `(label, &Histogram)`, allowing exporters that only need sum/count to skip building cumulative bucket vectors.
- tooling and docs: added ClickHouse integration tests, a Docker-based ClickHouse smoke/benchmark harness, Criterion export-format comparisons, and updated ClickHouse documentation.

Install:

```toml
[dependencies]
fast-telemetry = "0.2"
fast-telemetry-export = "0.2"
```

## 0.1.2 - 2026-04-28

Republished crates: `fast-telemetry`. (`fast-telemetry-macros` is unchanged since 0.1.1 and stays at that version. `fast-telemetry-export` is unchanged since 0.1.0.)

Highlights:

- export performance: the Prometheus and DogStatsD text exporters now format numeric values via `itoa` (integers) and `ryu` (floats) instead of going through the `core::fmt::Display` formatter machinery. Microbenchmarks show 18% to 45% reductions in format-path time across counter, histogram, and distribution exports. The largest wins are on distribution exports (44% on Prometheus, 42% on DogStatsD).
- floating-point output: `f64` values now use `ryu`'s shortest-roundtrip canonical form. For typical values this matches the previous output. Very large or very small values may now use scientific notation (for example, `1e10` instead of `10000000000`); both forms parse correctly per the Prometheus and DogStatsD specs.
- internal: a `FastFormat` trait is exposed under `__macro_support`. It is not part of the stable public API.

Install:

```toml
[dependencies]
fast-telemetry = "0.1.2"
fast-telemetry-export = "0.1.0"
```

## 0.1.1 - 2026-04-27

Republished crates: `fast-telemetry`, `fast-telemetry-macros`. (`fast-telemetry-export` is unchanged since 0.1.0 and stays at that version; it picks up `fast-telemetry` 0.1.1 via semver.)

Highlights:

- new metric types: `MaxGauge`, `MinGauge`, `MaxGaugeF64`, `MinGaugeF64` for tracking running extrema (peaks/troughs) without a single contended atomic on the hot path
- new metric types: `SampledTimer` and `LabeledSampledTimer` for low-cost elapsed-time measurement, composing a call counter with a stride-sampled latency histogram and an RAII timing guard
- dynamic-metric label lookup now uses a multi-entry per-thread cache, fixing a single-entry cache thrash under rotating label sets
- bug fix: `MinGauge::new()` and `MinGaugeF64::new()` now initialize to `i64::MAX` / `f64::INFINITY`, so any first observation displaces the initial value (previously the 0/0.0 default silently no-oped against positive observations). See #9.
- bench harness: added a CPU workload, a `metrics` + `metrics-util` comparison mode, and a refreshed suite report renderer
- macros: `MetricKind` now covers all extrema gauge types and `SampledTimer`

Install:

```toml
[dependencies]
fast-telemetry = "0.1.1"
fast-telemetry-export = "0.1.0"
```

## 0.1.0 - 2026-04-06

Initial public release of the fast-telemetry workspace on crates.io.

Published crates:

- `fast-telemetry`
- `fast-telemetry-macros`
- `fast-telemetry-export`

Highlights:

- renamed the project from `ophanim` to `fast-telemetry`
- published the runtime, derive macros, and exporter crates to crates.io
- added first-touch dynamic-series, write-plus-export overlap, and span OTLP cycle benchmarks
- expanded README and Rustdoc coverage for dynamic metric eviction, span flushing, manual `traceparent` propagation, and DogStatsD export state
- documented the Criterion benchmark surface and current benchmark-report scope
- added the `eviction` feature flag for stale-series eviction tooling

Install:

```toml
[dependencies]
fast-telemetry = "0.1.0"
fast-telemetry-export = "0.1.0"
```

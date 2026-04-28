# Release Notes

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

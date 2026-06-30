# Release Notes

## 0.7.0 - 2026-06-30

Published crates: `fast-telemetry`, `fast-telemetry-macros`, `fast-telemetry-export`.

Why this is a 0.7.0 bump:

- this release adds bench-only public grouped-counter helpers behind the `bench-tools` feature while evaluating a possible production batching API.
- all workspace crates are aligned at `0.7.0` so downstream services can keep `fast-telemetry` and `fast-telemetry-export` on the same runtime and metric type boundary.

Highlights:

- grouped counter buffering: added a bench-only `CounterSetBuffer` prototype that accumulates related counter deltas locally and flushes shared atomics every configurable number of logical operations.
- individual grouped updates: added indexed `CounterSet::inc(...)` and `CounterSet::add(...)` helpers so grouped counters can still be updated one at a time when only part of the group changes.
- benchmark coverage: added `counter_buffered`, `counter_buffered_indexed`, and `counter_buffered_lookup` harness entities plus `--flush-every` controls to measure direct grouped increments, pre-resolved index updates, per-op name lookup, and an OpenTelemetry Rust multi-counter handle loop separately.

Focused mac harness comparison, averaged across three full matrix runs with verified final counts:

```text
./crates/fast-telemetry/bench/run-counter-batch-bench.sh \
  --threads 16 \
  --runs 7 \
  --target-writes 512000000 \
  --batch-sizes 3,4,5,6 \
  --flush-every 64
```

| Counters | Fast counters | Grouped counters | OTel Rust |
| ---: | ---: | ---: | ---: |
| 3 | 0.92 CPU ns/write | 0.38 CPU ns/write | 244.61 CPU ns/write |
| 4 | 0.78 CPU ns/write | 0.28 CPU ns/write | 256.75 CPU ns/write |
| 5 | 0.75 CPU ns/write | 0.24 CPU ns/write | 190.47 CPU ns/write |
| 6 | 0.73 CPU ns/write | 0.19 CPU ns/write | 254.91 CPU ns/write |

Fast counters use independent fast-telemetry `Counter` handles, grouped counters use the buffered `CounterSet` prototype, and OpenTelemetry uses pre-built Rust `u64_counter` handles. Each row records the same number of counter writes per logical operation and reports the mean of three full matrix summaries. Grouped counters were 58.84% to 73.39% lower than independent fast counters and 643.72x to 1318.50x lower than OpenTelemetry Rust in trimmed CPU ns/write. The OTel rows had higher CPU-time variance (`cpu_ns_per_counter_write_cv_pct`: 13.19% to 27.79% averaged across the matrix summaries), but the gap remained multiple orders of magnitude in the trimmed results.

The name-lookup control confirms that registry lookup should stay out of the hot path. With 128 registered metric names and 6 active counters per operation, direct buffered updates measured 0.19 CPU ns/write, pre-resolved indexed updates measured 0.82 CPU ns/write, and per-op `BTreeMap` name lookup measured 30.15 CPU ns/write.

Install:

```toml
[dependencies]
fast-telemetry = "0.7"
fast-telemetry-export = "0.7"
```

## 0.5.1 - 2026-05-26

Published crates: `fast-telemetry`, `fast-telemetry-export`. (`fast-telemetry-macros` is unchanged since 0.5.0.)

Highlights:

- dynamic metric internals: runtime-labeled metrics keep deterministic `BTreeMap` indexes while using a larger per-thread series cache, cache-entry reuse, and a hybrid label canonicalization path that keeps small label sets on a sorted vector and falls back to `BTreeMap` for larger sets.
- dynamic counter export overlap: `DynamicCounter::sum_all()` now sums series values directly from shards instead of cloning a full dynamic-label snapshot before aggregation.
- export internals: DogStatsD distribution export now walks exponential bucket snapshots directly instead of allocating an intermediate sample iterator, and dynamic DogStatsD tag rendering avoids extra branch work for each label.
- benchmark coverage: added microbenchmark cases for dynamic label cardinality/canonicalization and ClickHouse dynamic gauge export at multiple cardinalities; release-note percentages below are from the script-driven production harness rather than Criterion.

Harness comparison against the 0.5.0 baseline on the same machine. Both sides used the same corrected harness and all runs reported `verified=true`:

```text
./crates/fast-telemetry/bench/run-cache-bench.sh \
  --threads 8 \
  --iters 50000000 \
  --runs 9 \
  --modes fast \
  --entity dynamic_counter \
  --labels 256 \
  --profile hotspot \
  --export-interval-ms 1
```

| Harness Metric | 0.5.0 | 0.5.1 | Change |
| --- | ---: | ---: | ---: |
| `trimmed_export_avg_ms` | 0.043257 ms | 0.006025 ms | 86.1% lower |
| `trimmed_cpu_ns_per_op` | 1.18 ns | 1.20 ns | 1.7% higher, within harness noise |
| `trimmed_total_ops_per_sec` | 6.396B/s | 6.163B/s | 3.6% lower, wall-time informational |

The table uses the low-CV hotspot run (`cpu_ns_per_op_cv_pct`: 3.60% on 0.5.0, 3.50% on 0.5.1); exploratory profiles remain benchmark artifacts rather than release-note claims.

Install:

```toml
[dependencies]
fast-telemetry = "0.5.1"
fast-telemetry-export = "0.5.1"
```

## 0.5.0 - 2026-05-25

Published crates: `fast-telemetry`, `fast-telemetry-macros`, `fast-telemetry-export`.

Why this is a 0.5.0 bump:

- this release adds a new optional `compio` exporter feature and new public exporter entry points in `fast-telemetry-export`.
- because these crates are still pre-1.0, new public API should advance the minor version so downstream users have a clear compatibility boundary.
- all workspace crates are aligned at `0.5.0` to keep the runtime, derive macros, and exporter adapters on the same API contract and avoid dependency skew.

Highlights:

- compio export support: added `dogstatsd::run_compio(...)`, `sweeper::run_compio(...)`, and `spans::run_local_flusher_compio(...)` behind the optional `compio` feature.
- DogStatsD compio transport: `dogstatsd::run_compio(...)` mirrors the Tokio DogStatsD exporter, including newline-delimited batching and `DogStatsDConfig::max_packet_size` enforcement.
- span flushing: added a compio local flusher helper so low-volume spans buffered in thread-local collectors can be published for the OTLP span exporter to drain.
- release metadata: raised the workspace Rust version to `1.90.0` and stabilized the published compio dependency on `compio` `0.18`.

Install:

```toml
[dependencies]
fast-telemetry = "0.5"
fast-telemetry-export = "0.5"
```

## 0.4.0 - 2026-05-16

Published crates: `fast-telemetry`, `fast-telemetry-macros`, `fast-telemetry-export`.

Why this is a 0.4.0 bump:

- this release adds a new optional `monoio` exporter feature and new public exporter entry points in `fast-telemetry-export`.
- because these crates are still pre-1.0, new public API should advance the minor version so downstream users have a clear compatibility boundary.
- all workspace crates are aligned at `0.4.0` to keep the runtime, derive macros, and exporter adapters on the same API contract and avoid dependency skew.

Highlights:

- monoio export support: added `dogstatsd::run_monoio(...)`, `otlp::run_monoio(...)`, `sweeper::run_monoio(...)`, and `spans::run_local_flusher_monoio(...)` behind the optional `monoio` feature.
- OTLP monoio transport: `otlp::run_monoio(...)` sends plaintext `http://` OTLP HTTP/protobuf over monoio TCP for monoio-native services. Keep using the default Tokio exporter for `https://` endpoints.
- span flushing: added a per-worker monoio flusher helper so low-volume spans buffered in thread-local collectors are published for the exporter to drain.
- benchmark validation: compared the branch against `main` with Criterion and the quick telemetry harness; no serious hot-path regressions were observed.

Install:

```toml
[dependencies]
fast-telemetry = "0.4"
fast-telemetry-export = "0.4"
```

## 0.3.0 - 2026-05-07

Published crates: `fast-telemetry`, `fast-telemetry-macros`, `fast-telemetry-export`.

Why this is a 0.3.0 bump:

- this release adds a new public visitor/exporter API surface in `fast-telemetry` and new generated methods from `#[derive(ExportMetrics)]` in `fast-telemetry-macros`.
- because these crates are still pre-1.0, new public API should advance the minor version so downstream users have a clear compatibility boundary.
- all workspace crates are aligned at `0.3.0` to keep the runtime, derive macros, and exporter adapters on the same API contract and avoid dependency skew.

Highlights:

- generic exporter API: added `MetricVisitor`, `MetricMeta`, `MetricKind`, `MetricLabels`, `HistogramSnapshot`, and `DistributionSnapshot` so custom exporters can consume metrics without parsing Prometheus or DogStatsD text output.
- derive macros: `#[derive(ExportMetrics)]` now generates `visit_metrics(...)`, covering scalar metrics, labeled metrics, histograms, distributions, sampled timers, dynamic metrics, and nested metric structs.
- dynamic metrics: visitor traversal exposes canonical borrowed labels and emits overflow notifications for evicted or dropped dynamic series.
- documentation and tests: added visitor API examples and coverage for the generated visitor branches.

Install:

```toml
[dependencies]
fast-telemetry = "0.3"
fast-telemetry-export = "0.3"
```

## 0.2.1 - 2026-04-29

Published crates: `fast-telemetry`. (`fast-telemetry-macros` and `fast-telemetry-export` are unchanged since 0.2.0.)

Highlights:

- distribution export performance: Prometheus summary export now uses `Distribution::sum_and_count()` to read each shard once instead of separately walking shards for sum and count.
- public API: added `Distribution::sum_and_count()` for callers that need both aggregate values without two shard scans.
- span performance: replaced the thread-local span submit-path `RefCell` borrow check with an `UnsafeCell`-backed implementation, preserving the zero-atomic hot path while avoiding per-submit runtime borrow overhead.

Install:

```toml
[dependencies]
fast-telemetry = "0.2.1"
fast-telemetry-export = "0.2"
```

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

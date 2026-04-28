# Bench Harness

Script-driven contention workloads for benchmarking fast-telemetry against the
OpenTelemetry SDK under realistic multi-threaded load.

See [BENCHMARK_REPORT.md](BENCHMARK_REPORT.md) for detailed results and
root cause analysis.

## Why Not `benches/`?

These workloads are intentionally separate from Criterion microbenchmarks:

- They are end-to-end workload drivers, not `cargo bench` functions.
- They require orchestration beyond Criterion (CPU pinning, `perf`, optional DogStatsD collector integration).
- They produce machine-readable run artifacts for comparison.

Criterion microbenchmarks live in [`../benches/`](../benches/) and are useful for
single-operation latency measurement. The harness here measures sustained
throughput and contention behavior under realistic thread counts.

## Criterion Coverage

Run the microbenchmark suite directly with:

```bash
cargo bench -p fast-telemetry --features otlp --bench fast_vs_otel
```

The current Criterion suite covers:

- single-threaded recording hot paths
- multi-threaded contention for counters, histograms, and distributions
- export cost for Prometheus, DogStatsD, and OTLP
- dynamic-cardinality export scaling
- first-touch dynamic series insertion and overflow-bucket pressure
- concurrent write-plus-export overlap
- span OTLP drain/build/encode/gzip cost

Useful benchmark groups to inspect in the Criterion HTML report:

- `record/dynamic_counter_first_touch/...`
- `contention/counter_write_export_overlap/...`
- `export/span_otlp_cycle/...`

## CPU Usage

The script-driven workloads now emit process CPU accounting on every run:

- `cpu_user_seconds`
- `cpu_system_seconds`
- `cpu_total_seconds`
- `cpu_avg_cores`
- `cpu_utilization_pct`
- `cpu_ns_per_op`

`cpu_utilization_pct` is computed as `cpu_total_seconds / total_seconds * 100`, so
multi-threaded runs can exceed `100%`. `cpu_avg_cores` is the same number expressed
as average cores consumed during the run.

Example:

```bash
./bench/run-cache-bench.sh --entity counter --profile hotspot --threads 16 --runs 5
```

Each `*-run-*.txt` file and the generated `summary.csv` will include the CPU fields,
so you can compare throughput and CPU cost together on macOS or Linux without
requiring `perf`.

## metrics-rs Comparison

`run-cache-bench.sh` also supports `mode=metrics`, backed by the
[`metrics`](https://docs.rs/metrics/latest/metrics/) facade and
[`metrics-util`](https://docs.rs/metrics-util/latest/metrics_util/) `Registry<_, AtomicStorage>`.

This comparison is available for entities with direct equivalents in the
`metrics` ecosystem:

- `counter`
- `dynamic_counter`
- `dynamic_gauge`
- `dynamic_gauge_i64` (recorded via `f64` gauges)
- `dynamic_histogram`
- `labeled_counter`
- `labeled_gauge`
- `labeled_histogram`

`distribution` and `dynamic_distribution` are excluded because `metrics-rs`
does not expose a matching distribution primitive.

## Entry Points

- `run-cache-bench.sh` -- cache-line contention workloads across metric entities and label access profiles
- `run-span-bench.sh` -- span creation/export contention workloads across realism scenarios
- `run-bench-matrix.sh` -- multi-case sweep runner for publishing-ready comparison sets
- `run-bench-suite.sh` -- full suite with HTML report generation

## Quick Start

```bash
# Quick matrix to check broad behavior
./bench/run-bench-matrix.sh --preset quick --threads 16 --runs 5

# Focused cache workload with perf counters
./bench/run-cache-bench.sh --entity counter --profile hotspot --threads 16 --runs 5 --perf

# Compare fast-telemetry, metrics-rs, and OpenTelemetry on the same cache workload
./bench/run-cache-bench.sh --entity counter --profile hotspot --threads 16 --runs 5 --modes fast,metrics,otel

# All span scenarios
./bench/run-span-bench.sh --scenario all --threads 16 --runs 5

# Full suite with HTML report
./bench/run-bench-suite.sh --preset quick --threads 16 --runs 5
```

## Flags

| Flag                       | Description                                                      |
|----------------------------|------------------------------------------------------------------|
| `--perf`                   | Enable both `perf stat` and `perf record`                        |
| `--perf-stat`              | Hardware counters only                                           |
| `--perf-record`            | Capture `perf.data` + text report                                |
| `--perf-freq <N>`          | `perf record` sample frequency (default: 99)                     |
| `--export-interval-ms <N>` | Exporter thread period (default: 10)                             |
| `--modes <list>`           | Select modes (default: `fast,otel`; options: `fast,otel,atomic,metrics`) |
| `--entity <name>`          | Metric entity to benchmark                                       |
| `--labels <N>`             | Label cardinality (default: 16, max: 256)                        |
| `--profile <name>`         | Label access pattern: `uniform`, `hotspot`, `churn`              |
| `--pin`                    | CPU pinning via `taskset` + round-robin thread affinity          |
| `--cpu-list <list>`        | Explicit CPU list (e.g., `0-15`)                                 |
| `--validate-export`        | Run DogStatsD parsing + parity tests before benchmarking         |
| `--collector`              | Start local DogStatsD collector, emit summary metrics            |

## Entities

`counter`, `distribution`, `dynamic_counter`, `dynamic_distribution`,
`dynamic_gauge`, `dynamic_gauge_i64`, `dynamic_histogram`, `labeled_counter`,
`labeled_gauge`, `labeled_histogram`

## Label Access Profiles

- **uniform** -- spread writes across all label series
- **hotspot** -- concentrate writes into a small hot subset (cache-line pressure)
- **churn** -- pseudo-randomized label access to stress series switching

## DogStatsD Collector

For end-to-end UDP ingest validation:

```bash
./bench/run-dogstatsd-collector.sh up      # start
./bench/run-dogstatsd-collector.sh smoke   # smoke test
./bench/run-dogstatsd-collector.sh scrape  # Prometheus scrape
./bench/run-dogstatsd-collector.sh down    # stop
```

Runs `statsd-exporter` with DogStatsD tag parsing on UDP `:8125`,
Prometheus scrape at `http://127.0.0.1:9102/metrics`.

## ClickHouse Server

For repeatable ClickHouse setup and end-to-end native-protocol ingest validation
against the
[`fast-telemetry-export`](../../fast-telemetry-export) `clickhouse` feature:

```bash
./bench/run-clickhouse.sh up      # start
./bench/run-clickhouse.sh smoke   # HTTP ping + round-trip insert/select
./bench/run-clickhouse.sh bench   # export-format benchmark: DogStatsD vs OTLP vs ClickHouse rows
./bench/run-clickhouse.sh scrape  # row counts from otel_metrics_* tables
./bench/run-clickhouse.sh down    # stop
```

Runs `clickhouse/clickhouse-server` with the native TCP protocol on `:9000`
and the HTTP interface on `:8123` (used by `smoke`/`scrape` for ad-hoc
queries; the exporter itself talks native TCP).

The `bench` command runs:

```bash
cargo bench -p fast-telemetry-export --features clickhouse --bench clickhouse_export
```

It compares local export-format costs for Datadog-compatible DogStatsD text,
OTLP protobuf build/encode, the current ClickHouse `export_otlp()` → row
translation path, and the first-party `export_clickhouse()` ClickHouse row builder. It
does not require the ClickHouse server because it isolates serialization and row
construction from network insert latency.

Use `up` to pin the server environment for real ingest tests, run the ClickHouse
exporter workload or integration test you are measuring, then use `scrape` to
collect table row counts. The `smoke` command is a correctness check, not a
throughput benchmark.

The `otel_standard` exporter creates its tables (`otel_metrics_sum`,
`otel_metrics_gauge`, `otel_metrics_histogram`,
`otel_metrics_exponential_histogram`) on first export, so `scrape` returns
empty / "table missing" until you've run a workload that exports through
the ClickHouse adapter.

Integration tests (which spin up a fresh container per run via
`testcontainers`) are independent of this harness:

```bash
cargo test -p fast-telemetry-export --features clickhouse \
    --no-default-features --test clickhouse_integration
```

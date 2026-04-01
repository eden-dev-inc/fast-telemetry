# Bench Harness

Script-driven contention workloads for benchmarking ophanim against the
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
| `--modes <list>`           | Select modes (default: `fast,otel`; options: `fast,otel,atomic`) |
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

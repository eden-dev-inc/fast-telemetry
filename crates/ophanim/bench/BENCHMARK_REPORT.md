# Benchmark Results

**Machine:** Linux aarch64, NVIDIA Spark, 20 cores, 2026-04-01
**OpenTelemetry SDK:** 0.31
**Methodology:** Criterion microbenchmarks, `cargo bench -p ophanim --features otlp --bench fast_vs_otel`

Run these yourself: `cargo bench -p ophanim --features otlp --bench fast_vs_otel`

For multi-threaded harness workloads, see the [bench README](README.md).

---

## Single-Threaded Recording (ns/iter)

| Metric                          | ophanim | OTel SDK | Speedup |
|---------------------------------|---------|----------|---------|
| Counter `inc()`                 | 3.6     | 7.7      | 2.1x    |
| Gauge `set()` i64               | 0.5     | 7.2      | 14x     |
| Gauge `set()` f64               | 0.5     | 5.7      | 11x     |
| Histogram `record()`            | 6.0     | 32       | 5.3x    |
| Distribution `record()`         | 10.7    | --       | --      |
| Labeled Counter (16 labels)     | 3.8     | 83       | 22x     |
| Labeled Histogram               | 11.7    | 63       | 5.4x    |
| Dynamic Counter (series handle) | 3.8     | 65       | 17x     |
| Dynamic Counter (label lookup)  | 135     | 65       | 0.48x   |

## Multi-Threaded Contention (100K ops per thread, total wall time)

**Counter:**

| Threads | ophanim | OTel SDK | Speedup |
|---------|---------|----------|---------|
| 2       | 115 us  | 296 us   | 2.6x    |
| 4       | 179 us  | 604 us   | 3.4x    |
| 8       | 317 us  | 1,158 us | 3.7x    |

**Histogram:**

| Threads | ophanim | OTel SDK  | Speedup |
|---------|---------|-----------|---------|
| 2       | 162 us  | 1,596 us  | 9.9x    |
| 4       | 230 us  | 5,437 us  | 23.6x   |
| 8       | 355 us  | 14,438 us | 40.7x   |

**Distribution** (no OTel equivalent):

| Threads | ophanim |
|---------|---------|
| 2       | 159 us  |
| 4       | 225 us  |
| 8       | 332 us  |

## Export Cost (single metric, ns/iter)

| Format            | Counter | Histogram | Distribution |
|-------------------|---------|-----------|--------------|
| DogStatsD         | 14      | 23        | 600          |
| Prometheus        | 13      | 259       | 26           |
| OTLP build        | 84      | 98        | 167          |
| OTLP build+encode | 284     | 317       | 845          |

Full OTLP cycle (5 counters + 5 gauges + 3 histograms + 2 dynamic counters):
build 5.4 us, build+encode 9.1 us.

## Dynamic Cardinality Scaling (export cost)

**Counter:**

| Series | OTLP    | DogStatsD |
|--------|---------|-----------|
| 10     | 1.2 us  | 177 ns    |
| 50     | 3.6 us  | 711 ns    |
| 200    | 23.1 us | 3.0 us    |

**Histogram:**

| Series | OTLP    | DogStatsD |
|--------|---------|-----------|
| 10     | 1.5 us  | 408 ns    |
| 50     | 20.7 us | 1.8 us    |
| 200    | 101 us  | 7.2 us    |

**Distribution:**

| Series | OTLP    | DogStatsD |
|--------|---------|-----------|
| 10     | 3.5 us  | 7.5 us    |
| 50     | 18.2 us | 19.8 us   |
| 200    | 72.5 us | 79.2 us   |

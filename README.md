# fast-telemetry

High-performance, cache-friendly telemetry for Rust.

`fast-telemetry` provides thread-sharded counters, gauges, histograms,
distributions, lightweight spans, derive macros, and production export adapters
for Prometheus, DogStatsD, OTLP, ClickHouse, and custom in-process visitors.

## Crates

| Crate | Description |
| --- | --- |
| [`fast-telemetry`](crates/fast-telemetry) | Sharded metrics, spans, and format serialization |
| [`fast-telemetry-macros`](crates/fast-telemetry-macros) | `ExportMetrics` and `LabelEnum` derive macros |
| [`fast-telemetry-export`](crates/fast-telemetry-export) | DogStatsD, OTLP, ClickHouse, span export, and stale-series sweeping |

## Install

```toml
[dependencies]
fast-telemetry = "0.7"
fast-telemetry-export = "0.7"
```

Enable the shared runtime when a parent service should own telemetry and pass it
to child crates:

```toml
fast-telemetry = { version = "0.7", features = ["runtime"] }
```

## Why

`fast-telemetry` exists for hot paths where telemetry records millions of events
per second and shared atomic counters become a bottleneck. Counters are sharded
across cache-line-padded cells so the common write path avoids cross-core cache
line bouncing; exports aggregate shards later, which is fine because export is
infrequent relative to increments.

Representative counter costs from the focused 6-counter harness row:

| Operation | Cost |
| --- | ---: |
| Grouped buffered fast-telemetry counter | 0.19 CPU ns/incr, ~0.88 estimated cycles/incr |
| Independent fast-telemetry counter | 0.73 CPU ns/incr, ~3.37 estimated cycles/incr |
| OpenTelemetry Rust `u64_counter` | 254.91 CPU ns/incr, ~1175.14 estimated cycles/incr |

The cycle values above are estimated from the mac CPU-time result. Linux
`--perf-stat` benchmark runs record measured `*_cycles_per_write` fields.

Grouped buffered counters use the production `CounterSet` and
`CounterSetBuffer` APIs. Use them when one hot-path operation updates a known
group of related counters, and resolve counter indexes once during construction
so the hot path stays on direct indexed updates.

For runtime labels, `DynamicCounterSet` applies the same grouped-counter pattern
to dynamic series: resolve the label-keyed series and counter indexes once, then
record related counters in one grouped call.

## Quick Start

```rust
use fast_telemetry::{Counter, ExportMetrics, Gauge, Histogram};

#[derive(ExportMetrics)]
#[metric_prefix = "myapp"]
pub struct AppMetrics {
    #[help = "Total requests processed"]
    pub requests: Counter,
    #[help = "Request latency in microseconds"]
    pub latency: Histogram,
    #[help = "Current queue depth"]
    pub queue_depth: Gauge,
}

impl AppMetrics {
    pub fn new() -> Self {
        Self {
            requests: Counter::new(4),
            latency: Histogram::with_latency_buckets(4),
            queue_depth: Gauge::new(),
        }
    }
}

let metrics = AppMetrics::new();
metrics.requests.inc();
metrics.latency.record(240);
metrics.queue_depth.set(12);
```

## Documentation

- [Performance and use cases](docs/performance.md)
- [Getting started](docs/getting-started.md)
- [Shared runtime guide](docs/runtime.md)
- [Metrics and spans](docs/metrics-and-spans.md)
- [Export adapters](docs/exporters.md)
- [API reference](docs/reference.md)
- [Benchmark harness](crates/fast-telemetry/bench/README.md)
- [Release notes](RELEASE_NOTES.md)

## When To Use This

Use `fast-telemetry` when metrics are on a high-throughput path, you have
profiled telemetry contention, or you need per-request, per-endpoint, or
per-tenant counters without making instrumentation the bottleneck.

Use the broader OpenTelemetry ecosystem directly when API standardization,
automatic context propagation, SDK-managed pipelines, or low-frequency metrics
matter more than hot-path recording cost.

## Lineage

The `Counter` implementation originated from
[`JackThomson2/fast-counter`](https://github.com/JackThomson2/fast-counter).

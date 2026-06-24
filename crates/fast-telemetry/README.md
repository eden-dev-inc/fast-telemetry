# fast-telemetry

High-performance, cache-friendly telemetry for Rust.

Thread-sharded counters, gauges, histograms, distributions, and lightweight
spans with Prometheus, DogStatsD, OTLP, and optional first-party ClickHouse row
export.

`#[derive(ExportMetrics)]` also generates `visit_metrics`, a structured
in-process export path for custom `MetricVisitor` implementations that need
typed cumulative observations instead of a wire-format string or protobuf.

Enable the `runtime` feature when a parent crate should own a shared telemetry
registry and child crates should keep hot paths on direct metric handles.

See the [workspace README](../../README.md) for full documentation, examples,
and API reference.

## Companion Crates

- [`fast-telemetry-macros`](../fast-telemetry-macros) — `#[derive(ExportMetrics)]` and `#[derive(LabelEnum)]`
- [`fast-telemetry-export`](../fast-telemetry-export) — DogStatsD, OTLP, ClickHouse, and span export adapters

## Lineage

The `Counter` implementation originated from
[`JackThomson2/fast-counter`](https://github.com/JackThomson2/fast-counter).

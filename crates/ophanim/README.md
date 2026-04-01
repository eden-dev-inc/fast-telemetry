# ophanim

High-performance, cache-friendly telemetry for Rust.

Thread-sharded counters, gauges, histograms, distributions, and lightweight
spans with Prometheus, DogStatsD, and OTLP export.

See the [workspace README](../../README.md) for full documentation, examples,
and API reference.

## Companion Crates

- [`ophanim-macros`](../ophanim-macros) — `#[derive(ExportMetrics)]` and `#[derive(LabelEnum)]`
- [`ophanim-export`](../ophanim-export) — DogStatsD, OTLP, and span export adapters

## Lineage

The `Counter` implementation originated from
[`JackThomson2/fast-counter`](https://github.com/JackThomson2/fast-counter).

# Examples

| Example        | Crates used                             | What it shows                                                                                         |
|----------------|-----------------------------------------|-------------------------------------------------------------------------------------------------------|
| [`demo`](demo) | fast-telemetry, fast-telemetry-macros, fast-telemetry-export | Full pipeline: define metrics with derive macros, record counters/gauges/extrema, export via Prometheus/DogStatsD/OTLP, spans |

Run with:

```bash
cargo run -p fast-telemetry-demo
```

For smaller, single-crate examples (without export), see
[`crates/fast-telemetry/examples/`](../crates/fast-telemetry/examples/).

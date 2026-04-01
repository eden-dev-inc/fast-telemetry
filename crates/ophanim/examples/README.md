# ophanim crate examples

Single-crate examples showing ophanim's core API (without export adapters).

| Example           | What it shows                                                    |
|-------------------|------------------------------------------------------------------|
| `basic`           | Counters, gauges, histograms, Prometheus + DogStatsD text export |
| `labeled`         | Enum-labeled metrics with `DeriveLabel`                          |
| `service_metrics` | Realistic service metric struct with multi-threaded recording    |

Run with:

```bash
cargo run -p ophanim --example basic
cargo run -p ophanim --example labeled
cargo run -p ophanim --example service_metrics
```

For a full-stack example using `ophanim-export` (background exporters, spans,
OTLP), see [`examples/demo`](../../../examples/demo) at the workspace root.

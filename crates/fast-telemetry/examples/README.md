# fast-telemetry crate examples

Single-crate examples showing fast-telemetry's core API (without export adapters).

| Example           | What it shows                                                    |
|-------------------|------------------------------------------------------------------|
| `basic`           | Counters, gauges, histograms, Prometheus + DogStatsD text export |
| `labeled`         | Enum-labeled metrics with `DeriveLabel`                          |
| `service_metrics` | Realistic service metric struct with multi-threaded recording    |

Run with:

```bash
cargo run -p fast-telemetry --example basic
cargo run -p fast-telemetry --example labeled
cargo run -p fast-telemetry --example service_metrics
```

For a full-stack example using `fast-telemetry-export` (background exporters, spans,
OTLP), see [`examples/demo`](../../../examples/demo) at the workspace root.

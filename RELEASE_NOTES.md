# Release Notes

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

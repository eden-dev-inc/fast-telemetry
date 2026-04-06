# fast-telemetry-export

I/O adapters for the [`fast-telemetry`](https://crates.io/crates/fast-telemetry)
runtime crate.

This crate provides:

- DogStatsD export over UDP
- OTLP metrics export over HTTP/protobuf
- OTLP span export over HTTP/protobuf
- stale-series sweeping for dynamic metrics

See the workspace README at
[`eden-dev-inc/fast-telemetry`](https://github.com/eden-dev-inc/fast-telemetry)
for full examples and integration guidance.

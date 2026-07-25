//! Export adapters for fast-telemetry signals.
//!
//! This crate provides the I/O layer for getting fast-telemetry metrics out of your
//! process: DogStatsD over UDP, OTLP over HTTP/protobuf, ClickHouse native TCP,
//! span export, acknowledged OTLP log requests, and stale-series sweeping.
//!
//! Exporters are generic — they accept closures for metric serialization so
//! they work with any metrics struct, not just a specific `AllMetrics` type.
//!
//! # Configuration highlights
//!
//! - OTLP metrics and span exporters support custom `service.name`,
//!   instrumentation scope names, additional resource attributes, per-request
//!   headers, and request timeouts.
//! - OTLP exporters gzip-compress larger protobuf payloads automatically and
//!   use exponential backoff after transport failures.
//! - The span exporter also exposes `max_batch_size` to bound work per cycle.
//! - The stale-series sweeper expects the caller to invoke
//!   `fast_telemetry::advance_cycle()` once per sweep and then call each dynamic
//!   metric's `evict_stale(...)` method.
//! - The `monoio` feature adds monoio-native DogStatsD, OTLP HTTP/protobuf, and
//!   stale-series sweep loops, plus a per-worker span flusher.
//! - The `compio` feature adds compio-native DogStatsD and stale-series sweep
//!   loops, plus a per-worker span flusher when `otlp` is also enabled.
//!
//! # Features
//!
//! - `dogstatsd` (default) — DogStatsD UDP exporter
//! - `otlp` (default) — OTLP HTTP/protobuf metrics and span exporters
//! - `otlp-logs` — OTLP Logs protobuf support on the reusable acknowledged
//!   HTTP client
//! - `clickhouse` — ClickHouse native-protocol metrics exporter, including
//!   first-party rows and OTel-standard table support (via `klickhouse`)
//! - `monoio` — monoio-native exporter loops for monoio applications
//! - `compio` — compio-native exporter loops for compio applications

mod logging;

#[cfg(feature = "clickhouse")]
pub mod clickhouse;

#[cfg(any(feature = "dogstatsd", feature = "compio"))]
pub mod dogstatsd;

#[cfg(feature = "otlp")]
pub mod otlp;

#[cfg(feature = "otlp")]
pub mod spans;

pub mod sweeper;

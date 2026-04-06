//! Export adapters for fast-telemetry metrics.
//!
//! This crate provides the I/O layer for getting fast-telemetry metrics out of your
//! process: DogStatsD over UDP, OTLP over HTTP/protobuf, span export, and
//! stale-series sweeping.
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
//!   [`fast_telemetry::advance_cycle`] once per sweep and then call each dynamic
//!   metric's `evict_stale(...)` method.
//!
//! # Features
//!
//! - `dogstatsd` (default) — DogStatsD UDP exporter
//! - `otlp` (default) — OTLP HTTP/protobuf metrics and span exporters

#[cfg(feature = "dogstatsd")]
pub mod dogstatsd;

#[cfg(feature = "otlp")]
pub mod otlp;

#[cfg(feature = "otlp")]
pub mod spans;

pub mod sweeper;

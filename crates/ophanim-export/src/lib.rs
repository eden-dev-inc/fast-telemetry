//! Export adapters for ophanim metrics.
//!
//! This crate provides the I/O layer for getting ophanim metrics out of your
//! process: DogStatsD over UDP, OTLP over HTTP/protobuf, span export, and
//! stale-series sweeping.
//!
//! Exporters are generic — they accept closures for metric serialization so
//! they work with any metrics struct, not just a specific `AllMetrics` type.
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

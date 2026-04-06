//! Standalone distributed tracing for fast-telemetry.
//!
//! Provides lightweight span creation, parent-child nesting, and collection
//! without the OpenTelemetry SDK dependency. Spans are buffered in thread-local
//! Vecs (zero atomics on the push path) and exported as OTLP protobuf.
//!
//! # Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use fast_telemetry::{SpanCollector, SpanKind};
//!
//! let collector = Arc::new(SpanCollector::new(8, 1024));
//!
//! // Root span (new trace).
//! let mut root = collector.start_span("handle_request", SpanKind::Server);
//! root.enter(); // set thread-local for logging
//! root.set_attribute("http.method", "GET");
//!
//! // Child span (inherits trace_id from &root).
//! {
//!     let mut child = root.child("db_query", SpanKind::Client);
//!     child.set_attribute("db.statement", "SELECT ...");
//!     // child submitted to collector on drop
//! }
//!
//! // For outgoing HTTP requests.
//! let traceparent = root.traceparent();
//!
//! // Logging correlation on the same thread.
//! let trace_id = fast_telemetry::current_trace_id();
//! let span_id = fast_telemetry::current_span_id();
//!
//! // Exporter drains periodically.
//! let mut buf = Vec::new();
//! collector.flush_local();
//! collector.drain_into(&mut buf);
//! ```
//!
//! Call [`SpanCollector::flush_local`] before [`SpanCollector::drain_into`] when
//! draining on the same thread that just recorded spans. `SpanCollector::new`
//! keeps its historical `(shards, capacity)` signature for compatibility, but
//! those parameters are currently ignored because buffers are managed per thread.
//!
//! For manual cross-service propagation, use
//! [`SpanCollector::start_span_from_traceparent`] on inbound requests and
//! [`Span::traceparent`](super::Span::traceparent) for outbound headers.

mod collector;
pub(crate) mod context;
mod ids;
mod types;

pub use collector::SpanCollector;
pub use context::{current_span_id, current_trace_id};
pub use ids::{SpanId, TraceId};
pub use types::{CompletedSpan, Span, SpanAttribute, SpanEvent, SpanKind, SpanStatus, SpanValue};

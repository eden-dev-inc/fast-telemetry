//! Standalone distributed tracing for ophanim.
//!
//! Provides lightweight span creation, parent-child nesting, and collection
//! without the OpenTelemetry SDK dependency. Spans are buffered in thread-local
//! Vecs (zero atomics on the push path) and exported as OTLP protobuf.
//!
//! # Usage
//!
//! ```ignore
//! use std::sync::Arc;
//! use ophanim::{SpanCollector, SpanKind};
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
//! // Exporter drains periodically.
//! let mut buf = Vec::new();
//! collector.drain_into(&mut buf);
//! ```

mod collector;
pub(crate) mod context;
mod ids;
mod types;

pub use collector::SpanCollector;
pub use context::{current_span_id, current_trace_id};
pub use ids::{SpanId, TraceId};
pub use types::{CompletedSpan, Span, SpanAttribute, SpanEvent, SpanKind, SpanStatus, SpanValue};

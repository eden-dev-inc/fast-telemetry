//! Active and completed span types.
//!
//! A [`Span`] accumulates events and attributes during its lifetime. When it
//! drops (or [`Span::end`] is called), a [`CompletedSpan`] is submitted to the
//! associated [`SpanCollector`](super::SpanCollector).
//!
//! Child spans are created via [`Span::child`], which inherits the trace ID
//! and collector reference from the parent — no context cloning required.

use std::borrow::Cow;
use std::ptr::NonNull;
use std::sync::Arc;

use super::collector::SpanCollector;
use super::context::{SpanContext, SpanEnterGuard};
use super::ids::{SpanId, TraceId};

// ---------------------------------------------------------------------------
// CollectorRef — Arc-free borrowed reference to SpanCollector
// ---------------------------------------------------------------------------

/// A borrowed reference to a [`SpanCollector`], avoiding `Arc` refcount contention.
///
/// # Safety
///
/// The `SpanCollector` must outlive all `Span`s that reference it.
/// This is guaranteed by the usage pattern: the collector is held as
/// `Arc<SpanCollector>` in long-lived application state and spans are
/// request-scoped. The public API (`start_span`) takes `&Arc<Self>`,
/// so the `Arc` must be alive when creating spans.
#[derive(Clone, Copy)]
pub(crate) struct CollectorRef(NonNull<SpanCollector>);

// SAFETY: SpanCollector is Send+Sync (contains only Mutex<Vec> outboxes).
// Sharing a raw pointer to it across threads is safe because the underlying
// data is thread-safe and the pointee outlives all Spans.
unsafe impl Send for CollectorRef {}
unsafe impl Sync for CollectorRef {}

impl CollectorRef {
    /// Create a `CollectorRef` from an `Arc<SpanCollector>`.
    pub(crate) fn from_arc(arc: &Arc<SpanCollector>) -> Self {
        Self(NonNull::from(arc.as_ref()))
    }

    /// Dereference to the underlying `SpanCollector`.
    ///
    /// # Safety (upheld by construction)
    ///
    /// Safe because `CollectorRef` is only created via `from_arc`, and the
    /// `Arc<SpanCollector>` in application state outlives all request-scoped spans.
    #[inline]
    pub(crate) fn as_ref(&self) -> &SpanCollector {
        // SAFETY: The SpanCollector is alive as long as the Arc in app state.
        unsafe { self.0.as_ref() }
    }
}

/// Monotonic clock anchor — one `SystemTime::now()` at init, then `Instant` for all reads.
///
/// `Instant` is immune to NTP clock adjustments and slightly faster on most platforms.
/// The anchor captures a single wall-clock reference at first call; subsequent reads
/// use monotonic `Instant::elapsed()` offset from that anchor.
static CLOCK_ANCHOR: std::sync::LazyLock<(std::time::Instant, u64)> =
    std::sync::LazyLock::new(|| {
        let wall = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;
        (std::time::Instant::now(), wall)
    });

/// Current wall-clock time as nanoseconds since Unix epoch.
///
/// Uses a monotonic `Instant` offset from a single wall-clock anchor, avoiding
/// repeated `SystemTime::now()` calls and providing NTP-immune timestamps.
#[inline]
pub(crate) fn now_nanos() -> u64 {
    let (ref mono, wall) = *CLOCK_ANCHOR;
    wall + mono.elapsed().as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Indicates the role of a span in the trace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpanKind {
    /// Default. An internal operation within an application.
    Internal,
    /// Server-side handling of an RPC or HTTP request.
    Server,
    /// Client-side request to a remote service.
    Client,
    /// Producer sending a message to a broker.
    Producer,
    /// Consumer receiving a message from a broker.
    Consumer,
}

/// Final status of a span.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum SpanStatus {
    /// Status not explicitly set (default).
    #[default]
    Unset,
    /// The operation completed successfully.
    Ok,
    /// The operation contained an error.
    Error { message: Cow<'static, str> },
}

// ---------------------------------------------------------------------------
// SpanAttribute / SpanValue / SpanEvent
// ---------------------------------------------------------------------------

/// A key-value attribute attached to a span.
pub struct SpanAttribute {
    pub key: Cow<'static, str>,
    pub value: SpanValue,
}

impl SpanAttribute {
    /// Create a new attribute from a static key and any value that converts to [`SpanValue`].
    pub fn new(key: impl Into<Cow<'static, str>>, value: impl Into<SpanValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Typed attribute value.
pub enum SpanValue {
    String(Cow<'static, str>),
    I64(i64),
    F64(f64),
    Bool(bool),
    /// UUID stored inline (16 bytes), formatted to hyphenated hex only at export time.
    Uuid(uuid::Uuid),
}

impl From<&'static str> for SpanValue {
    fn from(s: &'static str) -> Self {
        Self::String(Cow::Borrowed(s))
    }
}

impl From<String> for SpanValue {
    fn from(s: String) -> Self {
        Self::String(Cow::Owned(s))
    }
}

impl From<Cow<'static, str>> for SpanValue {
    fn from(s: Cow<'static, str>) -> Self {
        Self::String(s)
    }
}

impl From<i64> for SpanValue {
    fn from(v: i64) -> Self {
        Self::I64(v)
    }
}

impl From<f64> for SpanValue {
    fn from(v: f64) -> Self {
        Self::F64(v)
    }
}

impl From<bool> for SpanValue {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}

impl From<uuid::Uuid> for SpanValue {
    fn from(v: uuid::Uuid) -> Self {
        Self::Uuid(v)
    }
}

/// A timestamped event recorded during a span's lifetime.
pub struct SpanEvent {
    pub name: Cow<'static, str>,
    pub time_ns: u64,
    pub attributes: Vec<SpanAttribute>,
}

// ---------------------------------------------------------------------------
// CompletedSpan
// ---------------------------------------------------------------------------

/// A finalized span with start and end timestamps, ready for export.
pub struct CompletedSpan {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: SpanId,
    pub name: Cow<'static, str>,
    pub kind: SpanKind,
    pub start_time_ns: u64,
    pub end_time_ns: u64,
    pub status: SpanStatus,
    pub attributes: Vec<SpanAttribute>,
    pub events: Vec<SpanEvent>,
}

// ---------------------------------------------------------------------------
// Span
// ---------------------------------------------------------------------------

/// An active span that accumulates events and attributes.
///
/// Created via [`SpanCollector::start_span`] (root) or [`Span::child`] (nested).
/// Submits a [`CompletedSpan`] to the collector when dropped or when
/// [`end()`](Span::end) is called explicitly.
///
/// Internally holds `Option<CompletedSpan>` with `end_time_ns = 0` until
/// the span finishes. On drop, `end_time_ns` is set and the span is submitted
/// — no field-by-field move between separate structs.
#[must_use = "span is submitted to collector on drop — bind it to a variable"]
pub struct Span {
    data: Option<CompletedSpan>,
    collector: CollectorRef,
    _enter_guard: Option<SpanEnterGuard>,
}

impl Span {
    /// Create a no-op span that skips all recording and submission.
    ///
    /// Used by adaptive sampling to avoid span creation overhead entirely.
    #[inline]
    pub(crate) fn noop(collector: CollectorRef) -> Self {
        Self {
            data: None,
            collector,
            _enter_guard: None,
        }
    }

    /// Create a new root span (new trace ID).
    #[inline]
    pub(crate) fn new_root(
        name: impl Into<Cow<'static, str>>,
        kind: SpanKind,
        collector: CollectorRef,
    ) -> Self {
        let data = CompletedSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: name.into(),
            kind,
            start_time_ns: now_nanos(),
            end_time_ns: 0,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        };
        Self {
            data: Some(data),
            collector,
            _enter_guard: None,
        }
    }

    /// Create a root span from a remote parent (incoming W3C traceparent).
    pub(crate) fn new_from_remote(
        name: impl Into<Cow<'static, str>>,
        kind: SpanKind,
        parent_ctx: SpanContext,
        collector: CollectorRef,
    ) -> Self {
        let data = CompletedSpan {
            trace_id: parent_ctx.trace_id,
            span_id: SpanId::random(),
            parent_span_id: parent_ctx.span_id,
            name: name.into(),
            kind,
            start_time_ns: now_nanos(),
            end_time_ns: 0,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        };
        Self {
            data: Some(data),
            collector,
            _enter_guard: None,
        }
    }

    /// Create a child span inheriting the trace ID and collector from this span.
    ///
    /// The child's `parent_span_id` is set to this span's `span_id`.
    /// If this span is a no-op (sampled out), the child is also a no-op.
    #[inline]
    pub fn child(&self, name: impl Into<Cow<'static, str>>, kind: SpanKind) -> Span {
        let Some(parent_data) = self.data.as_ref() else {
            return Span::noop(self.collector);
        };
        let data = CompletedSpan {
            trace_id: parent_data.trace_id,
            span_id: SpanId::random(),
            parent_span_id: parent_data.span_id,
            name: name.into(),
            kind,
            start_time_ns: now_nanos(),
            end_time_ns: 0,
            status: SpanStatus::Unset,
            attributes: Vec::new(),
            events: Vec::new(),
        };
        Span {
            data: Some(data),
            collector: self.collector,
            _enter_guard: None,
        }
    }

    /// Set this span as the thread-local "current span" for logging integration.
    ///
    /// The thread-local context is cleared when this span drops. Nested calls
    /// properly restore the previous context.
    pub fn enter(&mut self) -> &mut Self {
        if self._enter_guard.is_none()
            && let Some(data) = self.data.as_ref()
        {
            self._enter_guard = Some(SpanEnterGuard::enter(SpanContext {
                trace_id: data.trace_id,
                span_id: data.span_id,
                trace_flags: 0x01, // sampled
            }));
        }
        self
    }

    /// Add a timestamped event to this span.
    ///
    /// Takes ownership of the attributes vector to avoid cloning.
    pub fn add_event(
        &mut self,
        name: impl Into<Cow<'static, str>>,
        attributes: Vec<SpanAttribute>,
    ) {
        if let Some(data) = self.data.as_mut() {
            data.events.push(SpanEvent {
                name: name.into(),
                time_ns: now_nanos(),
                attributes,
            });
        }
    }

    /// Add a timestamped event with no attributes.
    pub fn add_simple_event(&mut self, name: impl Into<Cow<'static, str>>) {
        if let Some(data) = self.data.as_mut() {
            data.events.push(SpanEvent {
                name: name.into(),
                time_ns: now_nanos(),
                attributes: Vec::new(),
            });
        }
    }

    /// Set a span attribute.
    #[inline]
    pub fn set_attribute(
        &mut self,
        key: impl Into<Cow<'static, str>>,
        value: impl Into<SpanValue>,
    ) {
        if let Some(data) = self.data.as_mut() {
            data.attributes.push(SpanAttribute {
                key: key.into(),
                value: value.into(),
            });
        }
    }

    /// Set the span status.
    #[inline]
    pub fn set_status(&mut self, status: SpanStatus) {
        if let Some(data) = self.data.as_mut() {
            data.status = status;
        }
    }

    /// Get the trace ID of this span.
    pub fn trace_id(&self) -> TraceId {
        self.data.as_ref().map_or(TraceId::INVALID, |d| d.trace_id)
    }

    /// Get the span ID of this span.
    pub fn span_id(&self) -> SpanId {
        self.data.as_ref().map_or(SpanId::INVALID, |d| d.span_id)
    }

    /// Encode this span's context as a W3C `traceparent` header for outgoing requests.
    ///
    /// Returns an empty string for no-op (sampled-out) spans.
    pub fn traceparent(&self) -> String {
        let Some(data) = self.data.as_ref() else {
            return String::new();
        };
        let ctx = SpanContext {
            trace_id: data.trace_id,
            span_id: data.span_id,
            trace_flags: 0x01, // sampled
        };
        ctx.to_traceparent()
    }

    /// Explicitly end the span and submit it to the collector.
    ///
    /// Equivalent to dropping the span. After calling this, the span is consumed.
    pub fn end(self) {
        // Drop impl handles submission.
        drop(self);
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if let Some(mut completed) = self.data.take() {
            completed.end_time_ns = now_nanos();
            self.collector.as_ref().submit(completed);
        }
        // _enter_guard drops automatically, restoring thread-local context.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_collector() -> Arc<SpanCollector> {
        Arc::new(SpanCollector::new(1, 64))
    }

    /// Flush thread-local buffer + drain for same-thread tests.
    fn flush_and_drain(collector: &SpanCollector, buf: &mut Vec<CompletedSpan>) {
        collector.flush_local();
        collector.drain_into(buf);
    }

    #[test]
    fn root_span_lifecycle() {
        let collector = test_collector();
        {
            let mut span = Span::new_root(
                "test_op",
                SpanKind::Server,
                CollectorRef::from_arc(&collector),
            );
            span.set_attribute("key", "value");
            span.add_simple_event("checkpoint");
            span.set_status(SpanStatus::Ok);
        } // span drops here

        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 1);

        let completed = &buf[0];
        assert!(!completed.trace_id.is_invalid());
        assert!(!completed.span_id.is_invalid());
        assert!(completed.parent_span_id.is_invalid()); // root span
        assert_eq!(completed.name, "test_op");
        assert_eq!(completed.kind, SpanKind::Server);
        assert_eq!(completed.status, SpanStatus::Ok);
        assert_eq!(completed.attributes.len(), 1);
        assert_eq!(completed.events.len(), 1);
        assert!(completed.end_time_ns >= completed.start_time_ns);
    }

    #[test]
    fn child_inherits_trace_id() {
        let collector = test_collector();
        let root_trace_id;
        let root_span_id;
        {
            let root = Span::new_root(
                "parent",
                SpanKind::Server,
                CollectorRef::from_arc(&collector),
            );
            root_trace_id = root.trace_id();
            root_span_id = root.span_id();
            {
                let _child = root.child("child_op", SpanKind::Client);
                // child drops first
            }
            // root drops second
        }

        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 2);

        // Child was dropped first, so it's first in the queue.
        let child = &buf[0];
        let parent = &buf[1];

        assert_eq!(child.trace_id, root_trace_id);
        assert_eq!(child.parent_span_id, root_span_id);
        assert_eq!(parent.trace_id, root_trace_id);
        assert!(parent.parent_span_id.is_invalid());
    }

    #[test]
    fn explicit_end() {
        let collector = test_collector();
        let span = Span::new_root(
            "explicit",
            SpanKind::Internal,
            CollectorRef::from_arc(&collector),
        );
        span.end();

        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn traceparent_encoding() {
        let collector = test_collector();
        let span = Span::new_root(
            "tp_test",
            SpanKind::Server,
            CollectorRef::from_arc(&collector),
        );
        let tp = span.traceparent();

        // Format: 00-{32 hex}-{16 hex}-01
        assert!(tp.starts_with("00-"));
        assert!(tp.ends_with("-01"));
        assert_eq!(tp.len(), 55);
    }

    #[test]
    fn from_remote_parent() {
        let collector = test_collector();
        let remote_ctx = SpanContext::from_traceparent(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        )
        .expect("valid traceparent");

        let span = Span::new_from_remote(
            "server_handler",
            SpanKind::Server,
            remote_ctx,
            CollectorRef::from_arc(&collector),
        );
        assert_eq!(span.trace_id(), remote_ctx.trace_id);
        drop(span);

        let mut buf = Vec::new();
        flush_and_drain(&collector, &mut buf);
        let completed = &buf[0];
        assert_eq!(completed.trace_id, remote_ctx.trace_id);
        assert_eq!(completed.parent_span_id, remote_ctx.span_id);
    }
}

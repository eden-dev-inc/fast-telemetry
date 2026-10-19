//! Active and completed span types.
//!
//! A [`Span`] accumulates events and attributes during its lifetime. When it
//! drops (or [`Span::end`] is called), a [`CompletedSpan`] is submitted to the
//! associated [`SpanCollector`](super::SpanCollector).
//!
//! Child spans are created via [`Span::child`], which inherits the trace ID
//! and collector reference from the parent — no context cloning required.

use std::borrow::Cow;
use std::sync::Arc;

use super::clock::now_nanos;
use super::collector::{CollectorState, SpanCollector};
use super::context::{SpanContext, SpanEnterGuard};
use super::ids::{SpanId, TraceId};

// ---------------------------------------------------------------------------
// CollectorRef — Arc-free borrowed reference to SpanCollector
// ---------------------------------------------------------------------------

/// A borrow of collector state. Its lifetime prevents the owner from
/// being freed while a native span can still submit to it. Copying this shared
/// reference performs no Arc reference-count updates.
#[derive(Clone, Copy)]
pub(crate) struct CollectorRef<'collector>(&'collector CollectorState);

impl<'collector> CollectorRef<'collector> {
    /// Borrow an Arc owner for the lifetime of the span.
    pub(crate) fn from_arc(arc: &'collector Arc<SpanCollector>) -> Self {
        Self(arc.borrowed_state())
    }

    /// Access borrowed state without cloning its Arc.
    #[inline]
    pub(crate) fn as_ref(&self) -> &'collector CollectorState {
        self.0
    }
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

/// Optional metadata, created only when attributes, events or an error need it.
#[derive(Default)]
struct SpanPayload {
    error: Option<Cow<'static, str>>,
    attributes: Vec<SpanAttribute>,
    events: Vec<SpanEvent>,
}

/// Small active record; materializes the public completed record only at finish.
struct ActiveSpan {
    trace_id: TraceId,
    span_id: SpanId,
    parent_span_id: SpanId,
    name: Cow<'static, str>,
    kind: SpanKind,
    start_time_ns: u64,
    ok: bool,
    payload: Option<Box<SpanPayload>>,
}

impl ActiveSpan {
    #[inline]
    fn payload_mut(&mut self) -> &mut SpanPayload {
        self.payload.get_or_insert_with(new_payload)
    }

    fn set_status(&mut self, status: SpanStatus) {
        match status {
            SpanStatus::Error { message } => {
                self.payload_mut().error = Some(message);
                self.ok = false;
            }
            SpanStatus::Ok | SpanStatus::Unset => {
                self.ok = matches!(status, SpanStatus::Ok);
                if let Some(payload) = &mut self.payload {
                    payload.error = None;
                }
            }
        }
    }

    #[inline]
    fn finish(self, end_time_ns: u64) -> CompletedSpan {
        let (error, attributes, events) = finish_payload(self.payload);
        let status = match error {
            Some(message) => SpanStatus::Error { message },
            None if self.ok => SpanStatus::Ok,
            None => SpanStatus::Unset,
        };
        CompletedSpan {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name: self.name,
            kind: self.kind,
            start_time_ns: self.start_time_ns,
            end_time_ns,
            status,
            attributes,
            events,
        }
    }
}

// Keep only empty metadata boxes, bounded independently of collector lifetimes.
const PAYLOAD_CACHE_LIMIT: usize = 32;

thread_local! {
    // Each box becomes one active span's metadata without moving its allocation.
    #[allow(clippy::vec_box)]
    static PAYLOAD_CACHE: std::cell::RefCell<Vec<Box<SpanPayload>>> = const {
        std::cell::RefCell::new(Vec::new())
    };
}

fn new_payload() -> Box<SpanPayload> {
    let cached = PAYLOAD_CACHE.try_with(|cache| {
        cache
            .try_borrow_mut()
            .ok()
            .and_then(|mut values| values.pop())
    });
    match cached {
        Ok(Some(payload)) => payload,
        _ => Box::new(SpanPayload::default()),
    }
}

#[inline]
fn finish_payload(
    payload: Option<Box<SpanPayload>>,
) -> (
    Option<Cow<'static, str>>,
    Vec<SpanAttribute>,
    Vec<SpanEvent>,
) {
    let Some(mut payload) = payload else {
        return (None, Vec::new(), Vec::new());
    };
    let error = payload.error.take();
    let attributes = std::mem::take(&mut payload.attributes);
    let events = std::mem::take(&mut payload.events);
    let _ = PAYLOAD_CACHE.try_with(|cache| {
        if let Ok(mut values) = cache.try_borrow_mut()
            && values.len() < PAYLOAD_CACHE_LIMIT
        {
            values.push(payload);
        }
    });
    (error, attributes, events)
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
/// Keeps identifiers, start time and name in a compact active record. Optional
/// attributes, events and error storage are created when first used. On drop,
/// the end timestamp is captured and a completed record is submitted.
///
/// A native span borrows its collector's Arc owner, which must remain alive
/// until that span and its children finish. No reference count is updated per
/// span. A child may finish after its parent, as long as the collector lives.
///
/// ```compile_fail,E0597
/// use std::sync::Arc;
/// use fast_telemetry::{SpanCollector, SpanKind};
/// let span = {
///     let collector = Arc::new(SpanCollector::new(1, 64));
///     collector.start_span("request", SpanKind::Server)
/// };
/// drop(span);
/// ```
///
/// ```compile_fail,E0505
/// use std::sync::Arc;
/// use fast_telemetry::{SpanCollector, SpanKind};
/// let collector = Arc::new(SpanCollector::new(1, 64));
/// let span = collector.start_span("request", SpanKind::Server);
/// drop(collector);
/// drop(span);
/// ```
///
/// ```compile_fail,E0597
/// use std::sync::Arc;
/// use fast_telemetry::{SpanCollector, SpanKind};
/// let span = {
///     let collector = Arc::new(SpanCollector::new(1, 64));
///     collector.start_span_from_traceparent(None, "request", SpanKind::Server)
/// };
/// drop(span);
/// ```
#[must_use = "span is submitted to collector on drop — bind it to a variable"]
pub struct Span<'collector> {
    data: Option<ActiveSpan>,
    collector: CollectorRef<'collector>,
    _enter_guard: Option<SpanEnterGuard>,
}

impl<'collector> Span<'collector> {
    /// Create a no-op span that skips all recording and submission.
    ///
    /// Used by adaptive sampling to avoid span creation overhead entirely.
    #[inline]
    pub(crate) fn noop(collector: CollectorRef<'collector>) -> Self {
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
        collector: CollectorRef<'collector>,
    ) -> Self {
        let data = ActiveSpan {
            trace_id: TraceId::random(),
            span_id: SpanId::random(),
            parent_span_id: SpanId::INVALID,
            name: name.into(),
            kind,
            start_time_ns: now_nanos(),
            ok: false,
            payload: None,
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
        collector: CollectorRef<'collector>,
    ) -> Self {
        let data = ActiveSpan {
            trace_id: parent_ctx.trace_id,
            span_id: SpanId::random(),
            parent_span_id: parent_ctx.span_id,
            name: name.into(),
            kind,
            start_time_ns: now_nanos(),
            ok: false,
            payload: None,
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
    pub fn child(&self, name: impl Into<Cow<'static, str>>, kind: SpanKind) -> Span<'collector> {
        let Some(parent_data) = self.data.as_ref() else {
            return Span::noop(self.collector);
        };
        let data = ActiveSpan {
            trace_id: parent_data.trace_id,
            span_id: SpanId::random(),
            parent_span_id: parent_data.span_id,
            name: name.into(),
            kind,
            start_time_ns: now_nanos(),
            ok: false,
            payload: None,
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
    #[inline]
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
            data.payload_mut().events.push(SpanEvent {
                name: name.into(),
                time_ns: now_nanos(),
                attributes,
            });
        }
    }

    /// Add a timestamped event with no attributes.
    pub fn add_simple_event(&mut self, name: impl Into<Cow<'static, str>>) {
        if let Some(data) = self.data.as_mut() {
            data.payload_mut().events.push(SpanEvent {
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
            data.payload_mut().attributes.push(SpanAttribute {
                key: key.into(),
                value: value.into(),
            });
        }
    }

    /// Set the span status.
    #[inline]
    pub fn set_status(&mut self, status: SpanStatus) {
        if let Some(data) = self.data.as_mut() {
            data.set_status(status);
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

impl Drop for Span<'_> {
    fn drop(&mut self) {
        if let Some(active) = self.data.take() {
            self.collector.as_ref().submit(active.finish(now_nanos()));
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
    fn child_can_finish_after_parent_while_collector_lives() {
        let collector = test_collector();
        let parent = collector.start_span("parent", SpanKind::Server);
        let parent_id = parent.span_id();
        let child = parent.child("child", SpanKind::Client);
        drop(parent);
        drop(child);
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].name, "parent");
        assert_eq!(spans[1].name, "child");
        assert_eq!(spans[1].parent_span_id, parent_id);
        assert_eq!(spans[0].trace_id, spans[1].trace_id);
    }

    #[test]
    fn borrowed_span_is_send_sync_and_can_finish_on_a_scoped_thread() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Span<'_>>();
        let collector = test_collector();
        let span = collector.start_span("scoped", SpanKind::Internal);
        let id = span.span_id();
        std::thread::scope(|scope| {
            scope
                .spawn(move || drop(span))
                .join()
                .expect("join scoped span");
        });
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].span_id, id);
        assert_eq!(collector.recorded_count(), 1);
        assert_eq!(collector.sampled_out_count(), 0);
    }

    #[test]
    fn reused_payloads_preserve_values_and_status_replacement() {
        let collector = test_collector();
        for index in 0..64 {
            let mut span = collector.start_span(format!("reuse-{index}"), SpanKind::Server);
            match index % 4 {
                0 => {
                    span.set_attribute("index", index as i64);
                    span.add_simple_event("error-event");
                    span.set_status(SpanStatus::Error {
                        message: "borrowed error".into(),
                    });
                }
                1 => {
                    span.set_status(SpanStatus::Ok);
                }
                2 => {
                    span.set_status(SpanStatus::Error {
                        message: format!("owned-{index}").into(),
                    });
                    span.set_status(SpanStatus::Ok);
                    span.set_status(SpanStatus::Error {
                        message: "temporary".into(),
                    });
                    span.set_status(SpanStatus::Unset);
                }
                _ => {
                    span.add_simple_event("only-event");
                }
            }
        }
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 64);
        for (index, span) in spans.iter().enumerate() {
            assert_eq!(span.name, format!("reuse-{index}"));
            assert_eq!(span.attributes.len(), usize::from(index % 4 == 0));
            assert_eq!(
                span.events.len(),
                usize::from(index % 4 == 0 || index % 4 == 3)
            );
            match index % 4 {
                0 => {
                    assert_eq!(span.attributes[0].key, "index");
                    assert!(
                        matches!(span.attributes[0].value, SpanValue::I64(value) if value == index as i64)
                    );
                    assert_eq!(span.events[0].name, "error-event");
                    assert!(matches!(
                        &span.status,
                        SpanStatus::Error {
                            message: Cow::Borrowed("borrowed error")
                        }
                    ));
                }
                1 => assert_eq!(span.status, SpanStatus::Ok),
                2 => assert_eq!(span.status, SpanStatus::Unset),
                _ => {
                    assert_eq!(span.events[0].name, "only-event");
                    assert_eq!(span.status, SpanStatus::Unset);
                }
            }
        }
    }

    #[test]
    fn metadata_cache_is_bounded_and_retains_only_empty_boxes() {
        PAYLOAD_CACHE.with(|cache| cache.borrow_mut().clear());
        let collector = test_collector();
        let active: Vec<_> = (0..PAYLOAD_CACHE_LIMIT + 9)
            .map(|index| {
                let mut span = collector.start_span(format!("active-{index}"), SpanKind::Internal);
                span.set_attribute("index", index as i64);
                span.add_simple_event("owned-by-completed-span");
                span.set_status(SpanStatus::Error {
                    message: format!("error-{index}").into(),
                });
                span
            })
            .collect();
        drop(active);
        PAYLOAD_CACHE.with(|cache| {
            let cache = cache.borrow();
            assert_eq!(cache.len(), PAYLOAD_CACHE_LIMIT);
            assert!(cache.iter().all(|payload| payload.error.is_none()
                && payload.attributes.is_empty()
                && payload.events.is_empty()));
        });
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), PAYLOAD_CACHE_LIMIT + 9);
        for (index, span) in spans.iter().enumerate() {
            assert_eq!(span.name, format!("active-{index}"));
            assert!(
                matches!(span.attributes[0].value, SpanValue::I64(value) if value == index as i64)
            );
            assert_eq!(span.events[0].name, "owned-by-completed-span");
            assert_eq!(
                span.status,
                SpanStatus::Error {
                    message: format!("error-{index}").into()
                }
            );
        }
    }

    #[test]
    fn metadata_cache_borrow_falls_back_without_losing_payload() {
        let collector = test_collector();
        PAYLOAD_CACHE.with(|cache| {
            let mut guard = cache.borrow_mut();
            guard.clear();
            let mut span = collector.start_span("borrowed-cache", SpanKind::Internal);
            span.set_attribute("value", "retained");
            drop(span);
            assert!(guard.is_empty());
        });
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].attributes[0].key, "value");
        assert!(matches!(
            &spans[0].attributes[0].value,
            SpanValue::String(Cow::Borrowed("retained"))
        ));
    }

    #[test]
    fn metadata_span_can_finish_on_a_scoped_thread() {
        let collector = test_collector();
        let mut span = collector.start_span("moved-metadata", SpanKind::Client);
        span.set_attribute("owned", String::from("value"));
        span.add_simple_event("event");
        span.set_status(SpanStatus::Error {
            message: String::from("owned error").into(),
        });
        std::thread::scope(|scope| {
            scope
                .spawn(move || drop(span))
                .join()
                .expect("join metadata span")
        });
        let mut spans = Vec::new();
        flush_and_drain(&collector, &mut spans);
        assert_eq!(spans.len(), 1);
        assert!(
            matches!(&spans[0].attributes[0].value, SpanValue::String(Cow::Owned(value)) if value == "value")
        );
        assert_eq!(spans[0].events[0].name, "event");
        assert!(
            matches!(&spans[0].status, SpanStatus::Error { message: Cow::Owned(message) } if message == "owned error")
        );
    }

    #[test]
    fn metadata_span_survives_cache_tls_destruction() {
        struct LateSpan(Arc<SpanCollector>);
        impl Drop for LateSpan {
            fn drop(&mut self) {
                let mut span = self
                    .0
                    .start_span("after-cache-destruction", SpanKind::Internal);
                span.set_attribute("late", "retained");
                span.add_simple_event("late-event");
            }
        }
        thread_local! {
            static LATE: std::cell::RefCell<Option<LateSpan>> = const { std::cell::RefCell::new(None) };
        }
        let collector = test_collector();
        let worker = Arc::clone(&collector);
        std::thread::spawn(move || {
            drop(worker.start_span("initialize-collector-tls", SpanKind::Internal));
            LATE.with(|late| *late.borrow_mut() = Some(LateSpan(Arc::clone(&worker))));
            let mut span = worker.start_span("initialize-cache-tls", SpanKind::Internal);
            span.set_attribute("first", "value");
        })
        .join()
        .expect("join TLS destruction worker");
        let mut spans = Vec::new();
        collector.drain_into(&mut spans);
        assert_eq!(spans.len(), 3);
        let late = spans
            .iter()
            .find(|span| span.name == "after-cache-destruction")
            .expect("late metadata span");
        assert!(matches!(
            &late.attributes[0].value,
            SpanValue::String(Cow::Borrowed("retained"))
        ));
        assert_eq!(late.events[0].name, "late-event");
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

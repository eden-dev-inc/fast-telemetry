//! Tracing layer and standalone subscriber backends for fast-telemetry.
//!
//! Existing `tracing` macros, `#[instrument]`, and `Instrument` futures retain
//! their frontend. The composable layer uses Registry; the optional direct
//! subscriber owns its lifecycle storage. Completed spans are published directly
//! into a bounded [`SpanSink`], without native span TLS or adaptive sampling.
//! See the crate README for propagation, field mapping, capacity, and export.

use std::any::TypeId;
use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use fast_telemetry::{
    CompletedSpan, SpanAttribute, SpanCollector, SpanContext, SpanEvent, SpanId, SpanKind,
    SpanSink, SpanStatus, SpanValue, TraceId,
};
use parking_lot::Mutex;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Dispatch, Event, Level, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{Layer, Registry};

#[cfg(feature = "otlp")]
mod otlp;
mod subscriber;

pub use subscriber::{FastTelemetrySubscriber, MetadataPolicy};

const PREFIX: &str = "fast_telemetry.";
static NEXT_LAYER_ID: AtomicU64 = AtomicU64::new(1);
static CLOCK: std::sync::LazyLock<(Instant, u64)> = std::sync::LazyLock::new(|| {
    let wall = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    (Instant::now(), wall)
});

fn now_ns() -> u64 {
    CLOCK.1.saturating_add(CLOCK.0.elapsed().as_nanos() as u64)
}

struct State {
    span: CompletedSpan,
    trace_flags: u8,
}

impl State {
    fn context(&self) -> SpanContext {
        SpanContext {
            trace_id: self.span.trace_id,
            span_id: self.span.span_id,
            trace_flags: self.trace_flags,
        }
    }
}

// Registry extensions are keyed by TypeId. Keep the usual single layer inline;
// additional layers share this extension without replacing each other's state.
struct States {
    first: Option<(u64, State)>,
    additional: Vec<(u64, State)>,
}

impl States {
    fn new(layer_id: u64, state: State) -> Self {
        Self {
            first: Some((layer_id, state)),
            additional: Vec::new(),
        }
    }

    fn get(&self, layer_id: u64) -> Option<&State> {
        self.first
            .as_ref()
            .filter(|(id, _)| *id == layer_id)
            .or_else(|| self.additional.iter().find(|(id, _)| *id == layer_id))
            .map(|(_, state)| state)
    }

    fn get_mut(&mut self, layer_id: u64) -> Option<&mut State> {
        if let Some((id, state)) = &mut self.first
            && *id == layer_id
        {
            return Some(state);
        }
        self.additional
            .iter_mut()
            .find(|(id, _)| *id == layer_id)
            .map(|(_, state)| state)
    }

    fn insert(&mut self, layer_id: u64, state: State) {
        if let Some(existing) = self.get_mut(layer_id) {
            *existing = state;
        } else if self.first.is_none() {
            self.first = Some((layer_id, state));
        } else {
            self.additional.push((layer_id, state));
        }
    }

    fn remove(&mut self, layer_id: u64) -> Option<State> {
        if self.first.as_ref().is_some_and(|(id, _)| *id == layer_id) {
            return self.first.take().map(|(_, state)| state);
        }
        let index = self.additional.iter().position(|(id, _)| *id == layer_id)?;
        Some(self.additional.swap_remove(index).1)
    }
}

struct Shared {
    id: u64,
    sink: SpanSink,
    events: Mutex<Vec<RecordedEvent>>,
    event_capacity: usize,
    events_accepted: AtomicU64,
    events_dropped: AtomicU64,
}

/// A standalone event, including events whose parent is not recorded by this
/// layer. Events attached to a recorded span live in `CompletedSpan::events`.
pub struct RecordedEvent {
    pub event: SpanEvent,
    pub level: Level,
    pub context: Option<SpanContext>,
}

/// A registry layer that records tracing spans and events into fast-telemetry.
///
/// Parent selection, cloning, enter/exit, and final closure belong to tracing's
/// registry. This layer has no thread-local entered state of its own. Span
/// duration is the full creation-to-final-close lifetime, including async idle.
pub struct FastTelemetryLayer<S = Registry> {
    shared: Arc<Shared>,
    access: ContextAccess,
    _subscriber: PhantomData<fn(S)>,
}

/// An owning backend handle for propagation, loss counters, and event drain.
#[derive(Clone)]
pub struct LayerHandle {
    shared: Arc<Shared>,
}

impl<S> FastTelemetryLayer<S>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    /// Use a queue of at most 4096 completed spans and 4096 standalone events.
    pub fn new(collector: Arc<SpanCollector>) -> Self {
        Self::with_capacity(collector, 4096, 4096)
    }

    /// Configure independent bounded queues. No adaptive sampling is applied.
    ///
    /// Capacity exhaustion drops the newest item and increments its loss
    /// counter. Capacity counts items, not bytes or live span fields/events.
    /// Panics if either capacity is zero.
    pub fn with_capacity(
        collector: Arc<SpanCollector>,
        span_capacity: usize,
        event_capacity: usize,
    ) -> Self {
        assert!(event_capacity > 0, "event capacity must be nonzero");
        Self {
            shared: Arc::new(Shared {
                id: NEXT_LAYER_ID.fetch_add(1, Ordering::Relaxed),
                sink: collector.span_sink(span_capacity),
                events: Mutex::new(Vec::new()),
                event_capacity,
                events_accepted: AtomicU64::new(0),
                events_dropped: AtomicU64::new(0),
            }),
            access: ContextAccess(access::<S>),
            _subscriber: PhantomData,
        }
    }

    pub fn handle(&self) -> LayerHandle {
        LayerHandle {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl LayerHandle {
    /// Get this backend's context for a span, including a span from another
    /// subscriber than the caller's current default. Disabled/filtered spans
    /// and spans belonging to subscribers without this backend return `None`.
    pub fn context(&self, span: &tracing::Span) -> Option<SpanContext> {
        let mut result = None;
        self.with_state(span, &mut |state| result = Some(state.context()));
        result
    }

    /// Encode this span's W3C traceparent; returns `None` if not recorded here.
    pub fn traceparent(&self, span: &tracing::Span) -> Option<String> {
        self.context(span).map(SpanContext::to_traceparent)
    }

    /// Assign an incoming remote parent. Call immediately after creating the
    /// span, before creating children or propagating its context. Existing
    /// children and already-emitted events are not rewritten. No sampling is
    /// inferred from the remote flags: this layer always records enabled spans.
    /// Returns false for invalid IDs or a span not recorded by this layer.
    pub fn set_parent(&self, span: &tracing::Span, parent: SpanContext) -> bool {
        if parent.trace_id.is_invalid() || parent.span_id.is_invalid() {
            return false;
        }
        let mut found = false;
        self.with_state(span, &mut |state| {
            state.span.trace_id = parent.trace_id;
            state.span.parent_span_id = parent.span_id;
            // This backend records every enabled span, including children of
            // an unsampled remote context; propagate our local sampling choice.
            state.trace_flags = parent.trace_flags | 1;
            found = true;
        });
        found
    }

    fn with_state(&self, span: &tracing::Span, f: &mut dyn FnMut(&mut State)) {
        span.with_subscriber(|(id, dispatch)| {
            if let Some(subscriber) = dispatch.downcast_ref::<FastTelemetrySubscriber>()
                && Arc::ptr_eq(&subscriber.shared, &self.shared)
            {
                subscriber.with_state(id, f);
            } else if let Some(access) = dispatch.downcast_ref::<ContextAccess>() {
                (access.0)(dispatch, id, self.shared.id, f);
            }
        });
    }

    pub fn accepted_span_count(&self) -> u64 {
        self.shared.sink.accepted_count()
    }
    pub fn dropped_span_count(&self) -> u64 {
        self.shared.sink.rejected_count()
    }
    pub fn accepted_event_count(&self) -> u64 {
        self.shared.events_accepted.load(Ordering::Relaxed)
    }
    pub fn dropped_event_count(&self) -> u64 {
        self.shared.events_dropped.load(Ordering::Relaxed)
    }

    /// Drain standalone events; no worker-local flush is needed. This is
    /// destructive and competing consumers divide the available events.
    pub fn drain_events(&self, output: &mut Vec<RecordedEvent>) {
        output.append(&mut self.shared.events.lock());
    }
}

// Type-erased registry access, following tracing-subscriber's layer downcast
// protocol. The layer's function remembers S; the dispatch owns both S and the
// layer. Lookups use the span's own dispatch and a unique per-layer ID.
#[allow(clippy::type_complexity)] // Type-erased registry callback, tied to the owning dispatch.
struct ContextAccess(fn(&Dispatch, &Id, u64, &mut dyn FnMut(&mut State)));

fn access<S>(dispatch: &Dispatch, id: &Id, layer_id: u64, f: &mut dyn FnMut(&mut State))
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    if let Some(subscriber) = dispatch.downcast_ref::<S>()
        && let Some(span) = subscriber.span(id)
        && let Some(states) = span.extensions_mut().get_mut::<States>()
        && let Some(state) = states.get_mut(layer_id)
    {
        f(state);
    }
}

impl<S> Layer<S> for FastTelemetryLayer<S>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        // Arbitrary Debug/Display implementations may emit tracing recursively.
        // Visit all user values before acquiring any registry extensions lock.
        let mut fields = Fields::with_metadata(attrs.metadata(), attrs.values().len());
        attrs.record(&mut fields);
        metadata_fields(attrs.metadata(), &mut fields.0);
        let parent = if attrs.is_contextual() {
            ctx.lookup_current()
        } else {
            attrs.parent().and_then(|id| ctx.span(id))
        };
        let parent = parent.and_then(|span| {
            span.extensions()
                .get::<States>()?
                .get(self.shared.id)
                .map(State::context)
        });
        let state = State {
            trace_flags: parent.map_or(1, |p| p.trace_flags),
            span: CompletedSpan {
                trace_id: parent.map_or_else(TraceId::random, |p| p.trace_id),
                span_id: SpanId::random(),
                parent_span_id: parent.map_or(SpanId::INVALID, |p| p.span_id),
                name: Cow::Borrowed(attrs.metadata().name()),
                kind: SpanKind::Internal,
                start_time_ns: now_ns(),
                end_time_ns: 0,
                status: SpanStatus::Unset,
                attributes: fields.0,
                events: Vec::new(),
            },
        };
        if let Some(span) = ctx.span(id) {
            let mut extensions = span.extensions_mut();
            if let Some(states) = extensions.get_mut::<States>() {
                states.insert(self.shared.id, state);
            } else {
                extensions.insert(States::new(self.shared.id, state));
            }
        }
    }

    fn on_record(&self, id: &Id, record: &Record<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields(Vec::with_capacity(record.len()));
        record.record(&mut fields);
        if let Some(span) = ctx.span(id)
            && let Some(states) = span.extensions_mut().get_mut::<States>()
            && let Some(state) = states.get_mut(self.shared.id)
        {
            for attribute in fields.0 {
                upsert(&mut state.span.attributes, attribute);
            }
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut fields = Fields::with_metadata(event.metadata(), event.metadata().fields().len());
        event.record(&mut fields);
        metadata_fields(event.metadata(), &mut fields.0);
        let recorded = SpanEvent {
            name: Cow::Borrowed(event.metadata().name()),
            time_ns: now_ns(),
            attributes: fields.0,
        };
        // event_span handles contextual, explicit, and parent: None events.
        if let Some(parent) = ctx.event_span(event)
            && let Some(states) = parent.extensions_mut().get_mut::<States>()
            && let Some(state) = states.get_mut(self.shared.id)
        {
            state.span.events.push(recorded);
            return;
        }
        let mut events = self.shared.events.lock();
        if events.len() >= self.shared.event_capacity {
            self.shared.events_dropped.fetch_add(1, Ordering::Relaxed);
        } else {
            events.push(RecordedEvent {
                event: recorded,
                level: *event.metadata().level(),
                context: None,
            });
            self.shared.events_accepted.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn on_follows_from(&self, id: &Id, follows: &Id, ctx: Context<'_, S>) {
        // Do not hold extensions locks for two spans at once (self-links and
        // reversed concurrent links must not deadlock).
        let target = ctx.span(follows).and_then(|span| {
            span.extensions()
                .get::<States>()?
                .get(self.shared.id)
                .map(State::context)
        });
        if let Some(target) = target
            && let Some(span) = ctx.span(id)
            && let Some(states) = span.extensions_mut().get_mut::<States>()
            && let Some(state) = states.get_mut(self.shared.id)
        {
            state.span.events.push(SpanEvent {
                name: Cow::Borrowed("fast_telemetry.follows_from"),
                time_ns: now_ns(),
                attributes: vec![
                    SpanAttribute::new("fast_telemetry.link.trace_id", target.trace_id.to_string()),
                    SpanAttribute::new("fast_telemetry.link.span_id", target.span_id.to_string()),
                ],
            });
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let state = ctx.span(&id).and_then(|span| {
            span.extensions_mut()
                .get_mut::<States>()?
                .remove(self.shared.id)
        });
        if let Some(mut state) = state {
            state.span.end_time_ns = now_ns();
            // Capacity failure is observable through the sink's loss counter.
            let _ = self.shared.sink.publish(state.span);
        }
    }

    // SAFETY: returned pointers reference fields of this live layer. The type
    // check ensures callers can only interpret them as the matching concrete
    // type. ContextAccess never outlives its containing dispatch/layer.
    unsafe fn downcast_raw(&self, id: TypeId) -> Option<*const ()> {
        if id == TypeId::of::<Self>() {
            Some(self as *const Self as *const ())
        } else if id == TypeId::of::<ContextAccess>() {
            Some(&self.access as *const ContextAccess as *const ())
        } else {
            None
        }
    }
}

fn user_key(name: &'static str) -> Cow<'static, str> {
    if name.starts_with(PREFIX) {
        Cow::Owned(format!("fast_telemetry.user.{name}"))
    } else {
        Cow::Borrowed(name)
    }
}

fn upsert(attributes: &mut Vec<SpanAttribute>, attribute: SpanAttribute) {
    if let Some(existing) = attributes.iter_mut().find(|a| a.key == attribute.key) {
        existing.value = attribute.value;
    } else {
        attributes.push(attribute);
    }
}

struct Fields(Vec<SpanAttribute>);

impl Fields {
    fn with_metadata(metadata: &Metadata<'_>, user_fields: usize) -> Self {
        let metadata_fields = 2
            + usize::from(metadata.module_path().is_some())
            + usize::from(metadata.file().is_some())
            + usize::from(metadata.line().is_some());
        Self(Vec::with_capacity(user_fields + metadata_fields))
    }

    fn value(&mut self, field: &Field, value: impl Into<SpanValue>) {
        upsert(
            &mut self.0,
            SpanAttribute::new(user_key(field.name()), value),
        );
    }
}

impl Visit for Fields {
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.value(field, value);
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        match i64::try_from(value) {
            Ok(value) => self.value(field, value),
            Err(_) => self.value(field, value.to_string()),
        }
    }
    fn record_i128(&mut self, field: &Field, value: i128) {
        match i64::try_from(value) {
            Ok(value) => self.value(field, value),
            Err(_) => self.value(field, value.to_string()),
        }
    }
    fn record_u128(&mut self, field: &Field, value: u128) {
        match i64::try_from(value) {
            Ok(value) => self.value(field, value),
            Err(_) => self.value(field, value.to_string()),
        }
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.value(field, value);
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.value(field, value);
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.value(field, value.to_owned());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.value(field, format!("{value:?}"));
    }
    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        self.value(field, value.to_string());
    }
}

fn metadata_fields(metadata: &'static Metadata<'static>, attributes: &mut Vec<SpanAttribute>) {
    attributes.push(SpanAttribute::new(
        "fast_telemetry.tracing.level",
        metadata.level().as_str(),
    ));
    attributes.push(SpanAttribute::new(
        "fast_telemetry.tracing.target",
        metadata.target(),
    ));
    if let Some(module) = metadata.module_path() {
        attributes.push(SpanAttribute::new("fast_telemetry.code.module", module));
    }
    if let Some(file) = metadata.file() {
        attributes.push(SpanAttribute::new("fast_telemetry.code.file", file));
    }
    if let Some(line) = metadata.line() {
        attributes.push(SpanAttribute::new(
            "fast_telemetry.code.line",
            i64::from(line),
        ));
    }
}

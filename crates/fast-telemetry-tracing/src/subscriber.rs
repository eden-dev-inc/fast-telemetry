use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::hash_map::{DefaultHasher, Entry};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use fast_telemetry::{CompletedSpan, SpanCollector, SpanId, SpanKind, SpanStatus, TraceId};
use parking_lot::Mutex;
use thread_local::ThreadLocal;
use tracing::dispatcher::{Dispatch, WeakDispatch};
use tracing::span::{Attributes, Id, Record};
use tracing::subscriber::Interest;
use tracing::{Event, Metadata, Subscriber};
use tracing_core::span::Current;

use super::{
    Fields, LayerHandle, NEXT_LAYER_ID, RecordedEvent, Shared, SpanAttribute, SpanEvent, State,
    metadata_fields, now_ns, upsert,
};

/// Automatic metadata retained by [`FastTelemetrySubscriber`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum MetadataPolicy {
    /// Keep tracing level, target, module, file, and line, like the layer.
    #[default]
    Full,
    /// Keep user fields without automatic metadata attributes. Span and event
    /// names, timestamps, IDs, links, and field conversion are unchanged.
    UserFields,
}

/// A direct tracing subscriber without Registry or boxed span extensions.
///
/// This always-enabled backend supports tracing spans, events, `Instrument`,
/// and propagation through [`LayerHandle`]. Use [`super::FastTelemetryLayer`]
/// when composing Registry-dependent layers, extensions, or per-layer filters.
/// Completed spans use the same owning, immediate, bounded sink as the layer.
/// Install by value as a standalone subscriber. Arbitrary subscriber wrappers
/// and `Arc<Self>` combined with outer layer callbacks are outside this direct
/// installation contract; use the composable layer for those integrations.
pub struct FastTelemetrySubscriber {
    pub(super) shared: Arc<Shared>,
    metadata: MetadataPolicy,
    spans: [Shard; SHARDS],
    entered: ThreadLocal<RefCell<Entered>>,
    dispatch: OnceLock<WeakDispatch>,
    closing: ThreadLocal<Closing>,
}

const SHARDS: usize = 64;
const ID_BLOCK: u64 = 1024;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Default)]
struct IdBlock {
    next: u64,
    end: u64,
}

impl IdBlock {
    fn take(&mut self, allocator: &AtomicU64) -> u64 {
        if self.next == self.end {
            let mut start = allocator.load(Ordering::Relaxed);
            loop {
                let end = start
                    .checked_add(ID_BLOCK)
                    .expect("tracing span identity space exhausted");
                match allocator.compare_exchange_weak(
                    start,
                    end,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.next = start;
                        self.end = end;
                        break;
                    }
                    Err(current) => start = current,
                }
            }
        }
        let id = self.next;
        self.next += 1;
        id
    }
}

thread_local! {
    // The allocator owns only integers, never subscriber/span payloads. Blocks
    // are shared across subscriber instances on this thread and never recycled.
    static IDS: RefCell<IdBlock> = const { RefCell::new(IdBlock { next: 0, end: 0 }) };
}

fn next_id() -> u64 {
    IDS.with(|ids| ids.borrow_mut().take(&NEXT_ID))
}

type SpanMap = HashMap<u64, LiveSpan, BuildHasherDefault<SpanIdHasher>>;

// Only the checked ID allocator inserts keys into this private map. All lookups
// still compare the complete u64 key, including raw Subscriber API calls.
#[derive(Default)]
struct SpanIdHasher(u64);

impl Hasher for SpanIdHasher {
    #[inline]
    fn write_u64(&mut self, id: u64) {
        self.0 = id;
    }

    fn write(&mut self, bytes: &[u8]) {
        // u64 keys use write_u64. Keep a general, non-panicking byte fallback
        // without adding its state or work to the integer-key path.
        let mut fallback = DefaultHasher::new();
        fallback.write_u64(self.0);
        fallback.write(bytes);
        self.0 = fallback.finish();
    }

    #[inline]
    fn finish(&self) -> u64 {
        // SplitMix64 output finalizer by Sebastiano Vigna, public domain:
        // https://prng.di.unimi.it/splitmix64.c
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}

// Shard cache lines are independent. HashMap storage grows with live spans and
// retains capacity for reuse; every payload is owned by this subscriber, so
// dropping it also reclaims spans left open by raw Subscriber API callers.
#[repr(align(64))]
#[derive(Default)]
struct Shard(Mutex<SpanMap>);

struct LiveSpan {
    metadata: &'static Metadata<'static>,
    parent: Option<Id>,
    refs: usize,
    state: State,
}

#[derive(Default)]
struct Entered(Vec<(Id, bool)>);

#[derive(Default)]
struct Closing {
    active: Cell<bool>,
    pending: RefCell<Vec<Id>>,
}

struct CloseGuard<'a>(&'a Closing);

impl Drop for CloseGuard<'_> {
    fn drop(&mut self) {
        self.0.active.set(false);
    }
}

impl Entered {
    fn push(&mut self, id: &Id) {
        let duplicate = self.0.iter().any(|(entered, _)| entered == id);
        self.0.push((id.clone(), duplicate));
    }

    fn pop(&mut self, id: &Id) {
        if let Some(index) = self.0.iter().rposition(|(entered, _)| entered == id) {
            self.0.remove(index);
        }
    }

    fn current(&self) -> Option<&Id> {
        self.0
            .iter()
            .rev()
            .find_map(|(id, duplicate)| (!duplicate).then_some(id))
    }
}

impl FastTelemetrySubscriber {
    /// Use full metadata and queues of 4096 completed spans/standalone events.
    pub fn new(collector: Arc<SpanCollector>) -> Self {
        Self::with_capacity(collector, 4096, 4096)
    }

    /// Configure two nonzero, independent item limits, without sampling.
    /// Newest completed items are dropped at capacity and counted by the handle.
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
            metadata: MetadataPolicy::Full,
            spans: std::array::from_fn(|_| Shard::default()),
            entered: ThreadLocal::new(),
            dispatch: OnceLock::new(),
            closing: ThreadLocal::new(),
        }
    }

    /// Explicitly select whether automatic metadata attributes are retained.
    pub fn with_metadata_policy(mut self, policy: MetadataPolicy) -> Self {
        self.metadata = policy;
        self
    }

    /// Own propagation access, queue counters, and the standalone-event drain.
    pub fn handle(&self) -> LayerHandle {
        LayerHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    fn shard(&self, id: &Id) -> &Mutex<SpanMap> {
        // Keep each thread-local allocation block on one shard so successive
        // spans reuse its map's cache lines. Blocks spread across the shards,
        // with modulo collisions protected by the same locks. The mapping
        // depends only on the ID, including when a span moves between threads.
        let index = ((id.into_u64() - 1) / ID_BLOCK) & (SHARDS as u64 - 1);
        &self.spans[index as usize].0
    }

    fn current_id(&self) -> Option<Id> {
        self.entered
            .get()
            .and_then(|entered| entered.borrow().current().cloned())
    }

    fn fields(&self, metadata: &'static Metadata<'static>, count: usize) -> Fields {
        match self.metadata {
            MetadataPolicy::Full => Fields::with_metadata(metadata, count),
            MetadataPolicy::UserFields => Fields(Vec::with_capacity(count)),
        }
    }

    fn metadata_fields(&self, metadata: &'static Metadata<'static>, fields: &mut Fields) {
        if self.metadata == MetadataPolicy::Full {
            metadata_fields(metadata, &mut fields.0);
        }
    }

    pub(super) fn with_state(&self, id: &Id, f: &mut dyn FnMut(&mut State)) {
        if let Some(span) = self.shard(id).lock().get_mut(&id.into_u64()) {
            f(&mut span.state);
        }
    }

    fn retain(span: &mut LiveSpan) {
        span.refs = span
            .refs
            .checked_add(1)
            .expect("span reference count overflow");
    }

    fn close(&self, id: Id) -> bool {
        let removed = {
            let mut spans = self.shard(&id).lock();
            let Entry::Occupied(mut span) = spans.entry(id.into_u64()) else {
                return false;
            };
            if span.get().refs > 1 {
                span.get_mut().refs -= 1;
                return false;
            }
            span.remove()
        };
        // No map/TLS borrow survives publication or payload destruction.
        let mut state = removed.state;
        state.span.end_time_ns = now_ns();
        let _ = self.shared.sink.publish(state.span);
        if let Some(parent) = removed.parent {
            self.release_parent(parent);
        }
        true
    }

    fn release_parent(&self, id: Id) {
        {
            let mut spans = self.shard(&id).lock();
            let Some(parent) = spans.get_mut(&id.into_u64()) else {
                return;
            };
            if parent.refs > 1 {
                parent.refs -= 1;
                return;
            }
        }
        // Actual final parent closure is cold when the application keeps its
        // parent Span alive. Route it through the owning dispatch so outer
        // layers receive on_close too. The queue bounds recursion in deep
        // ancestor chains and is never borrowed while invoking a callback.
        let closing = self.closing.get_or_default();
        closing.pending.borrow_mut().push(id);
        if closing.active.replace(true) {
            return;
        }
        let _guard = CloseGuard(closing);
        let dispatch = self.dispatch.get().and_then(WeakDispatch::upgrade);
        loop {
            let Some(id) = closing.pending.borrow_mut().pop() else {
                break;
            };
            if let Some(dispatch) = &dispatch {
                dispatch.try_close(id);
            } else {
                // Raw Subscriber API use has no outer dispatch to notify.
                self.close(id);
            }
        }
    }
}

impl Subscriber for FastTelemetrySubscriber {
    fn on_register_dispatch(&self, dispatch: &Dispatch) {
        let _ = self.dispatch.set(dispatch.downgrade());
    }

    fn register_callsite(&self, _: &'static Metadata<'static>) -> Interest {
        Interest::always()
    }

    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        // Arbitrary Debug/Display may emit tracing. Visit before all locks and
        // TLS borrows, including when a formatter creates or enters spans.
        let mut fields = self.fields(attrs.metadata(), attrs.values().len());
        attrs.record(&mut fields);
        self.metadata_fields(attrs.metadata(), &mut fields);
        let id = Id::from_u64(next_id());
        let parent_id = if attrs.is_contextual() {
            self.current_id()
        } else {
            attrs.parent().cloned()
        };
        let parent = parent_id.and_then(|id| {
            let mut spans = self.shard(&id).lock();
            let span = spans.get_mut(&id.into_u64())?;
            Self::retain(span);
            Some((id, span.state.context()))
        });
        let context = parent.as_ref().map(|(_, context)| context);
        let state = State {
            trace_flags: context.map_or(1, |parent| parent.trace_flags),
            span: CompletedSpan {
                trace_id: context.map_or_else(TraceId::random, |parent| parent.trace_id),
                span_id: SpanId::random(),
                parent_span_id: context.map_or(SpanId::INVALID, |parent| parent.span_id),
                name: Cow::Borrowed(attrs.metadata().name()),
                kind: SpanKind::Internal,
                start_time_ns: now_ns(),
                end_time_ns: 0,
                status: SpanStatus::Unset,
                attributes: fields.0,
                events: Vec::new(),
            },
        };
        self.shard(&id).lock().insert(
            id.into_u64(),
            LiveSpan {
                metadata: attrs.metadata(),
                parent: parent.map(|(id, _)| id),
                refs: 1,
                state,
            },
        );
        id
    }

    fn record(&self, id: &Id, record: &Record<'_>) {
        let mut fields = Fields(Vec::with_capacity(record.len()));
        record.record(&mut fields);
        if let Some(span) = self.shard(id).lock().get_mut(&id.into_u64()) {
            for field in fields.0 {
                upsert(&mut span.state.span.attributes, field);
            }
        }
    }

    fn record_follows_from(&self, id: &Id, follows: &Id) {
        let target = self
            .shard(follows)
            .lock()
            .get(&follows.into_u64())
            .map(|span| span.state.context());
        if let Some(target) = target {
            let event = SpanEvent {
                name: Cow::Borrowed("fast_telemetry.follows_from"),
                time_ns: now_ns(),
                attributes: vec![
                    SpanAttribute::new("fast_telemetry.link.trace_id", target.trace_id.to_string()),
                    SpanAttribute::new("fast_telemetry.link.span_id", target.span_id.to_string()),
                ],
            };
            if let Some(span) = self.shard(id).lock().get_mut(&id.into_u64()) {
                span.state.span.events.push(event);
            }
        }
    }

    fn event(&self, event: &Event<'_>) {
        let mut fields = self.fields(event.metadata(), event.metadata().fields().len());
        event.record(&mut fields);
        self.metadata_fields(event.metadata(), &mut fields);
        let mut recorded = Some(SpanEvent {
            name: Cow::Borrowed(event.metadata().name()),
            time_ns: now_ns(),
            attributes: fields.0,
        });
        let parent = if event.is_contextual() {
            self.current_id()
        } else {
            event.parent().cloned()
        };
        if let Some(id) = parent {
            let mut spans = self.shard(&id).lock();
            if let Some(span) = spans.get_mut(&id.into_u64()) {
                span.state.span.events.push(recorded.take().unwrap());
            }
        }
        let Some(recorded) = recorded else {
            return;
        };
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

    fn enter(&self, id: &Id) {
        if self.shard(id).lock().contains_key(&id.into_u64()) {
            self.entered.get_or_default().borrow_mut().push(id);
        }
    }

    fn exit(&self, id: &Id) {
        if let Some(entered) = self.entered.get() {
            entered.borrow_mut().pop(id);
        }
    }

    fn clone_span(&self, id: &Id) -> Id {
        if let Some(span) = self.shard(id).lock().get_mut(&id.into_u64()) {
            Self::retain(span);
        }
        id.clone()
    }

    fn try_close(&self, id: Id) -> bool {
        self.close(id)
    }

    fn current_span(&self) -> Current {
        self.current_id()
            .and_then(|id| {
                let spans = self.shard(&id).lock();
                let span = spans.get(&id.into_u64())?;
                Some(Current::new(id, span.metadata))
            })
            .unwrap_or_else(Current::none)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_blocks_fail_before_wrap_and_do_not_reuse_partial_blocks() {
        let allocator = AtomicU64::new(u64::MAX - ID_BLOCK);
        let mut block = IdBlock::default();
        for expected in u64::MAX - ID_BLOCK..u64::MAX {
            assert_eq!(block.take(&allocator), expected);
        }
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| { block.take(&allocator) }))
                .is_err()
        );
        assert_eq!(allocator.load(Ordering::Relaxed), u64::MAX);

        let allocator = AtomicU64::new(1);
        let first = IdBlock::default().take(&allocator);
        let second = IdBlock::default().take(&allocator);
        assert_eq!(first, 1);
        assert_eq!(second, ID_BLOCK + 1);
    }
}

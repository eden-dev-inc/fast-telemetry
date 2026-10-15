use std::collections::HashSet;
use std::sync::{Arc, Barrier, Mutex};

use fast_telemetry::{CompletedSpan, SpanCollector};
use fast_telemetry_tracing::{FastTelemetrySubscriber, MetadataPolicy};
use tracing::span::{Attributes, Id, Record};
use tracing::{Callsite as _, Dispatch, Subscriber, info, info_span};
use tracing_subscriber::layer::SubscriberExt;

fn drain(collector: &SpanCollector) -> Vec<CompletedSpan> {
    let mut spans = Vec::new();
    collector.drain_into(&mut spans);
    spans
}

fn metadata() -> &'static tracing::Metadata<'static> {
    tracing::callsite! {
        name: "raw", kind: tracing::metadata::Kind::SPAN,
        level: tracing::Level::INFO, fields: value
    }
    .metadata()
}

fn event_metadata() -> &'static tracing::Metadata<'static> {
    tracing::callsite! {
        name: "raw event", kind: tracing::metadata::Kind::EVENT,
        level: tracing::Level::INFO, fields: value
    }
    .metadata()
}

#[test]
fn entered_duplicates_and_out_of_order_exits_match_registry() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let subscriber = FastTelemetrySubscriber::new(collector.clone());
    tracing::subscriber::with_default(subscriber, || {
        let a = info_span!(parent: None, "a");
        let b = info_span!(parent: None, "b");
        let first_a = a.enter();
        let entered_b = b.enter();
        let second_a = a.enter();
        assert_eq!(tracing::Span::current().id(), b.id());
        drop(first_a);
        assert_eq!(tracing::Span::current().id(), b.id());
        drop(info_span!("on b"));
        drop(entered_b);
        // Registry marks re-entries as duplicates of the original entry. An
        // out-of-order exit of that original does not promote the duplicate.
        assert!(tracing::Span::current().is_disabled());
        drop(info_span!("outside"));
        drop(second_a);
        assert!(tracing::Span::current().is_disabled());
    });
    let spans = drain(&collector);
    let b = spans.iter().find(|s| s.name == "b").unwrap();
    assert_eq!(
        spans
            .iter()
            .find(|s| s.name == "on b")
            .unwrap()
            .parent_span_id,
        b.span_id
    );
    assert!(
        spans
            .iter()
            .find(|s| s.name == "outside")
            .unwrap()
            .parent_span_id
            .is_invalid()
    );
}

#[test]
fn foreign_stale_and_unknown_raw_ids_are_harmless_and_never_reissued() {
    let first_collector = Arc::new(SpanCollector::new(1, 1));
    let second_collector = Arc::new(SpanCollector::new(1, 1));
    let first = FastTelemetrySubscriber::with_capacity(first_collector, 8192, 4);
    let second = FastTelemetrySubscriber::new(second_collector.clone());
    let handle = second.handle();
    let metadata = metadata();
    let empty = metadata.fields().value_set_all(&[None]);
    let attrs = Attributes::new_root(metadata, &empty);
    let id = first.new_span(&attrs);
    let other = second.new_span(&attrs);
    let value = 17i64;
    let values = [Some(&value as &dyn tracing::field::Value)];
    let values = metadata.fields().value_set_all(&values);
    let record = Record::new(&values);
    let event_metadata = event_metadata();
    let event_values = event_metadata.fields().value_set_all(&[None]);
    for invalid in [&id, &Id::from_u64(u64::MAX)] {
        second.record(invalid, &record);
        second.record_follows_from(invalid, &other);
        second.record_follows_from(&other, invalid);
        second.enter(invalid);
        assert!(second.current_span().id().is_none());
        second.exit(invalid);
        assert_eq!(second.clone_span(invalid), *invalid);
        assert!(!second.try_close(invalid.clone()));
        second.event(&tracing::Event::new_child_of(
            Some(invalid.clone()),
            event_metadata,
            &event_values,
        ));
        let attrs = Attributes::child_of(invalid.clone(), metadata, &empty);
        let independent = second.new_span(&attrs);
        assert!(second.try_close(independent));
    }
    assert_eq!(handle.accepted_event_count(), 2);
    assert_eq!(handle.dropped_event_count(), 0);
    first.enter(&id);
    assert!(first.try_close(id.clone()));
    assert!(first.current_span().id().is_none());
    first.exit(&id);
    first.record(&id, &record);
    assert!(!first.try_close(id.clone()));
    let mut issued = HashSet::from([id.into_u64(), other.into_u64()]);
    for _ in 0..4096 {
        let next = first.new_span(&attrs);
        assert!(issued.insert(next.into_u64()));
        assert!(first.try_close(next));
    }
    assert!(second.try_close(other));
    assert!(
        drain(&second_collector)
            .iter()
            .all(|span| span.parent_span_id.is_invalid()
                && span.events.is_empty()
                && !span.attributes.iter().any(|a| a.key == "value"))
    );
}

#[test]
fn handle_uses_own_dispatch_and_subscriber_domain() {
    let a = FastTelemetrySubscriber::new(Arc::new(SpanCollector::new(1, 1)));
    let b = FastTelemetrySubscriber::new(Arc::new(SpanCollector::new(1, 1)));
    let handle_a = a.handle();
    let handle_b = b.handle();
    let a = Dispatch::new(a);
    let b = Dispatch::new(b);
    let span_a = tracing::dispatcher::with_default(&a, || info_span!("a"));
    let span_b = tracing::dispatcher::with_default(&b, || info_span!("b"));
    assert!(handle_a.context(&span_b).is_none());
    assert!(handle_b.context(&span_a).is_none());
    tracing::dispatcher::with_default(&b, || assert!(handle_a.context(&span_a).is_some()));
}

#[test]
fn clones_racing_other_reference_drops_publish_exactly_once() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let dispatch = Dispatch::new(FastTelemetrySubscriber::new(collector.clone()));
    let span = tracing::dispatcher::with_default(&dispatch, || info_span!("racing"));
    let spans: Vec<_> = (0..8).map(|_| span.clone()).collect();
    drop(span);
    let barrier = Barrier::new(8);
    std::thread::scope(|scope| {
        for span in spans {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..256 {
                    drop(span.clone());
                }
                drop(span);
            });
        }
    });
    let spans = drain(&collector);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "racing");
}

#[derive(Clone)]
struct CloseObserver(Arc<Mutex<Vec<Id>>>);

impl<S: Subscriber> tracing_subscriber::Layer<S> for CloseObserver {
    fn on_close(&self, id: Id, _: tracing_subscriber::layer::Context<'_, S>) {
        self.0.lock().unwrap().push(id);
    }
}

#[test]
fn dropped_parents_close_iteratively_and_notify_by_value_outer_layers_once() {
    for shared in [false, true] {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let subscriber = FastTelemetrySubscriber::with_capacity(collector.clone(), 4096, 1);
        let handle = subscriber.handle();
        let closes = Arc::new(Mutex::new(Vec::new()));
        let dispatch = if shared {
            // Subscriber for Arc does not forward on_register_dispatch. The
            // documented standalone Arc path must still release parent refs.
            Dispatch::new(Arc::new(subscriber))
        } else {
            Dispatch::new(subscriber.with(CloseObserver(closes.clone())))
        };
        tracing::dispatcher::with_default(&dispatch, || {
            let mut parent = info_span!(parent: None, "root");
            for _ in 0..2048 {
                let child = info_span!(parent: &parent, "node");
                drop(parent);
                assert!(drain(&collector).is_empty());
                parent = child;
            }
            drop(parent);
        });
        let spans = drain(&collector);
        assert_eq!(spans.len(), 2049);
        assert_eq!(handle.accepted_span_count(), 2049);
        assert_eq!(handle.dropped_span_count(), 0);
        let by_id: std::collections::HashMap<_, _> = spans.iter().map(|s| (s.span_id, s)).collect();
        assert_eq!(by_id.len(), spans.len());
        for child in spans.iter().filter(|s| !s.parent_span_id.is_invalid()) {
            let parent = by_id[&child.parent_span_id];
            assert_eq!(child.trace_id, parent.trace_id);
            assert!(parent.end_time_ns >= child.end_time_ns);
        }
        if !shared {
            let closes = closes.lock().unwrap();
            assert_eq!(closes.len(), spans.len());
            assert_eq!(closes.iter().collect::<HashSet<_>>().len(), closes.len());
        }
    }
}

#[test]
fn reversed_concurrent_and_self_links_do_not_lock_two_shards() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let dispatch = Dispatch::new(FastTelemetrySubscriber::new(collector.clone()));
    let (a, b) =
        tracing::dispatcher::with_default(&dispatch, || (info_span!("a"), info_span!("b")));
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        for (span, other) in [(&a, &b), (&b, &a)] {
            let barrier = &barrier;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..128 {
                    span.follows_from(other.id());
                    span.follows_from(span.id());
                }
            });
        }
    });
    drop((a, b));
    let spans = drain(&collector);
    assert_eq!(spans.len(), 2);
    assert!(spans.iter().all(|span| span.events.len() == 256));
}

#[test]
fn formatter_panics_leave_parent_fields_and_lifecycle_usable() {
    struct Panics;
    impl std::fmt::Debug for Panics {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("format panic");
        }
    }
    let collector = Arc::new(SpanCollector::new(1, 1));
    tracing::subscriber::with_default(FastTelemetrySubscriber::new(collector.clone()), || {
        let parent = info_span!("parent", value = tracing::field::Empty);
        let _entered = parent.enter();
        for operation in 0..3 {
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match operation {
                    0 => drop(info_span!("failed", value = ?Panics)),
                    1 => {
                        parent.record("value", tracing::field::debug(Panics));
                    }
                    _ => info!(value = ?Panics, "failed"),
                }))
                .is_err()
            );
        }
        parent.record("value", 19i64);
        info!("after panic");
        drop(info_span!("child"));
    });
    let spans = drain(&collector);
    assert_eq!(spans.len(), 2);
    let parent = spans.iter().find(|s| s.name == "parent").unwrap();
    assert_eq!(parent.events.len(), 1);
    assert!(
        parent
            .attributes
            .iter()
            .any(|a| a.key == "value" && matches!(a.value, fast_telemetry::SpanValue::I64(19)))
    );
}

#[test]
fn user_fields_policy_is_explicit_and_keeps_user_payload_and_names() {
    for policy in [MetadataPolicy::Full, MetadataPolicy::UserFields] {
        let collector = Arc::new(SpanCollector::new(1, 1));
        let subscriber =
            FastTelemetrySubscriber::new(collector.clone()).with_metadata_policy(policy);
        tracing::subscriber::with_default(subscriber, || {
            let span = info_span!(
                "policy",
                value = u64::MAX,
                "fast_telemetry.tracing.level" = "user"
            );
            let _entered = span.enter();
            info!(ok = true, "checkpoint");
        });
        let spans = drain(&collector);
        assert_eq!(spans[0].name, "policy");
        assert_eq!(
            spans[0].attributes.len(),
            if policy == MetadataPolicy::Full { 7 } else { 2 }
        );
        assert!(
            spans[0]
                .attributes
                .iter()
                .any(|a| a.key == "fast_telemetry.user.fast_telemetry.tracing.level")
        );
        assert_eq!(
            spans[0].events[0].attributes.len(),
            if policy == MetadataPolicy::Full { 7 } else { 2 }
        );
        assert!(spans[0].events[0].name.starts_with("event "));
    }
}

#[test]
fn dropping_raw_unclosed_subscriber_releases_collector_without_publishing_partial_spans() {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let weak = Arc::downgrade(&collector);
    let subscriber = FastTelemetrySubscriber::new(collector.clone());
    let metadata = metadata();
    let empty = metadata.fields().value_set_all(&[None]);
    let _unclosed = subscriber.new_span(&Attributes::new_root(metadata, &empty));
    assert!(drain(&collector).is_empty());
    drop(collector);
    assert!(weak.upgrade().is_some());
    drop(subscriber);
    assert!(weak.upgrade().is_none());
}

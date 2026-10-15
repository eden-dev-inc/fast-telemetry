use std::sync::Arc;

use fast_telemetry::{CompletedSpan, SpanAttribute, SpanCollector, SpanContext, SpanValue};
use fast_telemetry_tracing::{FastTelemetryLayer, FastTelemetrySubscriber, LayerHandle};
use tracing::{Instrument, info, info_span};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{Layer, Registry};

#[derive(Clone, Copy)]
enum TestBackend {
    Layer,
    Direct,
}

impl TestBackend {
    fn install(
        self,
        collector: Arc<SpanCollector>,
        spans: usize,
        events: usize,
    ) -> (tracing::Dispatch, LayerHandle) {
        match self {
            Self::Layer => {
                let layer = FastTelemetryLayer::with_capacity(collector, spans, events);
                let handle = layer.handle();
                (
                    tracing::Dispatch::new(Registry::default().with(layer)),
                    handle,
                )
            }
            Self::Direct => {
                let subscriber = FastTelemetrySubscriber::with_capacity(collector, spans, events);
                let handle = subscriber.handle();
                (tracing::Dispatch::new(subscriber), handle)
            }
        }
    }
}

fn contract(test: impl Fn(TestBackend)) {
    for backend in [TestBackend::Layer, TestBackend::Direct] {
        test(backend);
    }
}

fn drain(collector: &SpanCollector) -> Vec<CompletedSpan> {
    let mut output = Vec::new();
    // Deliberately no flush_local: the adapter must publish low-volume spans.
    collector.drain_into(&mut output);
    output
}

fn attr<'a>(attributes: &'a [SpanAttribute], key: &str) -> &'a SpanValue {
    &attributes
        .iter()
        .find(|a| a.key == key)
        .unwrap_or_else(|| panic!("missing {key}"))
        .value
}

fn text(value: &SpanValue) -> &str {
    match value {
        SpanValue::String(s) => s,
        _ => panic!("expected string"),
    }
}

#[test]
fn parents_events_and_late_fields_keep_frontend_semantics() {
    contract(parents_events_and_late_fields_keep_frontend_semantics_contract);
}

fn parents_events_and_late_fields_keep_frontend_semantics_contract(backend: TestBackend) {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, handle) = backend.install(collector.clone(), 4096, 4096);
    tracing::dispatcher::with_default(&dispatch, || {
        let root = info_span!("root", late = tracing::field::Empty, value = 1i64);
        root.record("late", "ready");
        root.record("value", 2i64);
        let _entered = root.enter();
        let child = info_span!("contextual");
        let explicit = info_span!(parent: &root, "explicit");
        let independent = info_span!(parent: None, "independent");
        let _child_entered = child.enter();
        info!("on child");
        info!(parent: &explicit, "on explicit");
        info!(parent: None, "standalone inside child");
        assert!(fast_telemetry::current_span_id().is_none());
        assert!(handle.traceparent(&root).is_some());
        drop(independent);
    });
    let spans = drain(&collector);
    assert_eq!(spans.len(), 4);
    let root = spans.iter().find(|s| s.name == "root").unwrap();
    let child = spans.iter().find(|s| s.name == "contextual").unwrap();
    let explicit = spans.iter().find(|s| s.name == "explicit").unwrap();
    let independent = spans.iter().find(|s| s.name == "independent").unwrap();
    assert!(root.parent_span_id.is_invalid());
    for span in [child, explicit] {
        assert_eq!(span.parent_span_id, root.span_id);
        assert_eq!(span.trace_id, root.trace_id);
        assert_eq!(span.events.len(), 1);
    }
    assert!(independent.parent_span_id.is_invalid());
    assert_ne!(independent.trace_id, root.trace_id);
    assert_eq!(text(attr(&root.attributes, "late")), "ready");
    assert!(matches!(attr(&root.attributes, "value"), SpanValue::I64(2)));
    assert_eq!(
        root.attributes.iter().filter(|a| a.key == "value").count(),
        1
    );
    let mut events = Vec::new();
    handle.drain_events(&mut events);
    assert_eq!(events.len(), 1);
    assert_eq!(
        text(attr(&events[0].event.attributes, "message")),
        "standalone inside child"
    );
    assert_eq!(handle.dropped_span_count(), 0);
}

#[test]
fn clone_final_close_and_concurrent_reentry_publish_once() {
    contract(clone_final_close_and_concurrent_reentry_publish_once_contract);
}

fn clone_final_close_and_concurrent_reentry_publish_once_contract(backend: TestBackend) {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, _) = backend.install(collector.clone(), 4096, 4096);
    let span = tracing::dispatcher::with_default(&dispatch, || info_span!("shared"));
    let keep_alive = span.clone();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let span = span.clone();
            let dispatch = dispatch.clone();
            scope.spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    for _ in 0..8 {
                        let _outer = span.enter();
                        let _inner = span.enter();
                        info!("concurrent");
                    }
                })
            });
        }
    });
    drop(span);
    assert!(drain(&collector).is_empty());
    drop(keep_alive);
    let spans = drain(&collector);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].events.len(), 32);
}

#[test]
fn sibling_nested_and_multiple_layer_state_is_isolated() {
    let first = Arc::new(SpanCollector::new(1, 1));
    let second = Arc::new(SpanCollector::new(1, 1));
    let layer1 = FastTelemetryLayer::new(first.clone());
    let handle1 = layer1.handle();
    let layer2 = FastTelemetryLayer::new(second.clone());
    let handle2 = layer2.handle();
    let subscriber = Registry::default().with(layer1).with(layer2);
    tracing::subscriber::with_default(subscriber, || {
        let root = info_span!("root");
        assert_ne!(
            handle1.context(&root).unwrap().span_id,
            handle2.context(&root).unwrap().span_id
        );
        let _root = root.enter();
        {
            let child = info_span!("nested");
            let _child = child.enter();
            let grandchild = info_span!("grandchild");
            drop(grandchild);
        }
        let sibling = info_span!("sibling");
        drop(sibling);
    });
    for collector in [&first, &second] {
        let spans = drain(collector);
        assert_eq!(spans.len(), 4);
        let root = spans.iter().find(|s| s.name == "root").unwrap();
        let nested = spans.iter().find(|s| s.name == "nested").unwrap();
        let grandchild = spans.iter().find(|s| s.name == "grandchild").unwrap();
        let sibling = spans.iter().find(|s| s.name == "sibling").unwrap();
        assert_eq!(grandchild.parent_span_id, nested.span_id);
        assert_eq!(sibling.parent_span_id, root.span_id);
    }
}

#[test]
fn three_filtered_layers_keep_context_fields_and_close_state_independent() {
    let collectors: [_; 3] = std::array::from_fn(|_| Arc::new(SpanCollector::new(1, 1)));
    let first = FastTelemetryLayer::new(collectors[0].clone());
    let second = FastTelemetryLayer::new(collectors[1].clone());
    let third = FastTelemetryLayer::new(collectors[2].clone());
    let handles = [first.handle(), second.handle(), third.handle()];
    let subscriber = Registry::default()
        .with(first.with_filter(LevelFilter::INFO))
        .with(second.with_filter(LevelFilter::WARN))
        .with(third.with_filter(LevelFilter::DEBUG));
    tracing::subscriber::with_default(subscriber, || {
        let root = info_span!("root");
        assert!(handles[1].context(&root).is_none());
        let child = tracing::warn_span!(parent: &root, "shared", late = tracing::field::Empty);
        let contexts = handles
            .each_ref()
            .map(|handle| handle.context(&child).unwrap());
        for (index, context) in contexts.iter().enumerate() {
            for other in &contexts[index + 1..] {
                assert_ne!(context.span_id, other.span_id);
                assert_ne!(context.trace_id, other.trace_id);
            }
        }
        child.record("late", 7i64);
        tracing::warn!(parent: &child, value = 9i64, "shared event");
        let grandchild = tracing::debug_span!(parent: &child, "debug child");
        drop(grandchild);
        drop(child);
        drop(root);
    });
    for (index, collector) in collectors.iter().enumerate() {
        let spans = drain(collector);
        assert_eq!(spans.len(), [2, 1, 3][index]);
        let child = spans.iter().find(|span| span.name == "shared").unwrap();
        assert!(matches!(attr(&child.attributes, "late"), SpanValue::I64(7)));
        assert_eq!(child.events.len(), 1);
        assert!(matches!(
            attr(&child.events[0].attributes, "value"),
            SpanValue::I64(9)
        ));
        if index == 1 {
            assert!(child.parent_span_id.is_invalid());
        } else {
            let root = spans.iter().find(|span| span.name == "root").unwrap();
            assert_eq!(child.parent_span_id, root.span_id);
            assert_eq!(child.trace_id, root.trace_id);
        }
        if index == 2 {
            let grandchild = spans
                .iter()
                .find(|span| span.name == "debug child")
                .unwrap();
            assert_eq!(grandchild.parent_span_id, child.span_id);
            assert_eq!(grandchild.trace_id, child.trace_id);
        }
        assert_eq!(handles[index].accepted_span_count(), spans.len() as u64);
        assert_eq!(handles[index].dropped_span_count(), 0);
    }
}

#[test]
fn concurrent_close_has_one_global_capacity_and_is_visible_before_workers_exit() {
    contract(concurrent_close_has_one_global_capacity_and_is_visible_before_workers_exit_contract);
}

fn concurrent_close_has_one_global_capacity_and_is_visible_before_workers_exit_contract(
    backend: TestBackend,
) {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, handle) = backend.install(collector.clone(), 5, 1);
    let published = std::sync::Barrier::new(9);
    let drained = std::sync::Barrier::new(9);
    let mut rounds = Vec::new();
    std::thread::scope(|scope| {
        for worker in 0..8 {
            let dispatch = dispatch.clone();
            let published = &published;
            let drained = &drained;
            scope.spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    for round in 0..2 {
                        for item in 0..2 {
                            let span = info_span!(parent: None, "concurrent", worker, round, item);
                            drop(span);
                        }
                        published.wait();
                        drained.wait();
                    }
                });
            });
        }
        for _ in 0..2 {
            published.wait();
            // Every worker is alive and blocked, with no TLS flush or exit.
            let spans = drain(&collector);
            let accepted = handle.accepted_span_count();
            let dropped = handle.dropped_span_count();
            let empty = collector.is_empty();
            rounds.push((spans, accepted, dropped, empty));
            drained.wait();
        }
    });
    for (index, (spans, accepted, dropped, empty)) in rounds.into_iter().enumerate() {
        assert_eq!(spans.len(), 5);
        assert_eq!(accepted, (index as u64 + 1) * 5);
        assert_eq!(dropped, (index as u64 + 1) * 11);
        assert!(empty);
        for span in &spans {
            assert_eq!(span.name, "concurrent");
            assert!(matches!(
                attr(&span.attributes, "worker"),
                SpanValue::I64(0..=7)
            ));
            assert!(matches!(
                attr(&span.attributes, "item"),
                SpanValue::I64(0..=1)
            ));
            assert!(
                matches!(attr(&span.attributes, "round"), SpanValue::I64(round) if *round == index as i64)
            );
        }
    }
}

#[test]
fn handle_uses_spans_own_dispatch_and_filters_compose() {
    let a = Arc::new(SpanCollector::new(1, 1));
    let b = Arc::new(SpanCollector::new(1, 1));
    let layer_a = FastTelemetryLayer::new(a.clone());
    let handle_a = layer_a.handle();
    let layer_b = FastTelemetryLayer::new(b.clone());
    let handle_b = layer_b.handle();
    let dispatch_a =
        tracing::Dispatch::new(Registry::default().with(layer_a.with_filter(LevelFilter::INFO)));
    let dispatch_b = tracing::Dispatch::new(Registry::default().with(layer_b));
    let span_a = tracing::dispatcher::with_default(&dispatch_a, || info_span!("a"));
    let span_b = tracing::dispatcher::with_default(&dispatch_b, || info_span!("b"));
    assert!(handle_a.context(&span_b).is_none());
    assert!(handle_b.context(&span_a).is_none());
    tracing::dispatcher::with_default(&dispatch_b, || assert!(handle_a.context(&span_a).is_some()));
    drop(span_a);
    drop(span_b);
    assert_eq!(drain(&a).len(), 1);
    assert_eq!(drain(&b).len(), 1);

    let filtered = Arc::new(SpanCollector::new(1, 1));
    let all = Arc::new(SpanCollector::new(1, 1));
    let layer = FastTelemetryLayer::new(filtered.clone());
    let filtered_handle = layer.handle();
    let subscriber = Registry::default()
        .with(layer.with_filter(LevelFilter::INFO))
        .with(FastTelemetryLayer::new(all.clone()));
    tracing::subscriber::with_default(subscriber, || {
        let excluded = tracing::debug_span!("excluded");
        assert!(filtered_handle.context(&excluded).is_none());
        let _entered = excluded.enter();
        info!("retained even with excluded parent");
        let child = info_span!("included");
        drop(child);
    });
    assert_eq!(drain(&filtered).len(), 1);
    assert_eq!(drain(&all).len(), 2);
    let mut events = Vec::new();
    filtered_handle.drain_events(&mut events);
    assert_eq!(events.len(), 1);
}

#[test]
fn integer_debug_display_and_metadata_keys_are_lossless() {
    contract(integer_debug_display_and_metadata_keys_are_lossless_contract);
}

fn integer_debug_display_and_metadata_keys_are_lossless_contract(backend: TestBackend) {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, _) = backend.install(collector.clone(), 4096, 4096);
    tracing::dispatcher::with_default(&dispatch, || {
        let span = info_span!(target: "custom_target", "values",
            small = 42u64, large = u64::MAX, huge = u128::MAX, negative = i128::MIN,
            debug = ?vec![1, 2], display = %"hello", flag = true, ratio = 1.5f64,
            "fast_telemetry.tracing.level" = "user level",
            "fast_telemetry.user.fast_telemetry.tracing.level" = "other user level");
        drop(span);
    });
    let spans = drain(&collector);
    let attributes = &spans[0].attributes;
    assert!(matches!(attr(attributes, "small"), SpanValue::I64(42)));
    assert_eq!(text(attr(attributes, "large")), u64::MAX.to_string());
    assert_eq!(text(attr(attributes, "huge")), u128::MAX.to_string());
    assert_eq!(text(attr(attributes, "negative")), i128::MIN.to_string());
    assert_eq!(text(attr(attributes, "debug")), "[1, 2]");
    assert_eq!(text(attr(attributes, "display")), "hello");
    assert!(matches!(attr(attributes, "flag"), SpanValue::Bool(true)));
    assert!(matches!(attr(attributes, "ratio"), SpanValue::F64(v) if *v == 1.5));
    assert_eq!(
        text(attr(attributes, "fast_telemetry.tracing.target")),
        "custom_target"
    );
    assert_eq!(
        text(attr(attributes, "fast_telemetry.tracing.level")),
        "INFO"
    );
    assert_eq!(
        text(attr(
            attributes,
            "fast_telemetry.user.fast_telemetry.tracing.level"
        )),
        "user level"
    );
    assert_eq!(
        text(attr(
            attributes,
            "fast_telemetry.user.fast_telemetry.user.fast_telemetry.tracing.level"
        )),
        "other user level"
    );
    assert!(matches!(attr(attributes, "fast_telemetry.code.line"), SpanValue::I64(v) if *v > 0));
}

#[test]
fn static_metadata_is_borrowed_and_dynamic_user_strings_outlive_recording() {
    contract(static_metadata_is_borrowed_and_dynamic_user_strings_outlive_recording_contract);
}

fn static_metadata_is_borrowed_and_dynamic_user_strings_outlive_recording_contract(
    backend: TestBackend,
) {
    use std::borrow::Cow;

    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, handle) = backend.install(collector.clone(), 4096, 4096);
    tracing::dispatcher::with_default(&dispatch, || {
        let value = String::from("dynamic user value");
        let span = info_span!(target: "static target", "metadata", user = value.as_str());
        info!(parent: &span, user = value.as_str(), "attached");
        drop(span);
        info!(parent: None, user = value.as_str(), "standalone");
    });
    let spans = drain(&collector);
    let mut events = Vec::new();
    handle.drain_events(&mut events);
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].events.len(), 1);
    assert_eq!(events.len(), 1);
    for attributes in [
        &spans[0].attributes,
        &spans[0].events[0].attributes,
        &events[0].event.attributes,
    ] {
        assert_eq!(text(attr(attributes, "user")), "dynamic user value");
        for key in [
            "fast_telemetry.tracing.level",
            "fast_telemetry.tracing.target",
            "fast_telemetry.code.module",
            "fast_telemetry.code.file",
        ] {
            assert!(matches!(
                attr(attributes, key),
                SpanValue::String(Cow::Borrowed(_))
            ));
        }
        assert_eq!(text(attr(attributes, "fast_telemetry.code.file")), file!());
        assert_eq!(
            text(attr(attributes, "fast_telemetry.code.module")),
            module_path!()
        );
        assert!(
            matches!(attr(attributes, "fast_telemetry.code.line"), SpanValue::I64(line) if *line > 0)
        );
    }
    assert_eq!(
        text(attr(&spans[0].attributes, "fast_telemetry.tracing.target")),
        "static target"
    );
}

#[test]
fn debug_formatting_can_emit_tracing_without_extension_lock_deadlock() {
    contract(debug_formatting_can_emit_tracing_without_extension_lock_deadlock_contract);
}

fn debug_formatting_can_emit_tracing_without_extension_lock_deadlock_contract(
    backend: TestBackend,
) {
    struct Reentrant;
    impl std::fmt::Debug for Reentrant {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            info!("from formatter");
            f.write_str("rendered")
        }
    }
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, _) = backend.install(collector.clone(), 4096, 4096);
    tracing::dispatcher::with_default(&dispatch, || {
        let parent = info_span!("parent", value = tracing::field::Empty);
        let _entered = parent.enter();
        parent.record("value", tracing::field::debug(Reentrant));
        info!(value = ?Reentrant, "outer");
        let child = info_span!("child", value = ?Reentrant);
        drop(child);
    });
    let spans = drain(&collector);
    assert_eq!(spans.len(), 2);
    let parent = spans.iter().find(|s| s.name == "parent").unwrap();
    assert_eq!(text(attr(&parent.attributes, "value")), "rendered");
    assert!(
        parent
            .events
            .iter()
            .any(|event| text(attr(&event.attributes, "message")) == "outer")
    );
}

#[test]
fn propagation_links_and_capacity_are_observable() {
    contract(propagation_links_and_capacity_are_observable_contract);
}

fn propagation_links_and_capacity_are_observable_contract(backend: TestBackend) {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, handle) = backend.install(collector.clone(), 2, 1);
    let remote =
        SpanContext::from_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00")
            .unwrap();
    tracing::dispatcher::with_default(&dispatch, || {
        let root = info_span!(parent: None, "server");
        assert!(handle.set_parent(&root, remote));
        assert!(handle.traceparent(&root).unwrap().ends_with("-01"));
        let child = info_span!(parent: &root, "child");
        child.follows_from(root.id());
        child.follows_from(child.id());
        drop(child);
        drop(root);
        let dropped = info_span!(parent: None, "dropped");
        drop(dropped);
        info!(parent: None, "first");
        info!(parent: None, "dropped");
    });
    let spans = drain(&collector);
    assert_eq!(spans.len(), 2);
    let root = spans.iter().find(|s| s.name == "server").unwrap();
    let child = spans.iter().find(|s| s.name == "child").unwrap();
    assert_eq!(root.trace_id, remote.trace_id);
    assert_eq!(root.parent_span_id, remote.span_id);
    assert_eq!(child.trace_id, root.trace_id);
    assert_eq!(child.events.len(), 2);
    assert_eq!(
        text(attr(
            &child.events[0].attributes,
            "fast_telemetry.link.span_id"
        )),
        root.span_id.to_string()
    );
    assert_eq!(handle.accepted_span_count(), 2);
    assert_eq!(handle.dropped_span_count(), 1);
    assert_eq!(handle.accepted_event_count(), 1);
    assert_eq!(handle.dropped_event_count(), 1);
}

#[tracing::instrument]
async fn instrumented(value: u64) {
    tokio::task::yield_now().await;
    info!(value, "after await");
    let child = info_span!("async child");
    tokio::task::yield_now().instrument(child).await;
}

#[test]
fn instrumented_future_moves_threads_and_restores_siblings() {
    contract(instrumented_future_moves_threads_and_restores_siblings_contract);
}

fn instrumented_future_moves_threads_and_restores_siblings_contract(backend: TestBackend) {
    // Explicitly poll the same Instrument future on two OS threads. A runtime
    // scheduler is not required to happen to migrate it for this regression.
    struct OncePending(bool);
    impl std::future::Future for OncePending {
        type Output = ();
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            info!("poll");
            let child = info_span!("poll child");
            drop(child);
            if self.0 {
                std::task::Poll::Ready(())
            } else {
                self.0 = true;
                std::task::Poll::Pending
            }
        }
    }
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, _) = backend.install(collector.clone(), 4096, 4096);
    let span = tracing::dispatcher::with_default(&dispatch, || info_span!("moving"));
    let mut future = Box::pin(OncePending(false).instrument(span));
    for ready in [false, true] {
        let dispatch = dispatch.clone();
        future = std::thread::spawn(move || {
            tracing::dispatcher::with_default(&dispatch, || {
                let waker = std::task::Waker::noop();
                let mut context = std::task::Context::from_waker(waker);
                assert_eq!(future.as_mut().poll(&mut context).is_ready(), ready);
                assert!(tracing::Span::current().is_disabled());
                assert!(fast_telemetry::current_span_id().is_none());
                let sibling = info_span!("outside poll");
                drop(sibling);
            });
            future
        })
        .join()
        .unwrap();
    }
    drop(future);
    let spans = drain(&collector);
    assert_eq!(spans.len(), 5);
    let moving = spans.iter().find(|s| s.name == "moving").unwrap();
    assert_eq!(moving.events.len(), 2);
    for child in spans.iter().filter(|s| s.name == "poll child") {
        assert_eq!(child.parent_span_id, moving.span_id);
    }
    assert!(
        spans
            .iter()
            .filter(|s| s.name == "outside poll")
            .all(|s| s.parent_span_id.is_invalid())
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .build()
        .unwrap();
    use tracing::instrument::WithSubscriber;
    runtime.block_on(instrumented(7).with_subscriber(dispatch));
    let spans = drain(&collector);
    assert_eq!(spans.len(), 2);
    let parent = spans.iter().find(|s| s.name == "instrumented").unwrap();
    assert_eq!(parent.events.len(), 1);
    assert!(matches!(
        attr(&parent.attributes, "value"),
        SpanValue::I64(7)
    ));
}

#[test]
fn layer_and_live_span_keep_collector_owned_after_caller_drops_it() {
    contract(layer_and_live_span_keep_collector_owned_after_caller_drops_it_contract);
}

fn layer_and_live_span_keep_collector_owned_after_caller_drops_it_contract(backend: TestBackend) {
    let collector = Arc::new(SpanCollector::new(1, 1));
    let weak = Arc::downgrade(&collector);
    let (dispatch, _) = backend.install(collector.clone(), 4096, 4096);
    let span = tracing::dispatcher::with_default(&dispatch, || info_span!("owned"));
    drop(collector);
    drop(dispatch);
    assert!(weak.upgrade().is_some());
    drop(span);
    assert!(weak.upgrade().is_none());
}

#[cfg(feature = "otlp")]
#[test]
fn otlp_span_and_standalone_log_preserve_values_and_metadata() {
    contract(otlp_span_and_standalone_log_preserve_values_and_metadata_contract);
}

#[cfg(feature = "otlp")]
fn otlp_span_and_standalone_log_preserve_values_and_metadata_contract(backend: TestBackend) {
    use fast_telemetry::otlp::pb::any_value::Value;
    let collector = Arc::new(SpanCollector::new(1, 1));
    let (dispatch, handle) = backend.install(collector.clone(), 4096, 4096);
    tracing::dispatcher::with_default(&dispatch, || {
        let span = info_span!("encoded", large = u64::MAX);
        info!(parent: &span, ok = true, "child event");
        drop(span);
        tracing::warn!(parent: None, count = 3u64, "standalone");
    });
    let spans = drain(&collector);
    let proto = spans[0].to_otlp();
    assert_eq!(proto.events.len(), 1);
    assert!(proto.attributes.iter().any(|a| a.key == "large"
        && a.value.as_ref().unwrap().value == Some(Value::StringValue(u64::MAX.to_string()))));
    let mut events = Vec::new();
    handle.drain_events(&mut events);
    let proto = events[0].to_otlp();
    assert_eq!(proto.severity_number, 13);
    assert_eq!(
        proto.body.as_ref().unwrap().value,
        Some(Value::StringValue("standalone".into()))
    );
    assert!(proto.trace_id.is_empty());
    assert!(proto.span_id.is_empty());
    assert!(
        proto.attributes.iter().any(
            |a| a.key == "count" && a.value.as_ref().unwrap().value == Some(Value::IntValue(3))
        )
    );
}

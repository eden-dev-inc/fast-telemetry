# fast-telemetry-tracing

An optional `tracing-subscriber::Layer` that sends existing `tracing` instrumentation
through fast-telemetry collection and OTLP conversion. Adding this companion crate
does not add `tracing` to the native fast-telemetry API or change its recording path.
The crate uses edition 2024 and Rust 1.93, like the workspace.

## Use existing instrumentation

```toml
[dependencies]
fast-telemetry = "0.9"
fast-telemetry-tracing = "0.9"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["registry"] }
```

```rust
use std::sync::Arc;
use fast_telemetry::SpanCollector;
use fast_telemetry_tracing::FastTelemetryLayer;
use tracing_subscriber::prelude::*;

let collector = Arc::new(SpanCollector::new(1, 1));
let layer = FastTelemetryLayer::with_capacity(collector.clone(), 4096, 4096);
let handle = layer.handle();
let subscriber = tracing_subscriber::registry().with(layer);
tracing::subscriber::with_default(subscriber, || {
    let request = tracing::info_span!("request", user_id = 42u64);
    let _entered = request.enter();
    tracing::info!("accepted");
});

let mut completed = Vec::new();
collector.drain_into(&mut completed); // no flush_local needed for this layer
assert_eq!(completed.len(), 1);
assert_eq!(completed[0].events.len(), 1);
assert_eq!(handle.dropped_span_count(), 0);
```

`tracing::{span!, info!, ...}`, `#[tracing::instrument]`, and
`tracing::Instrument` futures keep tracing's registry behavior. Contextual and
explicit span parents, `parent: None`, late `Span::record` upserts, cloned spans,
repeated/concurrent enter/exit, and futures polled on different threads are
covered by focused regression tests. Spans publish once on the registry's final
close, after the last reference. Duration is creation to final close, including
async idle time. Use `Instrument` across awaits as usual; the layer never enters
a native fast-telemetry span or leaves native TLS active across a suspension.

Layers can compose with `.with_filter(...)` and other subscriber layers. Multiple
fast-telemetry layers on one registry have independent IDs, state, propagation
handles, queues, and loss counters. A handle queries the span's own subscriber,
so changing the caller's current subscriber does not change that span's context.

## Field and event policy

| Input | Recorded value |
| --- | --- |
| `i64`, signed/unsigned integers fitting `i64` | OTLP signed integer |
| Larger `u64`/`u128` or out-of-range `i128` | Lossless base-10 string, never a truncating cast or float |
| `bool`, `f64`, strings | Corresponding typed fast-telemetry value |
| `?value`, `%value`, `dyn Error` | Full Debug or Display string; no adapter truncation |
| Repeated recording of a field | Replaces that field's previous value |

OTLP has no unsigned or 128-bit scalar integer. Decimal strings preserve the full
value beyond its signed range; they do not preserve a separate integer type tag.
The same policy applies to fields on span events and standalone events.

Adapter metadata uses the reserved `fast_telemetry.` prefix: tracing level/target
and source module/file/line are included. User fields normally keep their names.
A user field starting with that prefix becomes `fast_telemetry.user.` followed by
its **entire original name**. This injective escape prevents user fields from
replacing adapter metadata or each other; remove one escape prefix when decoding
such user keys. For example, `fast_telemetry.tracing.level` becomes
`fast_telemetry.user.fast_telemetry.tracing.level`. No field is interpreted as a
command to change status, kind, name, or propagation.

Events use `Context::event_span`, so explicit parents and `parent: None` are
respected even while a different span is entered. Events on a recorded parent
become `CompletedSpan::events`. Events with no recorded parent go into the
separately bounded standalone-event queue, including events outside all spans and
events whose parent this layer filtered out. Drain it with
`handle.drain_events(&mut output)`. Such events have no fabricated trace context.
OTLP log conversion uses the `message` field as the body when present, otherwise
the event name; it also retains the event name as metadata.

`Span::follows_from` between spans visible to this layer becomes a timestamped
`fast_telemetry.follows_from` span event with `fast_telemetry.link.trace_id` and
`fast_telemetry.link.span_id`. This preserves the relationship in the current
`CompletedSpan` format; it is **not** an OTLP `Link`. A link to a span filtered out
of this layer cannot be resolved and is omitted.

## Cross-service propagation

```rust
use fast_telemetry::SpanContext;
use fast_telemetry_tracing::LayerHandle;
fn inbound(handle: &LayerHandle, traceparent: Option<&str>) -> tracing::Span {
    let span = tracing::info_span!(parent: None, "http request");
    if let Some(parent) = traceparent.and_then(SpanContext::from_traceparent) {
        handle.set_parent(&span, parent);
    }
    span
}
// let outgoing_header = handle.traceparent(&request_span);
```

This reuses fast-telemetry's `SpanContext` W3C version-00 parser/encoder. Assign a
remote parent immediately after creating the span, before children, events, or
outbound propagation. Later changes do not rewrite children/events already
recorded. The adapter records every enabled span, even if an upstream context was
unsampled, and sets the outgoing sampled bit accordingly. Other flag bits are
retained. `tracestate`, baggage, automatic HTTP header injection, and
OpenTelemetry context activation are outside this layer's scope.

`LayerHandle::{context, traceparent, set_parent}` provide this backend's explicit
context access. `OpenTelemetrySpanExt`, the OpenTelemetry context/baggage API,
`otel.*` control fields, automatic error-to-status/exception conversion,
OpenTelemetry metric APIs, and native fast-telemetry `current_span_id` TLS
mirroring are not emulated. Keep the OpenTelemetry layer when those integrations
are required. Compatibility is for the common tracing instrumentation tested
here; it is not a universal drop-in replacement for the OpenTelemetry ecosystem.

## Capacity and export wiring

`with_capacity(collector, span_capacity, event_capacity)` configures two nonzero
item limits; `new` uses 4096 for each. Spans use an owning `SpanSink`, which keeps
the collector alive and bypasses native adaptive sampling and worker-local
buffers. A completed low-volume span is immediately visible to the exporter,
including while its worker stays alive and idle. Span publication uses fixed
producer shards with one shared span-capacity limit; idle shards do not reserve
capacity, and producer thread exit does not grow the collector's registry.
Newest completed spans or standalone events are dropped at capacity. Inspect
`accepted_span_count`, `dropped_span_count`, `accepted_event_count`, and
`dropped_event_count` on the handle. Counts of attached span events are available
in the completed spans; the standalone counters cover only their own queue.
Native collector sampling/recording counters do not include adapter spans.

These are item-count limits, not byte limits: attributes, event payloads, and
live spans may grow until the application closes them. No field/event limit is
silently applied. Queue storage is allocated lazily, and drains retain bounded
small-batch allocations for reuse. Drain promptly and choose queue sizes for your
traffic and export interval. Reuse layers/sinks; their fixed registered shards
remain until the collector is dropped. Drains are destructive; competing
consumers divide data. Accepted counts mean queued, not acknowledged by a remote
collector.

For the existing hosted trace exporter, enable these dependencies:

```toml
fast-telemetry-tracing = { version = "0.9", features = ["otlp"] }
fast-telemetry-export = { version = "0.9", default-features = false, features = ["otlp-logs"] }
tokio-util = { version = "0.7", features = ["rt"] }
```

```rust,ignore
use fast_telemetry_export::spans::{SpanExportConfig, spawn_on};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let export_task = spawn_on(
    &tokio::runtime::Handle::current(),
    collector.clone(),
    SpanExportConfig::new("http://localhost:4318")
        .with_service_name("my-service")
        .with_max_batch_size(4096),
    cancel.clone(),
);
// On shutdown: close application spans, cancel.cancel(), then await export_task.
```

The existing trace exporter is best effort. Its normal tick truncates a drain
above `max_batch_size`, so set that limit to at least this sink's capacity (and
allow for other producers if sharing the collector), or own the drain and send
all chunks yourself. HTTP failures/partial rejection do not requeue drained data.
The adapter's counters cover local queue loss, not remote export loss.

Standalone logs are opt-in and need a separate drain/export task. The `otlp`
feature enables `RecordedEvent::to_otlp`; it does not start a runtime or task.
Use the existing protobuf helper and HTTP client:

```rust,ignore
use fast_telemetry::otlp::{build_log_export_request, build_resource};
use fast_telemetry_export::otlp::{OtlpHttpClient, OtlpHttpConfig};

let client = OtlpHttpClient::new(OtlpHttpConfig::new("http://localhost:4318"))?;
let resource = build_resource("my-service", &[]);
let mut events = Vec::new();
handle.drain_events(&mut events);
for batch in events.chunks(512) {
    let request = build_log_export_request(
        &resource, "fast-telemetry-tracing", batch.iter().map(|e| e.to_otlp()).collect(),
    );
    let outcome = client.export_logs(&request).await?;
    // Inspect outcome.rejected/message. Retain the drained data until your
    // acknowledgement/retry policy is satisfied; do not infer success from drain.
}
```

Run a final event drain after application tasks stop and spans close. Applications
requiring durable delivery must retain failed batches themselves. No standalone
event is automatically converted into a fake span or silently exported as logs.

## Native and tracing benchmark

The [benchmark guide](../../docs/tracing-benchmark.md) describes the retained-count
contract, timings, repeated-run script, and limitations. It includes ordinary
native and payload-matched native baselines, tracing FT/OTel backends, and two
no-retention controls. There is no speed or native-parity claim for this adapter
before fresh measurements. Historical native-span versus SDK ratios do not
establish tracing-layer performance.

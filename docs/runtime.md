# Runtime Guide

### Runtime

Enable the `runtime` feature when a parent service should own telemetry and pass
the same concrete runtime type into child crates. The runtime owns metric
registration and a shared span collector so export setup happens once.
`RuntimeConfig` is currently reserved for future runtime tuning; use
`RuntimeConfig::default()` when creating a runtime.

```toml
[dependencies]
fast-telemetry = { version = "0.9", features = ["runtime"] }
```

```rust
use fast_telemetry::{MetricScope, Runtime, RuntimeConfig, SpanKind};
use std::sync::Arc;

pub type TelemetryRuntime = fast_telemetry::Runtime;

let runtime: Arc<TelemetryRuntime> = Runtime::new(RuntimeConfig::default());
let metrics = runtime.register_metrics(MetricScope::new("myapp"), AppMetrics::new());

metrics.requests.inc();

let span = runtime.start_span("handle_request", SpanKind::Server);
drop(span);
```

#### Parent Service

The top-level service should create one runtime and pass `Arc` clones to child
crates that accept telemetry. Exporters should also be wired from this runtime,
so metrics and spans are collected from one shared telemetry service.
Metric exporters that consume structured observations should call
`runtime.visit_metrics(&mut visitor)`. Span exporters should clone the runtime's
span collector once.

```rust
use fast_telemetry::{Runtime, RuntimeConfig};
use fast_telemetry_export::spans::{self, SpanExportConfig};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub type TelemetryRuntime = fast_telemetry::Runtime;

let runtime: Arc<TelemetryRuntime> = Runtime::new(RuntimeConfig::default());

let span_cancel = CancellationToken::new();
let span_exporter = spans::spawn_on(
    &tokio::runtime::Handle::current(),
    Arc::clone(runtime.span_collector()),
    SpanExportConfig::new("http://otel-collector:4318").with_service_name("myapp"),
    span_cancel.clone(),
);

let cache = shardmap::ShardMap::with_parent_telemetry(Arc::clone(&runtime));

// During graceful shutdown:
span_cancel.cancel();
span_exporter.await?;
```

Metric export and span export use different surfaces from the same runtime:

```rust
let mut visitor = MyMetricVisitor::default();
runtime.visit_metrics(&mut visitor);

let mut completed_spans = Vec::new();
runtime.flush_local_spans();
runtime.drain_spans_into(&mut completed_spans);
```

#### Child Crates

Child crates should re-export the exact runtime type they accept, register their
metrics once during construction, and store the returned `RegisteredMetrics<M>`
or direct handles inside their own telemetry state.

```rust
use fast_telemetry::{
    Counter, ExportMetrics, MetricScope, RegisteredMetrics, Runtime, RuntimeConfig, SpanKind,
};
use std::sync::Arc;

pub type TelemetryRuntime = fast_telemetry::Runtime;

#[derive(ExportMetrics)]
#[metric_prefix = "shardmap"]
pub struct CacheMetrics {
    #[help = "Cache lookups"]
    lookups: Counter,
}

impl CacheMetrics {
    fn new() -> Self {
        Self {
            lookups: Counter::new(4),
        }
    }
}

pub struct CacheTelemetry {
    runtime: Arc<TelemetryRuntime>,
    metrics: RegisteredMetrics<CacheMetrics>,
}

impl CacheTelemetry {
    pub fn with_parent_telemetry(runtime: Arc<TelemetryRuntime>) -> Self {
        let metrics =
            runtime.register_metrics(MetricScope::new("shardmap.cache"), CacheMetrics::new());
        Self { runtime, metrics }
    }

    pub fn standalone() -> Self {
        Self::with_parent_telemetry(Runtime::new(RuntimeConfig::default()))
    }

    pub fn record_lookup(&self) {
        self.metrics.lookups.inc();

        let _span = self.runtime.start_span("shardmap.cache.lookup", SpanKind::Internal);
        // lookup work
    }

    pub fn record_inbound_lookup(&self, traceparent: Option<&str>) {
        let _span = self.runtime.start_span_from_traceparent(
            traceparent,
            "shardmap.cache.inbound_lookup",
            SpanKind::Server,
        );
        // inbound lookup work
    }
}
```

#### Performance Rules

- Create one `Runtime` per service and share it across crate boundaries.
- Make parent-owned runtime injection the primary child-crate constructor.
- Name fallback constructors `standalone` so they cannot silently split a
  service's telemetry registry.
- Register metric groups once during construction, not during operations.
- Keep hot paths on `RegisteredMetrics<M>` or direct metric handles.
- Clone `runtime.span_collector()` once when wiring span exporters.
- Start spans through the shared runtime or its shared `SpanCollector`.
- Do not create a new `Runtime` or `SpanCollector` per child crate when a parent
  runtime is available.
- Run Tokio exporters on the parent executor with `spawn_on`, unless a
  dedicated telemetry thread is an explicit isolation choice.
- Do not look up metric groups in the registry per operation.

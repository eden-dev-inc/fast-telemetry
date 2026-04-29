# fast-telemetry

High-performance, cache-friendly telemetry for Rust.

## Crates

| Crate                                                   | Description                                                                                                        |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| [`fast-telemetry`](crates/fast-telemetry)               | Sharded counters, gauges, histograms, distributions, spans, and format serialization (Prometheus, DogStatsD, OTLP) |
| [`fast-telemetry-macros`](crates/fast-telemetry-macros) | Derive macros: `ExportMetrics` and `LabelEnum`                                                                     |
| [`fast-telemetry-export`](crates/fast-telemetry-export) | I/O adapters: DogStatsD over UDP, OTLP over HTTP/protobuf, ClickHouse over native TCP, span export, stale-series sweeping |

## Why

fast-telemetry grew out of [Eden](https://eden.dev)'s observability stack. Eden was a
heavy user of the OpenTelemetry ecosystem — we relied on the `opentelemetry`
crate and its SDK for metrics across our services. That worked fine until we
started benchmarking our high-performance Redis proxy under realistic production
load.

The proxy handles millions of operations per second across many cores, and we
care about per-request, per-endpoint, and per-organization telemetry -- a lot of
counters, which led to a lot of contention. Under benchmark loads, the metrics layer
itself became a clear bottleneck.

Profiling showed the root cause to be mostly excessive **cache-line bouncing** on shared atomic counters.

When multiple threads contend on a single shared atomic, the cache line holding
that counter continually transfers between cores
([MESI coherence traffic](https://travisdowns.github.io/blog/2020/07/06/concurrency-costs.html)).
This serializes what should be parallel work, creating latency spikes and
throughput cliffs — exactly the opposite of what you want under high concurrency.

fast-telemetry started as sharded counters and gauges to fix that contention. Once those
proved themselves, we expanded to cover the rest of what we'd been using the OTel
SDK for — histograms, distributions, labeled metrics, lightweight spans — and
added export adapters for the backends we actually use (Prometheus, DogStatsD,
OTLP). At that point we'd fully replaced the `opentelemetry` crate on the hot
path and decided to open-source the result.

We shard counting events across cache-line-padded atomic cells per
thread. The common write path is effectively thread-local, minimizing cross-core
contention. _Reads_ aggregate all shards, but this is fine because export is
infrequent relative to increments.

| Operation                               | Latency       |
| --------------------------------------- | ------------- |
| Thread-local increment (fast-telemetry) | ~2 ns         |
| Uncontended atomic                      | ~10 ns        |
| **Contended atomic (16 cores)**         | **40-400 ns** |

The difference is important when you're incrementing counters millions of times per
second and don't want your telemetry to be the thing that slows you down or pollutes your numbers.

## When to use this (and when not to)

fast-telemetry is for applications where **telemetry throughput matters** — you're
recording millions of metric events per second across many cores and you've
measured that your current metrics layer is a bottleneck.

**Use fast-telemetry when:**

- You need e.g. per-request, per-endpoint, or per-tenant counters at high concurrency, and you want every single event
- You've profiled and found your metrics SDK is a bottleneck
- You want to instrument a hot path without adding latency to it

**Use something else when:**

- Your metrics are low-frequency (< 10k increments/sec) — standard atomics are fine,
  and the [`opentelemetry`](https://crates.io/crates/opentelemetry) crate gives you
  a richer, community-standard API with broader ecosystem integration
- API ergonomics or OpenTelemetry spec compliance matter more than raw throughput
- You want automatic context propagation, SDK-managed pipelines, or deep
  integration with the broader OTel collector ecosystem

fast-telemetry trades API surface and ecosystem breadth for contention-free recording.

If you don't have a contention problem, you're probably better off with the
broader OpenTelemetry ecosystem.

For detailed benchmark results and methodology, see
[BENCHMARK_REPORT.md](crates/fast-telemetry/bench/BENCHMARK_REPORT.md) and the
[bench harness README](crates/fast-telemetry/bench/README.md).

## Feature Flags

| Feature      | Default | Description                                      |
| ------------ | ------- | ------------------------------------------------ |
| `macros`     | ✓       | `ExportMetrics` and `LabelEnum` derive macros    |
| `otlp`       |         | OTLP protobuf export support                     |
| `clickhouse` |         | First-party ClickHouse row export support        |
| `eviction`   |         | Enable stale-series eviction for dynamic metrics |

The `eviction` feature enables `evict_stale()`, `advance_cycle()`, and `current_cycle()` on
dynamic metric types. Without it, these APIs are not available.

## Quick Start

```toml
[dependencies]
fast-telemetry = "0.1"
```

### Define Metrics

```rust
use fast_telemetry::{Counter, ExportMetrics, Gauge, Histogram, MaxGauge, SampledTimer};

#[derive(ExportMetrics)]
#[metric_prefix = "myapp"]
pub struct AppMetrics {
    #[help = "Total requests processed"]
    pub requests: Counter,

    #[help = "Request latency in microseconds"]
    pub latency: Histogram,

    #[help = "Hot path latency"]
    pub hot_path_latency: SampledTimer,

    #[help = "Current queue depth"]
    pub queue_depth: Gauge,

    #[help = "Peak queue depth seen during the current export window"]
    pub queue_depth_peak: MaxGauge,
}

impl AppMetrics {
    pub fn new() -> Self {
        Self {
            requests: Counter::new(4),     // use available_parallelism() in production
            latency: Histogram::with_latency_buckets(4),
            hot_path_latency: SampledTimer::with_latency_buckets(4, 64),
            queue_depth: Gauge::new(),
            queue_depth_peak: MaxGauge::new(4),
        }
    }
}
```

### Record

```rust
metrics.requests.inc();
metrics.latency.record(elapsed_us);
metrics.hot_path_latency.record_elapsed(std::time::Duration::from_nanos(125_000));
metrics.queue_depth.set(queue.len() as i64);
metrics.queue_depth_peak.observe(queue.len() as i64);
```

### Sampled Timers

`SampledTimer` is for measuring elapsed time around an operation. You can start
timing with an RAII guard and let it record on drop, or record a pre-measured
`Duration` directly.

It is designed for hot paths where you want timing instrumentation to stay
cheap. Every use increments a total call counter, while elapsed-time
measurements are sampled at the configured stride before being recorded into the
latency histogram.

```rust
use std::time::Duration;
use fast_telemetry::{DeriveLabel, LabeledSampledTimer, SampledTimer};

let timer = SampledTimer::with_latency_buckets(4, 64);

// Record a duration you already measured elsewhere.
timer.record_elapsed(Duration::from_nanos(180_000));

{
    // Time this scope with an RAII guard.
    let _guard = timer.start();
    // timed work
}

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "phase"]
enum Phase {
    Parse,
    Execute,
}

let labeled = LabeledSampledTimer::<Phase>::with_latency_buckets(4, 32);

{
    // Time a labeled scope with the same guard pattern.
    let _guard = labeled.start(Phase::Parse);
    // timed parse work
}

// Or record a pre-measured labeled duration directly.
labeled.record_elapsed(Phase::Parse, Duration::from_nanos(90_000));
```

Sampled timers record durations in **nanoseconds**. They export as a composite
metric:

- Prometheus: `name_calls` and `name_samples`
- DogStatsD / OTLP: `name.calls` and `name.samples`

`name_calls` / `name.calls` is the total operation count. `name_samples` is the
sampled latency histogram. The timer/guard API is the instrumentation surface;
sampling is how it keeps that instrumentation cheap enough for hot code.

### Export

```rust
// Prometheus text format
let mut output = String::new();
metrics.export_prometheus( & mut output);

// DogStatsD
let mut output = String::new();
metrics.export_dogstatsd( & mut output, & [("env", "prod")]);
```

### Extrema Gauges

Use extrema gauges when you want peak/trough tracking without putting a single
contended atomic on the hot path.

```rust
use fast_telemetry::{MaxGauge, MaxGaugeF64, MinGauge, MinGaugeF64};

let queue_peak = MaxGauge::new(4);
queue_peak.observe(queue.len() as i64);

let min_free_slots = MinGauge::with_value(4, i64::MAX);
min_free_slots.observe(free_slots as i64);

let cpu_peak = MaxGaugeF64::new(4);
cpu_peak.observe(cpu_utilization);

let cheapest_latency_ms = MinGaugeF64::with_value(4, f64::INFINITY);
cheapest_latency_ms.observe(latency_ms);
```

For interval export, call `swap_reset()` in your sampler/exporter task:

```rust
let peak_in_window = queue_peak.swap_reset();
let min_in_window = cheapest_latency_ms.swap_reset();
```

`observe()` is hot-path safe and shard-friendly. `get()` returns the current
extremum across shards, and `swap_reset()` gives you the previous window's
extremum while resetting back to the constructor's initial value. The `f64`
variants ignore `NaN` observations.

## Labeled Metrics

### Compile-Time Labels (O(1) array lookup)

```rust
use fast_telemetry::{LabeledCounter, DeriveLabel};

#[derive(Copy, Clone, Debug, DeriveLabel)]
#[label_name = "method"]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
}

let counter: LabeledCounter<HttpMethod> = LabeledCounter::new(4);
counter.inc(HttpMethod::Get);
```

### Dynamic Runtime Labels

```rust
use fast_telemetry::{DynamicCounter, advance_cycle};

let counter = DynamicCounter::with_max_series(4, 10_000);
counter.inc(& [("endpoint_id", "ep-1"), ("org_id", "org-a")]);

// Hot-path optimization: resolve once, then increment via handle
let series = counter.series(& [("endpoint_id", "ep-1"), ("org_id", "org-a")]);
series.inc();

// With the `eviction` feature enabled:
// Long-lived handles can outlive a stale-series sweep.
if series.is_evicted() {
  let fresh = counter.series(& [("endpoint_id", "ep-1"), ("org_id", "org-a")]);
  fresh.inc();
}

advance_cycle();
let _evicted = counter.evict_stale(30);  // requires `eviction` feature
let _overflow = counter.overflow_count();
```

Dynamic metrics are useful when the active label set is only known at runtime,
but they come with a lifecycle worth planning for:

- `with_max_series(...)` bounds cardinality for `DynamicCounter`,
  `DynamicDistribution`, `DynamicGauge`, and `DynamicGaugeI64`
- `DynamicHistogram::with_limits(..., max_series)` provides the same cap for histograms
- once the cap is hit, new label sets are redirected into a single overflow series
  and `overflow_count()` tells you how often that happened
- with the `eviction` feature, stale series are evicted with `evict_stale(...)` after `advance_cycle()`
- long-lived handles can check `is_evicted()` and re-resolve with `series(...)`

## Spans

```rust
use std::sync::Arc;
use fast_telemetry::{
  SpanCollector, SpanKind, SpanStatus, current_span_id, current_trace_id,
};

let collector = Arc::new(SpanCollector::new(4, 4096));

{
let mut root = collector.start_span("handle_request", SpanKind::Server);
root.enter();
root.set_attribute("http.method", "GET");

{
let mut child = root.child("db_query", SpanKind::Client);
child.set_attribute("db.system", "postgres");
child.set_status(SpanStatus::Ok);
}

root.set_status(SpanStatus::Ok);
} // spans submit to collector on drop

if let (Some(trace_id), Some(span_id)) = (current_trace_id(), current_span_id()) {
println!("trace_id={trace_id} span_id={span_id}");
}

let mut completed = Vec::new();
collector.flush_local();
collector.drain_into( & mut completed);
```

Call `flush_local()` before `drain_into()` when you are draining on the same
thread that just recorded spans. `SpanCollector::new(shards, capacity)` keeps
its historical signature for compatibility, but those tuning arguments are
currently ignored because buffers are now managed per thread.

For manual cross-service propagation, use an incoming W3C `traceparent` header
to start a span and `traceparent()` on the current span for outgoing requests:

```rust
let mut inbound = collector.start_span_from_traceparent(
  request.headers().get("traceparent").and_then(|v| v.to_str().ok()),
  "handle_request",
  SpanKind::Server,
);
let outbound_traceparent = inbound.traceparent();
```

## Export Adapters (fast-telemetry-export)

For production use, `fast-telemetry-export` provides background export loops with
batching, compression, backoff, and graceful shutdown.

```toml
[dependencies]
fast-telemetry-export = "0.1"
```

### DogStatsD

```rust
use std::sync::Arc;
use fast_telemetry_export::dogstatsd::{DogStatsDConfig, run};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = DogStatsDConfig::new("127.0.0.1:8125");

let metrics = Arc::new(my_metrics);
let mut state = MyMetricsExportState::new();
let tags = vec![("service", "myapp"), ("env", "prod")];

tokio::spawn(run(config, cancel, move | output| {
  metrics.export_dogstatsd_delta(output, & tags, &mut state);
}));
```

`MyMetricsExportState` is the derive-generated per-sink state type from
`#[derive(ExportMetrics)]`. Keep one state value per DogStatsD export loop when
using delta temporality.

### OTLP Metrics

```rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry_export::otlp::{OtlpConfig, run};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = OtlpConfig::new("http://otel-collector:4318")
    .with_service_name("myapp")
    .with_scope_name("proxy")
    .with_attribute("service.version", "1.0")
    .with_header("Authorization", "Bearer <token>")
    .with_timeout(Duration::from_secs(5));

let metrics = Arc::new(my_metrics);

tokio::spawn(run(config, cancel, move | out| {
  metrics.export_otlp(out);
}));
```

The OTLP metrics exporter gzip-compresses larger payloads automatically and
applies exponential backoff on transport failures.

### OTLP Spans

```rust
use std::time::Duration;
use fast_telemetry_export::spans::{SpanExportConfig, spawn};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = SpanExportConfig::new("http://otel-collector:4318")
    .with_service_name("myapp")
    .with_scope_name("proxy")
    .with_header("Authorization", "Bearer <token>")
    .with_timeout(Duration::from_secs(5))
    .with_max_batch_size(1024);

spawn(collector, config, cancel);
```

### ClickHouse (native TCP)

Behind the `clickhouse` feature flag. Three layers are provided:

**First-party OTel-standard rows** — skips OTLP protobuf construction and writes
metrics directly into ClickHouse row batches. Enable the `clickhouse` feature on
`fast-telemetry`, add `#[clickhouse]` to the metrics struct, and use
`run_first_party`:

```rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry::{Counter, ExportMetrics, Gauge};
use fast_telemetry_export::clickhouse::otel_standard::{OtelStandardConfig, run_first_party};
use tokio_util::sync::CancellationToken;

#[derive(ExportMetrics)]
#[metric_prefix = "myapp"]
#[clickhouse]
pub struct AppMetrics {
    pub requests: Counter,
    pub queue_depth: Gauge,
}

let cancel = CancellationToken::new();
let config = OtelStandardConfig::new("clickhouse.internal:9000", "myapp")
    .with_credentials("metrics_writer", "<password>")
    .with_database("telemetry")
    .with_interval(Duration::from_secs(30));

let metrics = Arc::new(my_metrics);

tokio::spawn(run_first_party(config, cancel, move |batch, timestamp| {
    metrics.export_clickhouse(batch, timestamp);
}));
```

**Drop-in OTLP translation** — auto-creates the configured database and four
metric tables (`otel_metrics_sum`, `otel_metrics_gauge`,
`otel_metrics_histogram`, `otel_metrics_exponential_histogram`) compatible with
the [OpenTelemetry Collector ClickHouse exporter] layout, so common metric
queries and dashboards can use the same column names while reusing an existing
`export_otlp()` implementation:

```rust
use std::sync::Arc;
use std::time::Duration;
use fast_telemetry_export::clickhouse::otel_standard::{OtelStandardConfig, run};
use tokio_util::sync::CancellationToken;

let cancel = CancellationToken::new();
let config = OtelStandardConfig::new("clickhouse.internal:9000", "myapp")
    .with_credentials("metrics_writer", "<password>")
    .with_database("telemetry")
    .with_interval(Duration::from_secs(30));

let metrics = Arc::new(my_metrics);

tokio::spawn(run(config, cancel, move |out| {
    metrics.export_otlp(out);
}));
```

The built-in exporter writes sum, gauge, histogram, and exponential histogram
metrics. Collector compatibility columns for scope/schema/exemplar data are
created, but flat `export_otlp()` metrics populate them with defaults; summary
metrics are ignored.

**Generic primitive** — for custom schemas. Caller supplies a row type
deriving `klickhouse::Row` and a translator closure that turns each
`pb::Metric` into zero or more rows. The runtime handles connection,
ticking, batched native-protocol inserts, and exponential backoff;
schema and migrations are the caller's problem. Spawn one task per table
for multi-table layouts.

```rust
use fast_telemetry::otlp::pb;
use fast_telemetry_export::clickhouse::{ClickHouseConfig, run};
use klickhouse::{DateTime64, Tz};

#[derive(klickhouse::Row, Debug)]
#[allow(non_snake_case)]
struct MyRow {
    MetricName: String,
    TimeUnix: DateTime64<9>,
    Value: f64,
}

tokio::spawn(run(
    ClickHouseConfig::new("clickhouse.internal:9000").with_database("telemetry"),
    "my_metrics",
    cancel,
    move |out| metrics.export_otlp(out),
    |metric: &pb::Metric| match &metric.data {
        Some(pb::metric::Data::Sum(s)) => s.data_points.iter().map(|dp| MyRow {
            MetricName: metric.name.clone(),
            TimeUnix: DateTime64::<9>(Tz::UTC, dp.time_unix_nano),
            Value: 0.0,
        }).collect(),
        _ => Vec::new(),
    },
));
```

[OpenTelemetry Collector ClickHouse exporter]: https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter

### Stale Series Sweeper

Bounds memory from dynamic labels by evicting inactive series:

```rust
use std::sync::Arc;
use fast_telemetry::advance_cycle;
use fast_telemetry_export::sweeper::{SweepConfig, run};
use tokio_util::sync::CancellationToken;

let metrics = Arc::new(my_metrics);
let cancel = CancellationToken::new();

tokio::spawn(run(SweepConfig::default (), cancel, move | threshold| {
  advance_cycle();
  metrics.requests_by_endpoint.evict_stale(threshold)
    + metrics.latency_by_endpoint.evict_stale(threshold)
}));
```

If you wrap your metrics in a helper method, call `advance_cycle()` once per
sweep and then sum each dynamic metric's `evict_stale(...)` result.

## OTLP Protobuf (Manual)

For direct control over OTLP encoding without the export loop.
Add `#[otlp]` to your metrics struct to generate the `export_otlp()` method:

```rust
use fast_telemetry::otlp;

let mut metrics = Vec::new();
my_metrics.export_otlp( & mut metrics, fast_telemetry::otlp::now_nanos());

let resource = otlp::build_resource("myapp", & [("env", "prod")]);
let request = otlp::build_export_request( & resource, "fast-telemetry", metrics);
// Encode and send `request` with your own transport
```

## API Reference

### Metric Types

| Type                 | Use Case                                 | Hot Path Cost             |
| -------------------- | ---------------------------------------- | ------------------------- |
| `Counter`            | Totals that only go up                   | ~2ns (thread-local write) |
| `Histogram`          | Latency distributions with fixed buckets | ~2ns + bucket lookup      |
| `Distribution`       | Exponential-bucket distributions         | ~2ns + bucket lookup      |
| `Gauge` / `GaugeF64` | Point-in-time values                     | ~2ns (single atomic)      |
| `MaxGauge` / `MinGauge` | Peak/trough of integer observations   | ~2ns (sharded min/max)    |
| `MaxGaugeF64` / `MinGaugeF64` | Peak/trough of float observations | ~2ns (sharded min/max)    |

### Labeled Variants

| Type                               | Label Resolution                    |
| ---------------------------------- | ----------------------------------- |
| `LabeledCounter<L>`                | Compile-time enum, O(1) array index |
| `LabeledHistogram<L>`              | Compile-time enum, O(1) array index |
| `LabeledGauge<L>`                  | Compile-time enum, O(1) array index |
| `DynamicCounter`                   | Runtime labels, HashMap lookup      |
| `DynamicHistogram`                 | Runtime labels, HashMap lookup      |
| `DynamicDistribution`              | Runtime labels, HashMap lookup      |
| `DynamicGauge` / `DynamicGaugeI64` | Runtime labels, HashMap lookup      |

### Derive Macros

| Macro                                      | Purpose                                                                                                                                     |
| ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `#[derive(ExportMetrics)]`                 | Generate `export_prometheus`, `export_dogstatsd`, `export_dogstatsd_delta`, `export_dogstatsd_with_temporality`, and optional `export_otlp` |
| `#[derive(LabelEnum)]` (via `DeriveLabel`) | Generate `LabelEnum` trait impl for enum labels                                                                                             |

### Export Formats

| Format          | Method                                                                                                          | Transport                                       |
| --------------- | --------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| Prometheus text | `export_prometheus()`                                                                                           | Serve at `/metrics`                             |
| DogStatsD       | `export_dogstatsd()`, `export_dogstatsd_delta()`, or `export_dogstatsd_with_temporality(..., Temporality, ...)` | UDP via `fast-telemetry-export`                 |
| OTLP protobuf   | `export_otlp()` (requires `#[otlp]` on struct)                                                                  | HTTP via `fast-telemetry-export`                |
| ClickHouse rows | `export_clickhouse()` (requires `#[clickhouse]` on struct) or `ClickHouseExport`; `export_otlp()` fallback also supported | Native TCP via `fast-telemetry-export[clickhouse]` |

## Shard Count

Pass the number of shards to `Counter::new(n)` and other constructors:

- **`std::thread::available_parallelism()`** (or equivalent) for production
- **`4`** for tests
- Must be >= 1, rounded up to power of two internally
- One cache line (128 bytes on x86-64) per shard

## Lineage

The `Counter` implementation originated from
[`JackThomson2/fast-counter`](https://github.com/JackThomson2/fast-counter).
This project has since evolved substantially.

## Scope

fast-telemetry is **metrics and lightweight spans**. It does not cover:

- Structured logging
- Distributed trace backends (ingestion, storage, query)
- Automatic cross-service context propagation
- Alerting or dashboarding

## Further Reading

- [Concurrency Costs](https://travisdowns.github.io/blog/2020/07/06/concurrency-costs.html) — why contended atomics are
  slow
- [crossbeam CachePadded](https://docs.rs/crossbeam-utils/latest/crossbeam_utils/struct.CachePadded.html) — the padding
  we use

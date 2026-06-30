# Metrics and Spans

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

## Grouped Counters

Use grouped counters when one hot-path operation records several related
counters at the same time. `CounterSet` stores a fixed-size group of sharded
counters together, and `CounterSetBuffer` accumulates local deltas before
flushing them to shared atomics.

Resolve indexes once during construction and keep the hot path on direct
integer indexes. Avoid per-operation name lookup.

```rust
use fast_telemetry::{CounterSet, CounterSetBuffer};

const REQUESTS: usize = 0;
const BYTES: usize = 1;
const ERRORS: usize = 2;

let counters = CounterSet::new(4, 3);
let mut buffer = CounterSetBuffer::new(&counters, 64);

fn record_request(buffer: &mut CounterSetBuffer<'_>, bytes: isize, failed: bool) {
    buffer.inc(REQUESTS);
    buffer.add(BYTES, bytes);
    if failed {
        buffer.inc(ERRORS);
    }
    buffer.finish_op();
}

record_request(&mut buffer, 4096, false);
record_request(&mut buffer, 512, true);
buffer.flush();

assert_eq!(counters.sum(REQUESTS), 2);
assert_eq!(counters.sum(BYTES), 4608);
assert_eq!(counters.sum(ERRORS), 1);
```

`finish_op()` marks one logical operation complete and drives the `flush_every`
threshold. `inc_all()`, `add_all(...)`, and `add_values(...)` finish the
operation automatically. Buffers flush on drop, but explicit `flush()` is useful
before export or at request/task boundaries when fresh totals matter.

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
counter.inc(&[("endpoint_id", "ep-1"), ("org_id", "org-a")]);

// Hot-path optimization: resolve once, then increment via handle
let series = counter.series(&[("endpoint_id", "ep-1"), ("org_id", "org-a")]);
series.inc();

// With the `eviction` feature enabled:
// Long-lived handles can outlive a stale-series sweep.
if series.is_evicted() {
  let fresh = counter.series(&[("endpoint_id", "ep-1"), ("org_id", "org-a")]);
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
collector.drain_into(&mut completed);
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

# Performance and Use Cases

## Why

fast-telemetry grew out of [Eden](https://eden.dev)'s observability stack. Eden was a
heavy user of the OpenTelemetry ecosystem. At Eden, we relied on the `opentelemetry`
crate and its SDK for metrics across our services. That worked fine until we
started benchmarking our high-performance Redis proxy under realistic production
load.

The proxy handles millions of operations per second across many cores, and we
care about telemetry per-request, per-endpoint, and per-organization. That means
lots of counters, which led to a lot of contention. Under benchmark loads, the
metrics layer itself became a clear bottleneck.

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

We shard counting events across cache-line-padded atomic cells per thread. The
common write path is effectively thread-local, minimizing cross-core contention.
_Reads_ aggregate all shards, but this is fine because export is infrequent
relative to increments.

Representative counter costs:

| Operation | Cost |
| --- | ---: |
| Grouped buffered fast-telemetry counter, 6-counter group | 0.19 CPU ns/incr, ~0.88 estimated cycles/incr |
| Independent fast-telemetry counter, 6-counter group workload | 0.73 CPU ns/incr, ~3.37 estimated cycles/incr |
| Uncontended atomic | ~10 ns |
| Contended atomic (16 cores) | 40-400 ns |
| OpenTelemetry Rust `u64_counter`, 6-counter group workload | 254.91 CPU ns/incr, ~1175.14 estimated cycles/incr |

In that 6-counter harness row, grouped buffered counters measured 45.77B
counters/s versus 18.72B counters/s for independent fast counters and 60.87M
counters/s for OpenTelemetry Rust. The cycle numbers are estimated from the mac
CPU-time result; Linux `--perf-stat` runs record measured `*_cycles_per_write`
fields.

The difference is important when you're incrementing counters millions of times
per second and don't want your telemetry to be the thing that slows you down or
pollutes your numbers.

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
[BENCHMARK_REPORT.md](../crates/fast-telemetry/bench/BENCHMARK_REPORT.md) and the
[bench harness README](../crates/fast-telemetry/bench/README.md).

# Comparing the tracing backends

`fast-telemetry-tracing` provides `bench_tracing_backends`, a matched **in-memory
retention** benchmark against `tracing-opentelemetry` 0.32 with OpenTelemetry SDK
0.31. Both modes execute the exact same compiled `tracing` callsites through a
registry, using scoped subscriber defaults explicitly installed on every worker.
A disabled-subscriber control executes those callsites with recording disabled.
Every script sample starts a fresh process; no process changes global subscribers
or switches measured backends mid-run.

No adapter speedup is asserted here. Historical native fast-telemetry versus SDK
span ratios did not measure this layer and did not establish equal retention.

## Retention and workload contract

Both backends retain all completed spans into one bounded immediate in-memory
sink of the same configured capacity. Fast-telemetry uses `SpanSink` without
adaptive sampling. OpenTelemetry uses explicit `Sampler::AlwaysOn` and a
`SimpleSpanProcessor` with a custom bounded in-memory `SpanExporter`. There is no
background drain race or unbounded in-memory history. On each synchronized batch,
workers finish and stay alive while the coordinator drains the sinks. Next-batch
recording starts only after the drain and verification. OpenTelemetry
`force_flush` runs at the drain boundary. Neither backend sends HTTP traffic.
This measures this immediate retention policy, not the default asynchronous
OpenTelemetry batch processor or a production network exporter.

| Workload | Spans per iteration | Events per iteration | User payload |
| --- | ---: | ---: | --- |
| `root` | 1 | 0 | Root lifecycle/enter/exit |
| `attributes` | 2 | 1 | Root worker/iteration, child method/code, event ok/message |
| `children` | 4 | 0 | Root and three distinct child names |

Both layers record level, target, and source metadata. OpenTelemetry thread,
activity-time, and context-activation extras are disabled because this adapter
does not provide those facilities. Attribute names and event-message
representation differ between backends; verification checks equivalent user
payloads and relationships, not byte-identical metadata. This benchmark does not
exercise the adapter's decimal-string policy for large integers, async polling,
remote propagation, filtering, standalone logs, or durability; focused tests
cover those semantics separately.

Every batch inspects the **actual drained completed spans**, checking exact
span/event/root/child/user-field counts, root names, child names, unique span
identities, and parent/trace relationships. OpenTelemetry dropped-attribute/event
counts and both sink rejection counters must be zero. The final accepted counter
delta must equal the actual drained count. A mismatch exits nonzero. Focused
regressions run both real sinks with deliberately insufficient capacity and
require failure. The disabled control must drain zero spans/events.

## Run on Adam

Run through the engineering coordinator after static review and Adam's disk gate.
No benchmark is added to CI. Rust 1.93 or newer is required.

```sh
cargo test -p fast-telemetry-tracing --all-features
RUNS=5 THREADS='1 2 4 8' ITERS=10000 WARMUP=100 BATCH=64 CAPACITY=4096 \
  bash crates/fast-telemetry-tracing/scripts/benchmark.sh \
  /absolute/path/to/owned-run/tracing-results
```

The script requires a clean committed worktree, builds once in release mode,
then runs fresh samples with alternating fast/OTel order across repetitions. It
retains each command, timestamp, CSV row, and stderr along with revision, host,
kernel, Rust/Cargo versions, build log, binary checksum, dependency tree, and a
copy/checksum of the resolved ignored Cargo.lock. `all-runs.csv` preserves every
sample; `medians.csv` summarizes without discarding provenance or outliers.
`ITERS` and `WARMUP` are **per thread**. Keep capacity at least
`threads × min(batch, iterations) × spans_per_iteration` to retain each batch;
undersizing is permitted and must fail the count contract.

For an individual sample:

```sh
cargo run --release -p fast-telemetry-tracing --features bench-tools \
  --bin bench_tracing_backends -- \
  --mode fast --workload attributes --threads 4 --iters 10000 \
  --warmup 100 --batch 64 --capacity 4096
```

Use `--mode otel` or `--mode disabled` for the comparison. Fresh processes and
multiple runs are needed for conclusions; a single debug build or successful
submission count is not benchmark evidence. Preserve the lockfile from the run
when reproducing dependency versions.

## Read the timings

Warmup follows the same drain/verification policy, is fully drained, and is
excluded from reported times and accepted-count deltas. The measured run has a
fresh counter baseline. All workers install their dispatch before their loop.

| Output | Interval |
| --- | --- |
| `record_wall_ns`, `record_cpu_ns` | Same synchronized recording intervals, including barrier coordination |
| `drain_wall_ns`, `drain_cpu_ns` | Same intervals for draining; includes OTel force_flush, excludes semantic verification and destruction |
| `verify_wall_ns`, `verify_cpu_ns` | Semantic checks and destruction of drained payloads |
| `total_wall_ns` | Measured run including worker setup/join, all phases, and coordination; excludes warmup/backend setup |
| `record_cpu_ns_per_iteration` | Recording CPU divided by workload iterations across all threads |
| `record_cpu_ns_per_span` | Recording CPU divided by actual retained spans; `NA` for disabled |
| `pipeline_cpu_ns_per_iteration`, `pipeline_cpu_ns_per_span` | Recording plus drain CPU, with the same denominators; verification excluded |

CPU is process CPU time from `CLOCK_PROCESS_CPUTIME_ID`, summed over all process
threads; wall time uses `Instant`. CPU time is not elapsed latency, and a workload
iteration is not a span when it creates children. Barrier overhead matters for
small batches, especially the disabled control. Compare the same batch sizes,
thread counts, and workload. Larger batches change queue contention, allocation,
and coordination amortization; report the configuration with results.

SDK conversion and simple-exporter invocation occur during OTel span closure;
FT publication creates `CompletedSpan` during closure. Draining measures moving
those retained structures, not protobuf encoding or transport. Adding the drain
columns to recording does not produce an OTLP network cost. No cycles, full
OpenTelemetry API parity, universal performance ratio, or durable-delivery claim
can be inferred from these results.

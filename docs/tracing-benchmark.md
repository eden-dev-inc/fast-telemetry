# Comparing native spans and tracing backends

Use two complementary benchmarks: the backend lifecycle comparison below, and
[the complete sender pipeline](../benchmarks/tracing-end-to-end/README.md).
Both verify actual retained payloads; their timing boundaries differ.

| Benchmark | Included | Excluded |
| --- | --- | --- |
| `bench_tracing_backends` | Recording, required local flush, drain; separate actual payload destruction | Protobuf encoding, compression, network |
| `tracing-end-to-end`, `capture` | Recording, collection, real OTLP encoding, HTTP request construction, actual payload destruction | Compression, sockets, collector processing |
| `tracing-end-to-end`, HTTP | Sender pipeline plus HTTP response and OTLP acknowledgement validation | Compression, TLS, retries, downstream collector processing |
| Saturation binaries | Concurrent recording, drain, validation, destruction | Encoding and network; adaptive sampling and queue rejection may occur |

Process CPU sums time across threads. Wall throughput includes scheduling and
coordination. A `children` iteration is a complete four-span trace; distinguish
per-span and per-trace values. Registry and disabled controls retain no spans.
Saturation results must report attempted, sampled, rejected and collected counts
alongside complete-trace bounds; attempted no-op rate is not retained throughput.

## Implementation under measurement

The compatibility layer and standalone subscriber publish completed spans
immediately through an owning, bounded `SpanSink`. They retain every enabled
span until queue capacity is reached and do not use native adaptive sampling.
The default metadata policy is `Full`. Explicit `UserFields` measures a smaller
payload, with its own exact native control; it does not establish default-policy
parity. See [the adapter contract](../crates/fast-telemetry-tracing/README.md).

Native spans borrow their collector owner, use compact active records, and
materialize `CompletedSpan` at completion. Optional metadata uses a bounded cache
of empty boxes. Each producer owns two span buckets; publication normally occurs
at 256 spans, or on explicit local flush and thread exit. `SpanBatch` transfers
bucket allocations to export without flattening records. The queued-payload
limit remains 4,096 spans per producer; exporter-owned batches and retained empty
allocations are additional memory. Low-volume workers must flush explicitly.
The flat `drain_into` API remains available.

Native root counters use 32 padded stripes with immediately visible atomic
updates. Native Linux 64-bit timestamps use the OS monotonic clock with a fixed
Unix anchor; other platforms use `Instant`. The clock keeps its original
resolution and ordering, without cached readings or hardware-counter calibration.
Propagation uses fixed-size hexadecimal encoding and validates the W3C version-00
header. See [native spans and migration notes](metrics-and-spans.md#spans).

The direct OTLP encoder reads borrowed native payloads and writes one owned
request buffer, avoiding intermediate protobuf objects and string/ID copies.
Golden tests compare complete bytes with `CompletedSpan::to_otlp`, including
nested attributes, event fields, statuses and batch limits. Batches of 16–512
spans cache lengths in a bounded 4 KiB stack frame on 64-bit targets. Other sizes
recompute lengths. The first-party exporter releases native payloads before
awaiting HTTP and transfers uncompressed bodies without an extra copy. Existing
compression, response bounds, acknowledgement and best-effort delivery policies
still apply; the exporter does not retain failed drained batches for replay.

## Profiling and performance claims

Use optimized, immutable binaries and preserve their hashes, lockfiles, commands,
raw samples and host configuration. Rotate process order and repeat each fixture.
Verify warmup and measured output, including IDs, parent relationships, typed
fields, timestamps, actual serialized counts and zero sampling or loss for
lossless comparisons. Include actual payload destruction in complete-pipeline
figures. Keep instrumented profiles separate from uninstrumented timings.

Phase profiles distinguish recording, collection, encoding, transport construction
and cleanup. Per-thread CPU sampling can locate hot functions when hardware
performance counters are unavailable. Allocation profiling should run separately
from performance measurements. Profiles identify candidates; accept an
optimization only after repeated complete-pipeline measurements and regression
checks. Do not multiply improvements from separate experiments or combine results
from different revisions, payload policies, batch sizes or drivers.

The native parity, sub-100 ns complete pipeline and approximately 10 ns trace
performance goals are investigation targets, not guarantees. A faster native
backend alone does not prove native parity through existing `tracing` macros.
HTTP capture does not establish collector delivery, and a bounded in-memory queue
does not establish durability.

## Benchmark modes

`fast-telemetry-tracing` provides `bench_tracing_backends`, an **in-memory
retention** benchmark with three native fast-telemetry baselines, the composable
tracing layer, two direct-subscriber profiles, OpenTelemetry, and two controls.
The primary product target is ordinary native span performance. Exact full and
user-fields native payload controls separately diagnose metadata and frontend
cost. These controls do not replace ordinary native, and no native parity or
adapter speedup is asserted before measurements.

| Mode | Frontend and retained output |
| --- | --- |
| `native` | Public `SpanCollector::start_span`, `Span::child`, `enter`, setters, `add_event`, and drop; user payload only |
| `native-matched` | Same public native API, adding the exact fast-telemetry adapter metadata and event name |
| `native-user-fields` | Ordinary native user attributes with the exact tracing event name; no automatic metadata |
| `fast` | Shared tracing callsites through Registry and `FastTelemetryLayer` |
| `direct` | Same tracing callsites through the standalone `FastTelemetrySubscriber`, default full metadata |
| `direct-user-fields` | Same direct subscriber with explicit `MetadataPolicy::UserFields` |
| `otel` | Same tracing callsites through Registry and tracing-opentelemetry 0.32 / OpenTelemetry SDK 0.31 |
| `disabled` | Same tracing callsites with `NoSubscriber`; no retention |
| `registry` | Same tracing callsites through Registry alone; lifecycle/enter/exit without retained payloads |

`--dispatch scoped` (the default) explicitly installs a scoped subscriber on every
tracing worker. `--dispatch global` installs one process-global subscriber after
metadata capture and before warmup, and workers use that global default without
scoped overrides. Every sample starts a fresh process; a global subscriber is set
once, and no process switches measured backends mid-run. Native recording does
not run through tracing or `SpanSink`; its selected dispatch setup is still
performed outside timing for a consistent experiment configuration. Historical
native-versus-SDK results did not measure this adapter or establish equal
retention; retain their original scope.

Dispatch installation is a material benchmark input. In tracing-core 0.1.36,
scoped dispatches own an `Arc`, while the global dispatch uses a static subscriber
reference. Tracing 0.1.44 clones a dispatch when constructing an enabled span.
Workers sharing one scoped dispatch can therefore contend on that shared
reference count. A scoped Registry-only measurement is not an irreducible cost
floor for applications using a global subscriber. Compare each backend against
native and the controls under the same dispatch setting; report scoped and global
results separately. Global setup intentionally retains its subscriber for the
process lifetime; shutdown is outside timing in both settings.

## Retention and workload contract

Every recording mode must retain every completed span and event. All three FT
tracing modes use the same
bounded immediate `SpanSink` without adaptive sampling. OpenTelemetry uses
`Sampler::AlwaysOn` and a `SimpleSpanProcessor` with a bounded in-memory exporter.
Their configured `--capacity` is one shared item limit. Native uses its actual
thread-local collector, whose constructor capacity arguments are ignored and
whose shared outboxes have a fixed **4096 spans per worker** limit. These are
different queue policies, even when identical work is retained.

Each synchronized batch finishes while workers remain alive. Every native
worker calls `flush_local` **inside the timed recording interval**, including
partial batches below the automatic flush threshold. The coordinator then drains
all outboxes; OpenTelemetry `force_flush` runs in its drain interval. Workers
cannot start the next batch until drain, verification, and payload destruction
finish. No backend sends network traffic or serializes protobufs. This measures
immediate batch retention rather than a production asynchronous exporter.

Native preflight rejects a configuration whose maximum measured **or warmup**
batch exceeds 1024 completed spans per worker. Draining each batch below this
pressure boundary keeps the ordinary native adaptive sampler at full retention;
the pressure checks apply to both the record and export drivers. Both phases independently require
`sampled_out_count` delta zero and `recorded_count` delta equal to verified roots.
Native's recorded counter counts `start_span` roots, not `Span::child` children,
and its legacy dropped counter always returns zero. Exact inspection of **all
actual drained completed spans** is required in addition to those counters.
`--capacity` still appears in native rows to identify the shared experiment
configuration, but does not configure native queues.

| Workload | Spans / iteration | Events / iteration | User payload |
| --- | ---: | ---: | --- |
| `root` | 1 | 0 | Root lifecycle/enter/exit |
| `attributes` | 2 | 1 | Root worker/iteration, child method/code, event ok/message |
| `children` | 4 | 0 | Root and three distinct child names |

Native and tracing explicitly enter every span. The ordinary native mode omits
automatic tracing metadata and names its event `checkpoint`; this is the standard
native API target, with a deliberately smaller output than the adapter. The
`native-matched`, `fast`, and `direct` modes retain the same logical span/event/attribute
payloads, including level, target, module, file, line, typed user values, and the
tracing event name. Native static strings are naturally borrowed; the benchmark
does not add string copies to imitate adapter ownership costs.

`direct-user-fields` deliberately excludes automatic metadata attributes while
preserving user fields and tracing span/event names. Its exact native control is
`native-user-fields`, which uses the ordinary public native API with the
pre-resolved tracing event name. These two modes retain identical logical
payloads. Ordinary `native` retains its natural `checkpoint` event name and
remains the primary product target; do not present a smaller profile as default
full-metadata parity.

Before backend construction and timing, a metadata-only Layer executes the
selected workload's exact shared tracing callsites once in **every mode**. It
captures static metadata separately for that workload, including the correct
root callsite and event. Native matched recording uses resolved static references
and appends metadata through public native setters during timing. Metadata lookup
and semantic verification are outside recording; payload construction,
publication, and native flushes are inside. This setup introduces the same
callsite registration before warmup in all modes.

| Mode | Total attributes / iteration: root | attributes | children |
| --- | ---: | ---: | ---: |
| `native` | 0 | 6 | 0 |
| `native-user-fields`, `direct-user-fields` | 0 | 6 | 0 |
| `native-matched`, `fast`, `direct` | 5 | 21 | 20 |
| `disabled`, `registry` | 0 | 0 | 0 |

The totals include event attributes and the event's `message` field.
OpenTelemetry uses different metadata keys and represents the message as its
event name; its actual attribute total is reported, without claiming identical
metadata. Its thread, activity-time, and context-activation extras are disabled
because this adapter does not provide them.

Every batch verifies actual span/event/root/child/user-field counts, names,
unique IDs, and parent/trace relationships. All FT and native modes also check
exact attribute keys, typed values, metadata values, event placement/name and
lifecycle timestamps. Worker/iteration payloads must be unique and within the
current batch. OpenTelemetry payload-limit loss and both bounded sink rejection
counters must be zero; accepted counter deltas must equal actual drained spans.
Controls must drain zero spans/events. A mismatch exits nonzero. Regressions cover
all modes/workloads, partial native batches with live workers, the native pressure
boundary, warmup pressure rejection, and insufficient bounded sink capacity.

This benchmark does not exercise large integer conversion, async polling, remote
propagation, filtering, standalone logs, or durability. Focused tests cover those
semantics separately. Different frontend semantics and queue policies remain
material limitations even when payloads match.

## Run the backend comparison

Run the host's resource checks before building or measuring. Rust 1.93 or newer
is required. Timing benchmarks are separate from the CI correctness checks.

```sh
cargo test -p fast-telemetry-tracing --all-features
RUNS=5 THREADS='1 2 4 8' ITERS=10000 WARMUP=100 BATCH=64 CAPACITY=4096 \
  bash crates/fast-telemetry-tracing/scripts/benchmark.sh \
  /absolute/path/to/owned-run/tracing-results
```

The script requires a clean committed worktree, builds once in release mode,
then runs fresh samples with rotated mode order across repetitions. `MODES` can
select a subset; the default includes all nine modes. `DISPATCHES` defaults to
`scoped`; set `DISPATCHES='scoped global'` to measure both installations in
separate fresh processes, or `DISPATCHES=global` for the global control. It retains commands,
timestamps, CSV rows, stderr, revision, host, kernel, Rust/Cargo versions, build
log, binary checksum, dependency tree, and a copy/checksum of the resolved ignored
Cargo.lock. `all-runs.csv` preserves every sample; `medians.csv` summarizes them.
`comparisons.csv` reports each FT tracing mode over native, native-matched, and
OTel CPU cost, plus direct-user-fields/native-user-fields, separately for each
dispatch setting. The `numerator` and `denominator` columns identify each ratio;
a ratio above one means the numerator costs more CPU per retained span.
Controls are excluded from these ratios.

`ITERS` and `WARMUP` are **per thread**. For FT tracing/OTel, keep capacity at least
`threads × min(batch, max(iterations, warmup)) × spans_per_iteration` to retain
both phases; undersizing must fail. Native separately requires
`min(batch, max(iterations, warmup)) × spans_per_iteration <= 1024` per worker.
The default batch 64 creates at most 256 spans per native worker; eight workers
create at most 2048 spans per shared sink batch.

For an individual sample:

```sh
cargo run --release -p fast-telemetry-tracing --features bench-tools \
  --bin bench_tracing_backends -- \
  --mode native --workload attributes --threads 4 --iters 10000 \
  --warmup 100 --batch 64 --capacity 4096
```

For single-thread hot-path measurements, add `--execution inline --threads 1`.
This runs recording and draining on the calling thread, removing worker barriers
and wakeups from the recording interval. It uses the same instrumentation,
warmup, explicit native flush, payload verification, counters, and timed payload
destruction. Native sampling pressure limits still apply. The default
`--execution workers` retains the resident-worker experiment and supports
multiple threads. Results include an `execution` column; compare the same driver.

The script accepts `EXECUTION=inline THREADS=1`. Its summaries keep execution
drivers separate. To interpret the requested 10–100 ns target, report both
`pipeline_with_destroy_cpu_ns_per_span` and
`pipeline_with_destroy_cpu_ns_per_iteration`: a `children` iteration is a complete
four-span trace. Neither metric includes encoding or network export.

`--drain-storage fresh` is the default and drops the drained vectors after each
batch. `--drain-storage reuse` clears every actual span and event payload inside
the destruction interval, retaining the empty vector allocations across batches
and warmup. The production span exporter also clears and reuses its drain vector.
For OpenTelemetry, reusable storage is returned to the exporter queue at the
next drain. Final empty buffer release is outside the per-batch destruction
interval. Recording, verification and retention requirements are identical.
The script accepts `DRAIN_STORAGE=reuse`; CSV output and summary groups identify
the `drain_storage` policy. Keep fresh and reuse results separate.

Add `--dispatch global` for the global installation. Repeat with
`native-matched`, `native-user-fields`, `fast`, `direct`, `direct-user-fields`,
`otel`, `disabled`, and `registry`. Fresh
processes and repeated samples are needed for conclusions; one debug run or a
submission counter is not performance evidence. Preserve the run's lockfile when
reproducing dependency versions.

## Read the timings

The **same resident workers** execute warmup and measurement, preserving warmed
TLS and queue storage without registering extra native warmup outboxes. Warmup
uses the same retention/verification/destruction policy, is fully drained, and
is excluded from reported intervals and measured counter deltas. A final barrier
keeps workers alive through the last batch's drain and payload destruction.

| Output | Interval or denominator |
| --- | --- |
| `dispatch` | `scoped` worker defaults or one `global` process default; never aggregate them together |
| `record_wall_ns`, `record_cpu_ns` | Synchronized recording intervals, including barrier coordination and required native worker flushes |
| `drain_wall_ns`, `drain_cpu_ns` | Moving retained structures out of queues, including OTel force_flush |
| `verify_wall_ns`, `verify_cpu_ns` | Semantic checks and creation/destruction of verifier temporary structures; excludes destruction of actual retained payloads |
| `destroy_wall_ns`, `destroy_cpu_ns` | Dropping actual drained payloads after borrowed verification returns |
| `total_wall_ns` | Measured phase through final worker join, including all phases/counter checks/coordination; excludes metadata/backend setup and warmup; with positive warmup, initial worker setup is also excluded |
| `record_cpu_ns_per_iteration`, `record_cpu_ns_per_span` | Record CPU divided by workload iterations or actual retained spans |
| `pipeline_cpu_ns_per_iteration`, `pipeline_cpu_ns_per_span` | Record plus drain CPU; verification and payload destruction excluded, preserving the earlier pipeline definition |
| `pipeline_with_destroy_cpu_ns_per_iteration`, `pipeline_with_destroy_cpu_ns_per_span` | Record plus drain plus actual payload destruction CPU; the fuller retained-payload lifecycle diagnostic |
| `attempted_spans` | Work iterations × spans per iteration in every mode, including no-retention controls |
| `payload_attributes` | Actual total span and event attributes, including metadata and message fields |
| `sampled_out`, `recorded_roots` | Native measured sampler/root counter deltas; integer zero for non-native modes, whose root counter is inapplicable |

Per-retained-span metrics are `NA` for both controls. Compare controls by CPU per
iteration or derive CPU per attempted span explicitly; no-retention ratios do not
establish full-retention parity. Report each FT backend/native **and** its exact
native payload control side by side, emphasizing record+flush+drain and
record+flush+drain+destruction.
Ordinary native remains the product performance target. The latter matched
comparison diagnoses metadata cost, not parity with the smaller native output.

CPU is process CPU time from `CLOCK_PROCESS_CPUTIME_ID`, summed over all process
threads; wall time uses `Instant`. CPU is not elapsed latency, and an iteration
may create multiple spans. Barrier overhead matters for small batches. Compare
the same batch, workload and thread count and preserve the configuration.
Derived wall rates must name their interval: record, record+drain,
record+drain+destroy, or total (which includes verification).
With `WARMUP=0`, the first timed barrier may include workers' initial arrival, so
record and total intervals can include that startup delay.

SDK conversion/export occurs during OTel span closure; FT `CompletedSpan`
construction/publication occurs during closure; native public APIs construct and
buffer the same completed structures on their actual native path. Production
payload work is never deferred to verification. Backend/queue shutdown and network
export are outside these lifecycle metrics. The split destruction interval and
resident warmup differ from the older benchmark revision; historical verify and
total columns must retain their original interpretation. No cycles, universal
speed ratio, full OpenTelemetry API parity, or durable-delivery claim follows
from these results.

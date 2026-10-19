# Tracing performance review — 2026-10-19

The final pass measures the retained implementation, including checked native TLS
borrowing. Native parity through existing tracing macros remains unproven. The
approximately 10 ns complete-trace target is not achieved. These shared-host
measurements support review of this proposal, not a universal speed claim.

## Complete sender pipeline

Six repetitions cover all process-order permutations for native fast-telemetry,
the direct tracing subscriber with explicit `UserFields`, and tracing/OpenTelemetry.
There are 108 successful fresh processes, 28,311,552 verified measured spans and
55,296 encoded requests, with zero sampling or queue loss. Each export contains
512 spans; each process runs 512 measured cycles after 16 warmup cycles.

Median **process CPU ns per complete trace**, including recording, collection,
real OTLP encoding, HTTP request construction and actual payload destruction:

| Workload | Producers | Native | Tracing UserFields | Tracing/OTel 0.30 | Paired OTel / UserFields CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| Root | 1 | 123 | 217 | 1066 | 4.90× |
| Root + three children | 1 | 489 | 938 | 4936 | 5.25× |
| Two spans + attributes/event | 1 | 795 | 1240 | 4876 | 3.95× |
| Root | 8 | 436 | 555 | 2540 | 4.56× |
| Root + three children | 8 | 1692 | 2343 | 10934 | 4.66× |
| Two spans + attributes/event | 8 | 1888 | 2417 | 7802 | 3.22× |

The root workload contains one span, children contains four, and attributes
contains two plus an event. Per-span CPU and wall throughput remain in the raw
CSV. Process CPU sums all threads; it is not individual request latency. One
producer runs inline; eight producers use resident workers and cycle barriers.

These are capture-mode sender measurements: no sockets, compression, TLS, retries
or actual collector processing are measured. The SDK uses its real background
BatchSpanProcessor and OTLP exporter with a common capture HTTP client. The native
and UserFields paths encode directly. Their resource, scope and user payloads
match; protocol flag/status representation differences remain and are validated.
Setup, warmup, semantic/wire verification and final empty-buffer release are
outside the reported intervals. Verification pacing can affect scheduling.

Measured source: `e5adb39bc115de5a6311639d6334eb741fb912b1`. The pinned stack is
tracing 0.1.44, tracing-subscriber 0.3.23, tracing-opentelemetry 0.31.0,
OpenTelemetry API/SDK/OTLP 0.30.0, prost 0.13.5 and reqwest 0.12.28.
The later two-line hex-iterator cleanup retains this measurement’s original
source identity. See [raw sender samples](sender-all-runs.csv),
[paired summaries](sender-summary.json) and [provenance](metadata.json).

## Default Full-metadata lifecycle controls

A separate 216-process comparison measures native recording, required local
flush, drain and actual destruction, excluding encoding and transport. It uses
six rotated mode orders, 16,384 iterations per producer, 256 warmup iterations,
batch 64, capacity 4,096, global dispatch and reused drain storage. All
37,158,912 measured spans retain their verified payloads with zero sampling/loss.

Median **process CPU ns per complete trace**:

| Workload | Producers / driver | Ordinary native | Matched Full native | Direct Full | Layer Full | Direct UserFields |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Root | 1 / inline | 130 | 265 | 307 | 434 | 226 |
| Root + three children | 1 / inline | 376 | 887 | 1135 | 1903 | 832 |
| Two spans + attributes/event | 1 / inline | 410 | 629 | 969 | 1383 | 735 |
| Root | 8 / workers | 351 | 688 | 663 | 881 | 445 |
| Root + three children | 8 / workers | 839 | 1936 | 1941 | 3612 | 1285 |
| Two spans + attributes/event | 8 / workers | 981 | 1669 | 1986 | 2635 | 1499 |

The raw data also includes the exact native UserFields control. Full/matched
and UserFields/native are distinct comparisons. Default Full adds automatic
tracing metadata; the smaller UserFields profile is explicit. Native API and
adapter publication policies remain different. No blanket native-parity claim
follows from the few close Full/matched cases. Do not compare these lifecycle
cells directly with the encoded sender table.

These diagnostic controls use current source
`893acf3fc1656c4e54b3c1057241f4f2104d2525` and genuine offline Cargo dependencies.
The isolated build uses the available SDK 0.30/bridge 0.31 stack and omits two
unsupported setup calls **only in the unmeasured OTel branch**. No OTel mode is
reported here. Instrumentation, payload verification and measured FT paths are
unchanged. The [driver patch](lifecycle-driver.patch) records that limitation.
The repository benchmark’s SDK 0.31 configuration separately passes GitHub CI.
See [all lifecycle samples](lifecycle-all-runs.csv) and
[summaries](lifecycle-summary.json).

## Profiles and retained decisions

Eighteen final phase/allocation profiles verify 37,896,192 measured spans and
collect 25,358 CPU instruction-pointer samples. Setup, warmup and semantic
verification are gated out of the phase profiles; four additional Full-policy
worker profiles include worker setup/warmup/flush and exclude coordinator work.
Instrumented timings are excluded from the tables above. Sampled libc addresses
have incomplete symbols, so individual nearest-symbol names are not treated as
precise libc function cost attribution.

At eight native producers with 4,096 spans per export, attributed encoding uses
about 56% of the instrumented elapsed batch interval and actual cleanup about
20%. Plain native record samples frequently land in the OS vDSO clock, ID
generation and active-record materialization. Direct and Registry layer worker
profiles retain lock, metadata and allocation work; the SDK profiles show shared
reference-count and channel work plus background encoding.

Separate warmed allocation profiling records four allocations / 752 requested
bytes per native attributed trace, seven / 570 for UserFields tracing and eleven
/ 1,803 on the SDK producer path, with additional SDK background allocations.
These count allocation traffic, not live or resident memory. Plain native
recording remains allocation-free after warmup in its regression fixture.

The final cleanup keeps the existing measured bucket/encoder optimizations,
removes redundant Option state from direct event attachment, and replaces
unchecked native TLS access with checked borrowing. Reentrant allocator callbacks
now fail before creating aliased mutable references. The regression verifies
rejection and recovery. A separate 84-sample, seven-pair comparison charges the
guard’s safety cost, including actual cleanup. Median paired CPU changes:

| Workload | Recording pipeline | Pipeline including native encoding |
| --- | ---: | ---: |
| Root | +8.26% | +6.78% |
| Root + three children | +2.93% | +2.48% |
| Attributed single root | +1.97% | +1.60% |

This guard-cost fixture uses one pinned producer, batches of 512 plain roots,
128 four-span traces or 256 owned attributed single roots, and 128 verified
cycles per sample. Its attributed payload differs from the two-span sender
fixture. Positive values are additional CPU, not a speedup. The safety guard is
retained despite that cost. [All guard samples](borrow-guard-all-runs.csv) and
[summaries](borrow-guard-summary.json) preserve every successful timing sample.
The [baseline patch](borrow-guard-baseline.patch), applied to e5adb39, reconstructs
the unchecked comparison source for valid non-reentrant benchmark workloads only.

Profiles point toward attribute/event storage reuse and parallel export work,
but this pass adds no new page, timestamp-source or sampling architecture. Such
changes need their own retention, ownership, memory-bound and complete-pipeline
validation. Profiling percentages are evidence for investigation, not measured
gains from unimplemented changes.

## Reproduce and validate

Build the [pinned sender workspace](../../README.md) and run:

```sh
python3 benchmarks/tracing-end-to-end/run.py \
  --binary benchmarks/tracing-end-to-end/target/release/tracing-end-to-end \
  --output /absolute/path/to/new-results --threads 1 8 \
  --export-batch 512 --cycles 512 --warmup 16 --repeats 6 --transport capture
```

Use the [backend guide](../../../../docs/tracing-benchmark.md) for the lifecycle
driver, matching the settings above and retaining each exact dependency lock.
Measured binaries were optimized with Rust 1.93.1 on a shared Ryzen 9 3950X
Linux host without CPU isolation; different code layout and host scheduling
can shift results. The source manifests, compiler details, binary hashes and
measurement boundaries are in [metadata](metadata.json); file hashes are in
[the artifact manifest](manifest.sha256.json).

All seven [source-head GitHub CI checks](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39183667526)
pass, including exact MSRV 1.93.0, workspace checks/tests/Clippy, documentation,
formatting and the pinned sender verifier/short capture matrix. The full-feature
core suite passes 247 unit tests. CI also runs the real HTTP acknowledgement and
drain/refill tests, which cannot bind listeners in the local sandbox. This is
engineering validation for public review, not approval, release or merge.

# Configurable native producer batching — 2026-10-19

Native producer publication now defaults to **384 completed spans per OS thread**, configurable from 1 to 1,024. This follows the strongest measured bend at 256 spans for plain traces and 384 for attributed traces. Larger sizes remain useful for plain bulk workloads; the default balances them with attributed cost, unpublished data and initial allocation size. It is a publication target, not a hard per-producer HTTP request size.

```rust
use std::sync::Arc;
use fast_telemetry::{SpanCollector, SpanCollectorConfig};

let collector = Arc::new(SpanCollector::with_config(
    SpanCollectorConfig::default().with_producer_batch_size(512),
));
```

`SpanCollector::new(shards, capacity)` retains its existing signature and uses the new default; its historical arguments remain ignored. A shared runtime accepts the same policy through `RuntimeConfig::default().with_span_config(config)`. The policy is immutable for a collector and applies to all its current/future producers. Explicit local flush and thread exit publish smaller batches; external tracing sinks retain immediate publication.

## Measured curve and selection

Eight resident native producers, process CPU ns per **complete trace**. A root has one span; children four spans; attributes two spans plus an event. All seven sizes use the unchanged source `810c8f1def4cd7c92faeb5dc350eee9b98b7bbb0`, whose automatic publication target is 256. Each application/export burst explicitly flushes its final partial buffer. This sweep changes the burst size, including synchronization/request frequency; it does not isolate automatic publication size.

| Spans/producer/export burst | Root | Four-span trace | Attributed trace |
| ---: | ---: | ---: | ---: |
| 64 | 445.6 | 1776.0 | 1728.9 |
| 128 | 323.5 | 1250.1 | 1499.8 |
| 256 | 248.5 | 1006.7 | 1301.4 |
| 384 | 233.2 | 909.0 | 1189.9 |
| 512 | 226.1 | 869.3 | 1133.9 |
| 768 | 201.2 | 796.0 | 1150.2 |
| 1024 | 182.5 | 699.5 | 1148.5 |

For each fixture, normalize batch size across the measured 64–1,024 range and CPU improvement from batch 64 to the best observed CPU. The sample with the largest gap above the normalized endpoint chord is the discrete knee: **256 roots, 256 four-span traces, 384 attributed traces**. `summarize.py` retains every distance and implements the calculation. This is a transparent selection heuristic on a shared-host curve, not a universal optimum or precise latency knee.

Choose the upper measured knee, 384, as the shared default. Attributed cost improves another roughly 5% from 384 to 512, then levels off; plain traces continue benefiting through 1024. Users prioritizing bulk plain throughput can select 512/768/1024, and latency-sensitive users can choose smaller targets. The two initial native producer allocations reserve 384 rather than 256 slots each, a 50% increase in initial slot reservation. Their 4,096-slot maximum and the 4,096-span queued-payload budget are unchanged. Queues may grow beyond the target when the other bucket is busy.

## Separate controls for the actual implementation

A second comparison holds the application/export burst fixed at 384 spans per producer. It compares unchanged source (fixed 256), the new configurable implementation explicitly set to 256, and its new 384 default. Six cyclic/reverse process orders balance every variant at every position within each fixture. CPU values are group medians; changes are medians of paired percentages relative to unchanged source.

| Producers | Workload | Before ns/trace | New default ns/trace | Configured 256 CPU change | Default 384 CPU change |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | Root | 142.1 | 142.4 | +0.59% | +0.62% |
| 1 | Children | 554.1 | 558.5 | +0.83% | +0.83% |
| 1 | Attributes | 811.9 | 827.2 | +0.80% | +1.64% |
| 8 | Root | 236.0 | 226.9 | +0.94% | -3.88% |
| 8 | Children | 920.2 | 884.2 | +1.85% | -4.30% |
| 8 | Attributes | 1226.7 | 1212.9 | -0.98% | -0.02% |

The configuration mechanism is within roughly 1–2% of the old implementation at the same 256 target, with paired ranges spanning zero. Default 384 gives a modest eight-producer plain improvement at this fixed burst; attributed work is effectively unchanged at eight producers and about 1.6% higher CPU at one producer. Raw paired ranges remain in `summary.json`. The much larger gains from increasing complete export bursts include amortized benchmark barriers and request-cycle work; **changing this default does not claim those large speedups**. No application-latency, async-interleaving, default Full-policy or network-performance result is claimed.

## Publication, latency and HTTP limits

The setting counts completed spans, including children, per OS thread. It does not impose a flush timer or promise exact-sized batches. Low-volume producers need an application-defined local-flush deadline, including before shutdown if their threads remain alive. The exporter cannot force a flush of another live thread's private buffer. Native sampling, root counters, bounded rejection and the guarded late-TLS completion path retain their existing policies.

The exporter's separate `SpanExportConfig::max_batch_size` defaults to 512 **total** spans per drain/request, and currently discards excess drained spans. Size it for aggregate drain volume: eight full default producer batches already contain 3,072 spans. Increasing producer sizes alone does not increase that cap. The exporter already coalesces available buffers on its interval; the producer policy does not delay export until every producer reaches its target. Payload memory, coalescing delay and receiver limits still determine an appropriate application configuration.

## Validation and provenance

The selection matrix has **126 fresh successful processes**, and the implementation controls **108**, totaling **46,006,272 verified measured spans**. Every sample checks complete resource/scope/span/event/attribute payloads, IDs, relationships, lifecycle times and accounting, with zero native sampling and queue loss. No timing rows were filtered. Each process measures 196,608 spans after 16 warmup cycles. Seven sizes run in three cyclic orders and their reverses; the order is not a full 14-order positional balance. Controls use all six cyclic/reverse orders for three variants.

Local genuine cached-dependency Cargo diagnostics pass 248 core unit tests, five new public config regressions, three existing allocation/ownership regressions and ten doctests (five pre-existing ignored), plus core/driver Clippy with warnings denied and all eight sender verifier tests. Regression coverage includes first-publication boundaries at 1/17/256/384/512/1024, repeated drain/refill, live workers with a shared policy, OS-thread-exit partial publication, explicit local flush, invalid sizes, Runtime wiring, full-queue loss/recovery at multiple targets and warmed zero-allocation recording. Normal full-feature workspace/MSRV/package checks remain the authority in [PR#40 CI](https://github.com/eden-dev-inc/fast-telemetry/pull/40/checks); CI also verifies every configured target across all three fixtures at 1/8/16 producers.

Linux x86_64 Ryzen 9 3950X, Rust 1.93.1, affinity CPUs 0–15, same authenticated registry package versions/sources/checksums for compared binaries. Release builds use `RUSTFLAGS=-Dwarnings`, ordinary optimization and stripping. Capture includes recording/context entry, local flush/drain, real OTLP encoding, HTTP request construction and actual native/wire destruction. Setup/warmup, semantic/wire verification, compression, sockets/TLS, retries, collector processing and final empty-vector release are excluded. Process CPU sums all threads. Shared load and scheduling affect the results.

Baseline source is 810c8f1. Candidate identity is that commit plus [implementation.patch](implementation.patch), with exact source, binary, manifest and lock hashes in [metadata.json](metadata.json). Tests and runtime/driver sources match the frozen measured candidate. Isolated manifests omit unavailable unused workspace dev dependencies; this is not a claim that those manifests qualify all workspace features. Original commands, logs, raw process output, immutable source snapshots and strict host resource-guard receipts are retained in the associated local evidence directory. Portable runners below preserve fixture orders and arguments, check 20 GB free disk and honor a fresh host guard when present. Work paused between stages when admission closed; no benchmark was started below the floor.

## Reproduction

Build the unchanged sender at 810c8f1 and the candidate sender at this source revision with their pinned `benchmarks/tracing-end-to-end/Cargo.lock`. The report's isolated package names/paths differ from normal checkout builds; hashes identify the actual measured binaries rather than promising bit-identical rebuilds. Reproduce the fixture comparisons with:

```sh
python3 run-knee.py --binary /path/to/before-sender --output /path/to/sweep
python3 run-confirmation.py --baseline /path/to/before-sender \
  --candidate /path/to/candidate-sender --output /path/to/controls
```

Copy the resulting `all-runs.csv` files to `baseline-runs.csv` and `confirmation-runs.csv` respectively, then run `python3 summarize.py`. `manifest.sha256.json` hashes the retained report artifacts except itself. See [native configuration](../../../../docs/metrics-and-spans.md#spans), [runtime configuration](../../../../docs/runtime.md), and [release notes](../../../../RELEASE_NOTES.md).

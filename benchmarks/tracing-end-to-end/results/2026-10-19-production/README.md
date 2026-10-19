# Production review — 2026-10-19

This pass found and fixed three native context/shutdown bugs before release.
The measured candidate is `3724f95b5a7caafd3c0083fdf81f4b36cea54031`.
All eight CI jobs pass for that candidate, including building the four extracted
crate archives. Later report/docs changes preserve the measured runtime.

## Findings and fixes

- An entered native span could move to another thread. Its destructor then
  overwrote that thread's current context and left the origin's context stale.
  `Span::enter()` now returns a scoped, thread-bound `EnteredSpan` guard.
  Unentered `Span` remains `Send + Sync` and can move to scoped threads.
- Closing an entered parent before its child cleared the active child; closing
  the child then restored the already closed parent. Entries now have unique
  tokens in a reused TLS stack. Removing an older entry preserves newer ones
  and cannot resurrect a closed entry.
- Dropping a native span from a user TLS destructor after collector TLS had
  been destroyed aborted the process (`AccessError`, exit 134 in the reproducer).
  Late completions now use one bounded shared writer per collector and remain
  drainable. Context queries tolerate destroyed TLS. The normal path does not
  lock the shutdown writer.

The first two bugs were reproduced with failing assertions before the fix.
Regression coverage checks out-of-order parent/child completion, explicit end
without double submission, nested entry, moving and reentering after guard
closure, and both retained-span completion and new recording during user TLS
shutdown. A compile-fail example rejects moving an entry guard to another thread.
The native guide and release notes document the API migration: bind the returned
guard, use it while entered, and close it before moving the span or awaiting.

The packaging audit also found missing license text in the archives and relative
README links that depend on the source workspace. All four archives now carry
the license, published README links point outside the archive, and a new CI job
packages/builds the release crates and checks license equality.

## Complete sender measurements

There are **300 fresh successful matrix processes**, 78,643,200 verified measured
spans, 45,875,200 complete traces, 13,107,200 events and 153,600 requests. Every
sample reports zero sampling and queue loss. All 300 raw timing rows are retained;
there is no outlier filtering. Each process runs 512 measured cycles after 16
warmup cycles, with 512 spans per export and either one inline producer or eight
resident producer threads. Five cyclic orders followed by their reverses put
every variant twice at every process position within each fixture.

Median **process CPU ns per complete trace**:

| Workload | Producers | Original native | Reviewed native | Final native | Tracing UserFields | Tracing/OTel 0.30 | Paired original / final |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Root | 1 | 356 | 125 | 139 | 217 | 1090 | 2.54× |
| Root + three children | 1 | 1593 | 490 | 545 | 948 | 4971 | 2.95× |
| Two spans + attributes/event | 1 | 2639 | 821 | 815 | 1202 | 4835 | 3.17× |
| Root | 8 | 635 | 425 | 453 | 546 | 2536 | 1.42× |
| Root + three children | 8 | 2695 | 1756 | 1693 | 2295 | 10483 | 1.59× |
| Two spans + attributes/event | 8 | 3604 | 1802 | 1828 | 2329 | 7884 | 2.01× |

“Original” is `9856da3d61a9161ba6f1a4dcc6283aba0efef2af`, before the native/export
optimization work. “Reviewed” is `6f74339a4ad39d99dd74d4589a351479db7cd9d4`, before
this production review. The original driver uses the original flat drain,
`CompletedSpan::to_otlp`, protobuf builder and prost encoding path; its minimal
API/encoder adaptation is retained in [original-driver.patch](original-driver.patch).
All variants share instrumentation, exact payload verification, timing boundaries,
HTTP envelope and actual cleanup. The baseline keeps its collector alive for all
spans, so its former lifetime hazard is not exercised in these timing runs.

Final native uses about **1.4–3.2× less CPU than original native** across these
fixtures, with all ten paired ratios above 1 in every cell. The final explicit
UserFields sender uses about **3.4–5.3× less CPU than the pinned OTel sender**
(median paired ratios). Default Full metadata remains a different payload policy;
these figures do not establish Full-policy or native parity.

The context/shutdown fixes cost about **12.0% for a single-producer root** and
**10.8% for a four-span trace** relative to the reviewed version (paired medians).
Attributed two-span traces cost 2.9% in the paired comparison. Eight-producer
paired changes are +3.6% roots, +0.2% children and -0.8% attributes; noisy ranges
are retained in [summary.json](summary.json). These figures describe the combined
review fixes; they do not isolate individual instructions or establish a reliable
multi-producer improvement. Median ratios differ from ratios of cell medians.
Do not add these percentages to the earlier checked-borrow experiment.

## Boundaries and reproduction

The shared host is Linux x86_64 on Ryzen 9 3950X, Rust 1.93.1. All variants use
the same CPU affinity (physical cores 0–15) and registry dependency versions.
The pinned SDK/API/OTLP is 0.30, bridge 0.31, tracing 0.1.44, subscriber 0.3.23,
prost 0.13.5 and reqwest 0.12.28. Source and binary hashes are in
[metadata.json](metadata.json).

Capture mode charges recording, required local flush, drain, real OTLP encoding,
HTTP request construction and actual native/wire-payload destruction. Process
CPU sums all threads. It excludes setup, warmup, semantic/wire verification,
compression, sockets, TLS, retries, collector processing and final empty-buffer
release. Verification pacing, shared load and scheduling affect these results.
Cold registration, new maximum context depth and metadata cache misses may
allocate; the matrix measures warmed reuse. This is not an application latency
or universal speedup claim. The approximately 10 ns complete-trace goal and
native parity through existing tracing macros remain unachieved.

Build the pinned sender at the candidate and reviewed revisions with normal
`cargo build --release --locked --manifest-path benchmarks/tracing-end-to-end/Cargo.toml`.
For the original, keep its library sources at the original revision and build a
copy of the candidate harness against those paths after applying the zero-context
patch (`git apply --unidiff-zero`). Start from the candidate pinned Cargo.lock;
Cargo updates the local package dependency lists, with identical registry package
names, versions and checksums. The metadata records the resulting lock hash.
Run the full matrix sequentially with:

```sh
python3 run-review.py --baseline /path/to/original-sender \
  --before /path/to/reviewed-sender --candidate /path/to/final-sender \
  --output /path/to/samples --affinity 0-15
```

The runner checks free disk and honors a host resource guard when present.
Copy `samples/all-runs.csv` here and run `python3 summarize.py` to recompute all
medians and paired ranges. [manifest.sha256.json](manifest.sha256.json) hashes
all report artifacts except itself.

## Release checks

[Candidate CI](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39185926401)
passes MSRV 1.93.0, workspace check/tests/Clippy, rustdoc, formatting, the pinned
sender verifier/short matrix and full-feature crate packaging. The full-feature
core suite passes 250 unit tests, including the three new regression tests.
Coverage also includes actual HTTP acknowledgements, native counts, adapter
lifecycle/capacity, drain/refill, retained owned payloads and direct encoder byte
parity. The new native entry example compiles and checks context restoration.

Local cached-dependency diagnostics pass 248 core unit tests, three native
allocation/ownership regressions, entry/lifetime doctests and Clippy; the pinned
sender passes its eight verification tests and Clippy. Full workspace/package
verification ran in CI because local registry archives are incomplete. Miri,
other architectures and downstream application deployment were not exercised.

See [publishing guidance](../../../../docs/publishing.md),
[release notes](../../../../RELEASE_NOTES.md), and the earlier
[Full-policy lifecycle/profiling report](../2026-10-19-capture/README.md). The
historical report keeps its original source identity. The built-in exporter
remains best effort; no durable delivery or automatic failed-batch replay is
claimed. This review does not publish crates or merge the PR.

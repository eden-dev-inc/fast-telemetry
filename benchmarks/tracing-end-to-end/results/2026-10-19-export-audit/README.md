# Span export correctness and encoding audit — 2026-10-19

The configurable 384-span native producer target exposed an existing delivery
problem: periodic span export truncated each drain at its default 512-span cap,
while shutdown ignored the cap entirely. Both paths now attempt the complete
drain as bounded requests, preserving drain order. No request is replayed, and
a failed or partially rejected chunk does not prevent later chunks from being
attempted. Any such failure backs off the following cycle; an empty drain does
not reset failure history. Native span payloads are released before network I/O.

## Correctness evidence

The regression-only commit
[`fd27daf`](https://github.com/eden-dev-inc/fast-telemetry/commit/fd27dafa563536bff7153e9bfcd303ce288cb180)
first reproduced both bugs in
[CI run 39253058799](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39253058799).
Three producers each publish 384 spans, followed by one partial span: 1,153
unique spans with owned names, attributes, error messages and event payloads.
Periodic export attempted only 512 spans; shutdown produced an oversized request.

At fixed source
[`f850b2b`](https://github.com/eden-dev-inc/fast-telemetry/commit/f850b2b548633e3d773f0f4c17faec90e4261906),
[all eight CI jobs pass](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39254520438).
Periodic and final export produce **512 + 512 + 129** spans, including when the
middle request returns HTTP 503 or OTLP partial success. Payload checks conserve
all names/IDs and verify native ownership fields. Golden tests compare complete
protobuf bytes across native producer and external sink buffers, including
limits around the 16–512-span length cache, partial tails and unlimited requests.
Core CI passes 253 unit tests; the HTTP suite passes all eight tests under both
all features and the minimal `otlp-logs` configuration. CI also checks MSRV,
Clippy, docs, formatting, 81 sender captures and packaging all four release crates.

Zero request limits and export intervals are rejected before draining. Backoff
jitter now varies in both directions and never exceeds its five-minute maximum.
Deterministic boundary tests cover those cases. The span exporter rustdoc example
is now compiled instead of ignored. Local genuine cached-dependency diagnostics
also pass 251 core unit tests, five public configuration regressions, three
allocation/ownership regressions and Clippy; CI remains the full-feature authority.

## Encoding comparison

An additional **96 fresh processes / 100,728,144 verified span encodings** compare
one unlimited request against 512-span requests. Both paths retain the complete
input, unlike the old truncated periodic export. These are encoding/body-cleanup
measurements, not production HTTP throughput or a before/after speedup for the
delivery fix. Every result is checked against complete prost golden request bytes.

| Fixture | Drain spans | One request ns/span | 512-span chunks ns/span | Paired CPU change | Chunk requests |
| --- | ---: | ---: | ---: | ---: | ---: |
| Borrowed plain | 512 | 21.9 | 22.0 | +0.1% | 1 |
| Borrowed plain | 1,153 | 21.6 | 20.7 | -4.4% | 3 |
| Borrowed plain | 3,072 | 21.5 | 20.1 | -5.8% | 6 |
| Borrowed plain | 8,192 | 20.7 | 36.7 | +73.3% | 16 |
| Owned attributes/event | 512 | 171.1 | 169.5 | -1.2% | 1 |
| Owned attributes/event | 1,153 | 195.3 | 171.9 | -13.6% | 3 |
| Owned attributes/event | 3,072 | 195.2 | 166.6 | -13.5% | 6 |
| Owned attributes/event | 8,192 | 202.8 | 261.4 | +30.3% | 16 |

Values are group medians; percentage changes are medians of six paired ratios.
Plain improvements at 1,153/3,072 span paired ranges cross zero. The corresponding
attributed ranges are -15.7% to -10.7% and -29.3% to -7.0%. Both 8,192-span
regressions occur in every pair. All ranges and rows are retained in
[`encode-summary.json`](encode-summary.json) and [`encode-runs.csv`](encode-runs.csv).
Additional resource/scope framing increases protobuf bytes by at most 0.20% in
these fixtures; HTTP framing and extra round trips are excluded and can matter more.

The bounded path uses the existing stack length cache rather than recomputing
top-level lengths for large requests. This helps medium attributed drains, but
more independently allocated bodies make the largest drain slower on this host.
The request cap remains configurable; this audit does not select a new HTTP cap.

## Further optimization opportunities

Four separate instrumented 8,192-span profiles verify another 67,108,864 span
encodings. They are diagnostics, not additional timing samples. The chunked plain
fixture spends about 9.7% of its measured encode/cleanup CPU destroying bodies;
the attributed fixture spends about 5.5%. Most of the regression is in encoding.
Chunked encoding samples shift toward copying and unresolved libc work, while
attributed `value_len` remains prominent. We retain addresses without assigning
unresolved libc offsets to guessed functions. See
[`profile-phases.csv`](profile-phases.csv) and
[`large-drain-profiles.json`](large-drain-profiles.json).

The next candidates, in order, are:

1. **Encoded body allocation and copying across many chunks.** Evaluate shared
   backing storage or buffer reuse with the real transport. Preserve ownership,
   byte parity, release timing and per-request acknowledgement counts. Shared
   backing storage could reduce allocations but retain all encoded bytes until
   the final request completes. Include peak memory, slow/failing receivers and
   actual HTTP latency before adopting it.
2. **Repeated attribute/value length work.** The attributed profile still spends
   samples in `value_len` after the top-level cache. Earlier length-cache/slice
   experiments were inconsistent or regressed other fixtures; an implementation
   needs a fresh controlled comparison across plain and attributed workloads.
3. **One-event native storage.** `Vec::push` initially reserves more than one
   event slot. A smaller initial reservation may reduce allocated bytes for
   one-event traces, but risks reallocations for multi-event traces. Benchmark
   event cardinalities and tail latency before changing this policy.

No speculative encoder, allocator or event-storage change is included in the
production fix. Producer publication remains configurable with the measured
384-span default from the [earlier batch-knee report](../2026-10-19-batch-knee/README.md).
That archived report describes its original source; its warning about truncation
is historical and is superseded by this audit and the current exporter docs.

## Method and reproduction

Linux x86_64 Ryzen 9 3950X, Rust 1.93.1, `RUSTFLAGS=-Dwarnings`, ordinary release
optimization, one pinned CPU. Six paired process orders alternate single/chunks
order and rotate/reverse fixture order. Each process performs at least 1,048,576
measured encodings after eight warmup cycles. The same immutable drained native
batch is reused. Setup, fixture creation, validation and final native destruction
are excluded; real encoding, body allocation and body destruction are charged.
Native recording, producer synchronization, HTTP request construction, gzip,
sockets, TLS, acknowledgements and collector work are excluded. This is a shared
host and the values are not latency or network-performance guarantees.

[`metadata.json`](metadata.json) identifies the exact source, measured/profiled
binaries, compiler, source hashes and actual isolated manifest. The retained
manifests and lock use genuine registry dependencies; isolated diagnostics do
not qualify all workspace features. Source is identical to `f850b2b` for all
measured runtime files. The local disk guard admitted every build, timing sample
and profile above its 20 GB floor; raw sample receipts are retained in
[`guard-receipts.json`](guard-receipts.json). No runs were filtered out. Original
commands, outputs and unmodified raw profiles remain in the local evidence folder.

Prepare the equivalent isolated diagnostic against this checkout, preserving the
retained package names and registry lock:

```sh
python3 prepare-probe.py /tmp/span-export-probe
cargo build --release --locked --manifest-path /tmp/span-export-probe/Cargo.toml
taskset -c 0 /tmp/span-export-probe/target/release/span-export-chunk-probe single attributes 3072 342
taskset -c 0 /tmp/span-export-probe/target/release/span-export-chunk-probe chunks attributes 3072 342
```

Use [`encode-probe-v1.rs`](encode-probe-v1.rs) for timings; `--phases` selects
[`encode-probe-v2.rs`](encode-probe-v2.rs) for diagnostic phase hooks. Its optional
Linux preload profiler is retained as [`profile.c`](profile.c); compile it as a
shared library and set `LD_PRELOAD` only for the diagnostic process. Profile
timings must not be mixed with uninstrumented results. Enforce the host resource
guard before every build or run. The normal workspace CI command remains
`cargo test --workspace --all-features`.

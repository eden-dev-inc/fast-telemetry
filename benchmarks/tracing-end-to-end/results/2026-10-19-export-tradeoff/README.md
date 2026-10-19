# Request-cap tradeoff: controlled encoding and HTTP observation

The large-drain slowdown in the [earlier audit](../2026-10-19-export-audit/README.md)
is sensitive to benchmark preparation and allocator history. In this controlled
repeat, giving every variant identical preparation removes the penalty: 512-span
chunks use about **4% less encoding/body-cleanup CPU for plain spans and 6% less
for attributed spans at 8,192 spans**. The earlier 30–73% penalty should not be
treated as an intrinsic cost of chunking or a production regression.

HTTP still has a clear tradeoff. Smaller requests use the encoder's length cache,
but more compression operations, HTTP requests and acknowledgements outweigh
that saving in the loopback observations. These results do not change defaults.

## Controlled setup and allocator experiment

The original verifier builds only the selected strategy's expected protobuf
requests. Creating those temporary protobuf objects and buffers changes allocator
history differently for one large request and many small requests. The matched
verifier creates and retains every cap's golden buffers in the same order for
every process. Only the selected encoding strategy changes inside timing.

At 8,192 spans, the same binary gives these median paired CPU changes for cap
512 relative to one complete unlimited request; lower is better:

| Payload | Original preparation, default allocator | Matched preparation, default allocator | Original preparation, fixed thresholds |
| --- | ---: | ---: | ---: |
| Borrowed plain | +60.0% | -3.8% | -2.5% |
| Owned attributes/event | +22.7% | -6.2% | -5.9% |

Default allocator matched ranges are -4.4% to -1.6% for plain spans and -7.0%
to -5.1% for attributed spans. All eight pairs improve. A separate causal
diagnostic fixes glibc mmap/trim thresholds at 1 GiB; the original-setup penalty
also disappears. Those settings are a diagnostic, not an application tuning
recommendation. GNU libc documents that default thresholds adapt to allocation
patterns. See the [GNU memory allocation tunables reference](https://sourceware.org/glibc/manual/latest/html_node/Memory-Allocation-Tunables.html).

This shows that allocator/setup state can reverse the conclusion while runtime
source and encoding strategy remain identical. It does not establish which
allocator policy is best for a long-running application. Both old and new raw
observations are retained; the interpretation of the old large-drain result is
superseded by these controls.

## Matched request-cap sweep

Six paired process orders sweep drains of 3,072/4,096/6,144/8,192 spans and
request caps of 256/512/1,024/2,048/unlimited. At 8,192 spans:

| Request cap | Requests | Plain CPU change | Attributed CPU change |
| ---: | ---: | ---: | ---: |
| 256 | 32 | -0.7% | -5.8% |
| 512 | 16 | -3.1% | -5.5% |
| 1,024 | 8 | -0.7% | +0.3% |
| 2,048 | 4 | -1.3% | -0.1% |
| Unlimited | 1 | reference | reference |

Cap 512 attributed CPU improves about 5.5–6.6% across all four drains in this
sweep, with every paired range below zero. Plain changes are smaller and some
ranges cross zero. Caps of 256/512 use the existing top-level length cache;
1,024/2,048 do not. There is no reproduced large-drain encoding crossover after
matching preparation. Complete medians and paired ranges are in
[`summary.json`](summary.json), with no sample filtering.

## Acknowledged HTTP observation

The unchanged production `OtlpHttpClient` source and reqwest 0.13 send requests
sequentially over one persistent loopback connection. A separately pinned receiver
process verifies exact protobuf bytes, decompressing gzip when enabled, before
acknowledging. Its allocations do not share the client's heap. Client CPU uses
thread CPU time on the current-thread Tokio executor; acknowledged wall time
includes the receiver's validation. Payload structure, encoded field widths and
verifier preparation are matched across caps. IDs are generated separately per
process; each process verifies every request against its exact native input.

For **8,192 attributed spans**, medians of four fresh processes per case:

| Request cap | Requests | Client CPU, plain ns/span | Plain wall ms/drain | Gzip wall ms/drain | Plain + 1 ms/request delay |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 512 | 16 | 362.4 | 3.53 | 12.86 | 20.91 ms |
| 2,048 | 4 | 308.2 | 2.68 | 11.46 | 7.08 ms |
| Unlimited | 1 | 286.4 | 2.45 | 11.02 | 3.58 ms |

Gzip client CPU is 1,070/1,001/989 ns/span respectively. The gzip body totals
are close: about 438/437/437 KB per drain. The large gap comes from processing
and request frequency rather than significant extra protobuf framing bytes.
For plain spans, uncompressed acknowledged times are 1.53/0.57/0.35 ms at the
same caps. Full 3,072-span cases, ranges, bytes and component times are retained.

The 1 ms delay is deliberately added by the receiver before each acknowledgement;
it is not a claim about real network latency. It demonstrates the cost of serial
requests: the 512-span drain incurs sixteen waits, versus four or one. It excludes
TLS, a real Collector's ingestion work, retries, multiple producers and async
application load. Bigger requests may exceed receiver limits or concentrate
memory and timeout exposure; this observation selects no new default.

The next useful test is body allocation/copy reuse at a fixed bounded request
cap, with memory retention and slow/failing receivers measured alongside CPU.
The current default remains 384 spans per native producer publication and a
separate 512-span HTTP request cap.

## Evidence, figures and scope

[Observation run 39260300991](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39260300991)
passes build, Clippy, formatting and all four stages: **476 fresh successful
processes / 406,568,960 verified measured encodings**. Controls contribute
67,108,864; fixed-threshold controls another 67,108,864; cap sweeps 251,904,000;
HTTP 20,447,232. Warmups are additional and excluded from those totals.
Every request conserves the input span count and matches complete prost bytes.

[Download the figures and complete run artifact](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39260300991/artifacts/12190401669).
It contains standalone PNG/SVG charts for allocator controls, request-cap curves,
HTTP/gzip and delayed acknowledgements, plus original process outputs, commands,
exit codes, guard receipts and binary/source hashes. Artifact retention is 90
days; the raw CSVs and summary in this directory remain in Git history. The
Matplotlib figure generator is retained in the harness for regeneration.

The CI runner is AMD EPYC 7763, Ubuntu 24.04, glibc 2.39, Rust 1.99.1. Client CPU
0 and receiver CPU 3 are pinned from a four-CPU affinity set. These are shared
runner observations compared within this run, not an absolute comparison to the
earlier Ryzen/Adam + Rust 1.93.1 timings. Adam's local disk admission remained
closed, so no new local timing was started. The disposable CI runner admitted
every build and sample above 20 GB free disk.

PR source head is `d6e11f6b450d65329b4ea521bedc5f066bbef9bf`; Actions checked out
merge ref `f33d793b47eb96bda5821990524745798e656601`. Exact runtime source hashes
match the earlier reviewed source at `5211f9a`. No production runtime code changed
for this observation. Normal production CI also passes all eight jobs at
[run 39260300972](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39260300972).
See [`metadata.json`](metadata.json) for compiler and binary identities.

## Reproduction

Use the standalone [harness](../../../span-export-tradeoff/README.md), its
retained Cargo lock and a fresh output directory. `run.py` enforces a fresh Eden
host guard when present, at least 20 GB free disk, CPU affinity, request/span
conservation and process success. Timing samples do not use profiler hooks.

Encoding measures actual serialization and wire-body cleanup. Setup, golden
validation, recording and final native payload destruction are excluded. The
HTTP path additionally charges compression, transport and acknowledgement;
native fixture payloads remain borrowed for reuse, so it does not measure the
periodic export loop's native clear or scheduling. The four-variant controls use
eight cyclic/reverse orders for position balance. Five-cap sweeps use six
cyclic/reverse orders, not complete position balance; HTTP uses four orders.
No configuration or allocator policy is inferred to be universally optimal.

```sh
cd benchmarks/span-export-tradeoff
python3 run.py admit --output /tmp/export-observation
cargo build --release --locked
python3 run.py controls --output /tmp/export-observation
python3 run.py allocator --output /tmp/export-observation
python3 run.py caps --output /tmp/export-observation
python3 run.py http --output /tmp/export-observation
python3 summarize.py /tmp/export-observation --plot  # requires Matplotlib 3.10.0
```

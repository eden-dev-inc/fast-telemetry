# Native fast-telemetry versus OpenTelemetry over actual HTTP

In this matched prepared-span exporter comparison, native fast-telemetry uses
about **5× less client CPU** and takes about **4.5× less acknowledged wall time**
than the pinned OpenTelemetry 0.30 exporter for 8,192 attributed spans at the
default 512-span request cap. Both stacks perform real persistent HTTP/1.1
exports to the same validating receiver implementation. These figures describe
the export stage after recording and collection, rather than tracing macro or
application latency.

The [CI measurement run](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39265023021)
passes release build, Clippy, formatting and **272 fresh successful processes /
47,710,208 verified measured spans**. Every request matches its expected complete
protobuf bytes before acknowledgement. All eight normal production CI jobs also
pass at [the measured source](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39265022379).
No production runtime code or defaults changed for this comparison.

## Matched request sizes

For **8,192 attributed spans**, group medians across eight fresh processes per
exporter and case. Ratio columns are medians of the eight adjacent paired
OTel/native ratios, so they can differ from ratios of group medians.

| Spans/request | Requests | Native CPU ns/span | OTel CPU ns/span | Paired CPU ratio | Native wall ms/drain | OTel wall ms/drain | Paired wall ratio |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 512 | 16 | 218.5 | 1,094.5 | 5.03× | 2.03 | 9.20 | 4.52× |
| 2,048 | 4 | 216.9 | 1,078.2 | 4.99× | 1.84 | 8.91 | 4.85× |
| Unlimited | 1 | 212.7 | 1,078.4 | 5.07× | 1.78 | 8.88 | 4.99× |

At cap 512, attributed CPU paired ratios range **4.84–5.56×**, and wall ratios
**4.38–5.12×**. All eight pairs favor native for both measurements. Native uses
about 80% less client CPU in this fixture. A plain span has no attributes or
event; the attributed fixture has an owned name and error, four owned string
attributes, and an event with one owned string and one integer attribute.

The smaller default-cap drains show the same direction:

| Attributed spans/drain | Native wall ms/drain | OTel wall ms/drain | Paired wall ratio | Paired CPU ratio |
| ---: | ---: | ---: | ---: | ---: |
| 512 | 0.132 | 0.692 | 5.20× | 5.62× |
| 3,072 | 0.771 | 3.433 | 4.47× | 4.95× |
| 8,192 | 2.031 | 9.203 | 4.52× | 5.03× |

For 8,192 plain spans, cap 512 gives 49.2 versus 259.9 client CPU ns/span
(5.24× paired ratio), and 0.636 versus 2.374 ms acknowledged wall time
(3.67×). CPU ratios at caps 2,048/unlimited are 8.78×/9.88×; corresponding
wall ratios are 7.01×/9.31×. Plain native timings are noisier: one cap-512 pair
has a wall ratio of 0.995×. All raw rows, including that sample, are retained.
The larger attributed SDK request at 3,072/unlimited also varies with this
particular preparation. These observations do not select a universal request
size or allocator policy.

## Delayed acknowledgements

For 8,192 attributed spans, adding **1 ms of receiver sleep before each ACK**:

| Spans/request | Native wall ms/drain | OTel wall ms/drain | Paired wall ratio |
| ---: | ---: | ---: | ---: |
| 512 | 19.05 | 26.22 | 1.38× |
| 2,048 | 6.08 | 13.11 | 2.16× |
| Unlimited | 2.84 | 9.97 | 3.50× |

Client CPU remains approximately 5× lower for native, but the shared serial
request waits reduce its relative wall-time advantage. The delay is a controlled
receiver delay, not a measurement of production network latency. Both stacks
send one request at a time and wait for its acknowledgement. Unlimited requests
are diagnostic and may violate a real receiver's limits; defaults remain 384
spans per native producer publication and a separate 512-span HTTP request cap.

## Boundary and payload equivalence

The native path uses the unchanged production `OtlpHttpClient` source with
reqwest 0.13.4. The baseline uses the actual `opentelemetry-otlp` 0.30.0
HTTP/protobuf `SpanExporter` and its official asynchronous reqwest 0.12.28
adapter. OTel API, SDK and protobuf packages are all pinned to 0.30.0, matching
the existing sender baseline. Both run on a current-thread Tokio executor. The
comparison includes the stacks' transport version and acknowledgement handling
differences; it is not an isolated encoder comparison or a claim about newer
SDK versions.

Timing starts with prepared completed spans and includes serialization, HTTP
request construction, selected input/body destruction, persistent HTTP/1.1
transport and successful acknowledgement. Native encodes all bounded bodies,
clears native records and then sends, matching its production loop. OTel
consumes one prepared `Vec<SpanData>` per request. Setup, recording, batch
partitioning, SDK processor/queue work, periodic scheduling, gzip, TLS, retries,
real Collector ingestion/storage and application concurrency are excluded.
Final empty-container release is outside timing.

Both representations are prepared in the same order before every cycle; unused
input remains alive through timing and is destroyed afterward. Every process
constructs and retains both exporters' golden requests for all three caps in a
fixed order. IDs are deterministic and identical across exporters. Full decoded
payload comparison checks resource/scope, names, IDs, timestamps, attributes,
events, errors and counts. The SDK's sampled flag and explicit unset status are
normalized only for this semantic check. Actual requests preserve those protocol
differences, adding six bytes per attributed span and eight per plain span to
the SDK body. At 8,192 spans/cap 512, attributed bodies total 1,868,966 native
versus 1,918,118 SDK bytes; plain bodies total 500,608 versus 566,144 bytes.
Thus the user payload is equivalent, while encoded requests are not byte-identical.

The receiver runs in a separately pinned process. It checks the complete exact
request bytes and acknowledges only successful matches, with no shared client
allocator state. Client CPU uses thread CPU time and excludes receiver CPU;
wall time includes receiver validation and any requested delay. Each process
runs eight warmup and 32 measured cycles. Eight adjacent pairs alternate
exporter order, four in each direction. No rows are filtered. Reported ranges
describe observed paired samples rather than confidence intervals. Input
reconstruction and shared-runner load can affect allocator state and scheduling;
this is a controlled benchmark, not a downstream application deployment result.

## Evidence and reproduction

The runner is **AMD EPYC 9V45**, Ubuntu 24.04, glibc 2.39, Rust 1.99.1. Client
CPU 0 and receiver CPU 3 are selected from four allowed CPUs. Compare exporters
within this run; the earlier request-cap HTTP observations used an EPYC 7763,
and earlier full-sender captures used Adam/Ryzen with Rust 1.93.1. Their absolute
times cannot be compared directly with these results. Adam's disk guard stayed
closed, so no new local performance process or compilation was started.

PR source is `c3c0e3312fbc1e574acf72f48d56b1f81ff8f408`; Actions measured merge
ref `999fcc40e4dddaa450a437123035cbb0b29d9d15`. Production encoder and HTTP source
hashes match the preceding reviewed runtime. Exact source, lock, binary, compiler
and CPU identities are retained in [`environment.json`](environment.json).
[`all-runs.csv`](all-runs.csv) contains all 272 samples;
[`summary.json`](summary.json) retains all paired CPU/wall ranges and byte counts.

[Download standalone PNG/SVG figures and all original process outputs](https://github.com/eden-dev-inc/fast-telemetry/actions/runs/39265023021/artifacts/12192372298).
The artifact includes commands, exit status, stderr, guard receipts, source
hashes and original stdout, with a 90-day retention period. Its archive SHA-256
is `f3a4825cc3c30ab3f3f433a8445c6aafddfdc1255f0b86076e74b948f7cf22d8`.
Raw measurements and the summary remain in Git history. The standalone
[harness and reproduction instructions](../../../span-export-http-head-to-head/README.md)
retain the Cargo lock, resource admission, exact payload verification, fresh
process runner, summary calculation and Matplotlib figure generator.

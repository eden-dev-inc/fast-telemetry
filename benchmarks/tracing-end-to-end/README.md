# Lossless tracing through OTLP encoding and export

This standalone Linux benchmark compares three complete recording backends:

See the [2026-10-19 review measurements](results/2026-10-19-capture/README.md)
for retained sample data, profiling results and source/dependency provenance.

| Mode | Path |
| --- | --- |
| `native` | Native fast-telemetry spans → thread-local buckets → direct OTLP encoder |
| `drop-in` | `tracing` → `FastTelemetrySubscriber`, explicit `UserFields` policy → direct OTLP encoder |
| `otel` | `tracing` → tracing-opentelemetry → OpenTelemetry SDK `BatchSpanProcessor` → actual opentelemetry-otlp HTTP/protobuf exporter |

All paths use one global subscriber where applicable, identical user payloads,
resource/scope names, export batch counts, and a common HTTP client. Ordinary
native OTLP flags are zero; the SDK emits sampled trace flags and an explicit
unset status. These protocol metadata differences remain in the wire bytes and
costs. The comparison does not claim byte-identical requests. IDs and timestamps
are independently generated and validated.

The pinned stack is `tracing` 0.1.44, tracing-subscriber 0.3.23,
tracing-opentelemetry 0.31.0, OpenTelemetry API/SDK/OTLP exporter 0.30.0,
prost 0.13.5, and reqwest 0.12.28. It deliberately has its own Cargo workspace and
lockfile; it does not change the main workspace's SDK 0.31 benchmark dependencies.

## Measurement boundary

`capture` runs both production OTLP encoders and constructs real HTTP/protobuf
requests. The common HTTP client retains the encoded allocation and returns an
in-process successful response. **There is no socket or actual collector in
capture mode.** Its result is sender pipeline CPU and throughput, excluding
compression, network/TLS, collector processing, and any export interval delay.

Pass `http://HOST:PORT` instead to send every request through the same reqwest
blocking client to `/v1/traces`. This mode includes HTTP response delivery and
protobuf acknowledgement validation. Non-success status, malformed response or
partial rejection fails the run. Use the same collector instance/configuration
for all modes. Compression, TLS and retries are disabled. This common transport
isolates recording/collection/encoding differences; it does not benchmark the
production fast-telemetry-export HTTP client's gzip, retry or scheduling policy.
Use a collector whose downstream pipeline can accept the benchmark volume.

One synchronized cycle records complete traces, flushes native TLS or the SDK,
exports **one complete batch**, and releases all actual span and body payloads.
The default matrix uses 512 spans per request across 1, 8 and 16 producers.
Each native producer stays at or below 1024 spans per cycle, below the sampling
pressure boundary. Queues allow 4096 spans per producer in all modes. Sampling
remains enabled with its normal native policy; every run requires zero sampled
roots and exact exported counts. The SDK uses `AlwaysOn` sampling. The 1-producer
case records inline on the coordinator; other cases use resident worker threads
and include their cycle barriers. The SDK's real background processor is active.
Workers remain alive through the final cleanup.

An optional final `NATIVE_PRODUCER_BATCH_SIZE` argument selects 1–1,024 spans
per automatic native publication; it defaults to the collector's 384-span policy.
This differs from `SPANS_PER_THREAD_BATCH`, which controls the synchronized
application/export burst and always ends with an explicit local flush. The CSV
reports both values. The native publication option does not change immediate
adapter publication or the SDK processor. For a fixed export burst, compare
publication targets separately; changing the burst also changes barrier and
request frequency. See the [batch-size knee report](results/2026-10-19-batch-knee/README.md).

Every request is decoded outside timing. Verification checks exact resource,
scope, span/event/attribute counts, values and types; unique nonzero IDs; complete
root/child graphs; and nested lifecycle timestamps. Root, four-span, and attributed
two-span-plus-event fixtures are included. The event's mandatory OpenTelemetry
level/target fields are explicitly recorded by the FT frontends so the payloads
match. The drop-in policy is an explicit smaller profile than its default full
automatic metadata. These checks run during warmup as well as measurement.

Timing uses process CPU summed across all threads and elapsed wall time. Two
intervals per cycle charge recording/collection/encoding/transport and actual
payload destruction. Verification and its temporary decoded objects are excluded.
The wall intervals also include cycle synchronization and scheduling; they are
not individual request latency percentiles. Final empty storage and backend
shutdown are outside timing. These measurements do not represent unpaced maximum
throughput: each cycle waits for successful export and verification before the
next cycle starts. Report CPU and wall throughput separately.

## Run

Run the host's required resource checks before builds/tests and before each
sample. The runner accepts `--admission-command` for the latter; any nonzero exit
stops the matrix. No benchmark is added to CI.

```sh
cargo build --release --locked --manifest-path benchmarks/tracing-end-to-end/Cargo.toml
cargo test --release --locked --manifest-path benchmarks/tracing-end-to-end/Cargo.toml
cargo clippy --release --locked --all-targets \
  --manifest-path benchmarks/tracing-end-to-end/Cargo.toml -- -D warnings

python3 benchmarks/tracing-end-to-end/run.py \
  --binary benchmarks/tracing-end-to-end/target/release/tracing-end-to-end \
  --output /absolute/path/to/new-results --transport capture
```

Replace `--transport capture` with `--transport http://127.0.0.1:4318` on a host
with socket access and a running OTLP collector. For a larger matched export
batch, use `--threads 8 16 --export-batch 4096`. Export batches must divide into
whole traces on each producer. Each sample starts a new process. Six repetitions
cover all mode permutations; raw commands, stdout/stderr, exit codes, binary
hashes, accounting and paired summaries are preserved. Run in release mode and
report exact versions, batch sizes, thread counts and transport alongside ratios.

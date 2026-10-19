# Production HTTP exporter comparison

This standalone diagnostic compares native fast-telemetry's direct encoder and
unchanged production `OtlpHttpClient` source with the real OpenTelemetry 0.30.0
HTTP/protobuf `SpanExporter` and its official asynchronous reqwest adapter.
Both run on a current-thread Tokio executor. Fast-telemetry uses reqwest 0.13;
the pinned OTel baseline uses reqwest 0.12.28, matching the existing sender
comparison. These are the two actual exporter stacks, including transport
version differences, rather than an isolated encoder comparison.

Timing starts with prepared completed spans and includes encoding, request
construction, actual selected span/body destruction, sequential persistent
HTTP/1.1 sends and acknowledgements. Native encodes all bounded requests and
clears native payloads before sending, as its production export loop does.
OTel consumes a prepared `Vec<SpanData>` per request. Input generation and
batch partitioning are outside timing for both. Recording, the SDK batch
processor/queue, periodic scheduling, compression, TLS, retries, real Collector
ingestion and application concurrency are excluded.

Every process prepares and retains both stacks' golden requests for all caps
in the same order, and prepares both input representations before every cycle.
Unused input cleanup is excluded; selected input cleanup is charged. IDs,
timestamps, names, resource/scope, attributes, events, errors and span counts
match exactly. The SDK's sampled flag and explicit unset status are the only
normalized protocol metadata when checking equivalence; actual requests retain
those differences and report their byte sizes. Every request is then checked
against its complete exact bytes by a receiver in a separate process on a
different CPU before that receiver acknowledges it. Client thread CPU excludes
receiver CPU; acknowledged wall time includes verification and any ACK delay.

Eight adjacent pairs alternate exporter order, with 32 measured cycles after
eight warmups in each fresh process. Fixtures include plain and owned attributed
spans, 512/3,072/8,192-span drains, and caps 512/2,048/unlimited. The 512-span
drain only tests cap 512. Separate 8,192 attributed cases add 1 ms of receiver
sleep before each ACK; this is a controlled delay, not real network latency.
All 272 processes must pass exact payload/count/byte verification. Raw rows,
commands, stderr, exit status, CPU affinity, source/binary hashes and resource
admission receipts are retained. Paired ranges describe samples, not confidence
intervals. There is no sample filtering.

The runner requires at least 20 GB free disk and a fresh, open Eden resource
guard when present. The dedicated CI workflow uses a disposable Ubuntu runner.

```sh
python3 run.py admit --output /tmp/http-exporters
cargo build --release --locked
cargo clippy --release --locked --bins -- -D warnings
cargo fmt -- --check
python3 run.py matrix --output /tmp/http-exporters
python3 summarize.py /tmp/http-exporters --plot  # Matplotlib 3.10.0
```

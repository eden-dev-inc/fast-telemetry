# Span export tradeoff observation

This isolated diagnostic compares complete retained drains across request caps.
It does not change producer or HTTP defaults. `single` produces one full request;
`chunks` uses a positive configurable request cap. Plain and owned attribute/event
fixtures are verified against complete prost bytes on every measured cycle.
Native recording, setup, verification and final native-payload destruction are
excluded. Encoding and encoded-body destruction are charged.

`original` prepares only the selected cap's expected bytes. `matched` prepares
and retains every cap's expected bytes in a fixed order in every process. This
controls for verification setup changing allocation history. The separate
`allocator` stage fixes glibc mmap/trim thresholds at 1 GiB as a causal diagnostic,
not an application configuration recommendation.

The HTTP binary compiles the unchanged production transport source with its
normal reqwest 0.13 dependencies. A current-thread Tokio client sends requests
sequentially over one persistent loopback connection to a separate process pinned
to another CPU. The receiver checks exact protobuf bytes, including gzip, before
acknowledging every request. Client thread CPU excludes receiver CPU; acknowledged
wall time includes receiver validation and any configured per-request delay.
All request caps receive the same native fixture and preparation. Native input
remains borrowed for reuse; this does not measure the export loop's final native
payload clear, periodic scheduling, TLS, concurrent producers or a real Collector.

The host must have at least 20 GB free disk. If the Eden host guard exists it
must also be fresh, OK and permit new runs. Every sample retains its admission
receipt, command, original output, exact count and exit status. No failed samples
are silently dropped. Run stages into a fresh output directory.

```sh
python3 run.py admit --output /tmp/export-observation
cargo build --release --locked --manifest-path Cargo.toml
python3 run.py controls --output /tmp/export-observation
python3 run.py allocator --output /tmp/export-observation
python3 run.py caps --output /tmp/export-observation
python3 run.py http --output /tmp/export-observation
```

The targeted CI workflow performs the same observation on a disposable Ubuntu
runner and uploads all outputs. Compare strategies within that runner; its
absolute timings are not a comparison to the earlier Ryzen/Adam measurements.
The separate crates' normal CI remains the authority for production correctness.

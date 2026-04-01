# Examples

| Example        | Crates used                             | What it shows                                                                                         |
|----------------|-----------------------------------------|-------------------------------------------------------------------------------------------------------|
| [`demo`](demo) | ophanim, ophanim-macros, ophanim-export | Full pipeline: define metrics with derive macros, record, export via Prometheus/DogStatsD/OTLP, spans |

Run with:

```bash
cargo run -p ophanim-demo
```

For smaller, single-crate examples (without export), see
[`crates/ophanim/examples/`](../crates/ophanim/examples/).

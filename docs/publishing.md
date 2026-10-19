# Publishing 0.9

The release consists of `fast-telemetry-macros`, `fast-telemetry`,
`fast-telemetry-export`, and the new `fast-telemetry-tracing` companion. They
share version 0.9.0 and require Rust 1.93 or newer. The demo is not publishable.

## Verification

Review [the release notes](../RELEASE_NOTES.md) and the
[native span migration](metrics-and-spans.md#spans). A stored native span now
carries its collector lifetime. `Span::enter()` returns a scoped, thread-bound
guard that must remain bound while logging correlation is needed and must close
before `.await`. Default tracing metadata is `Full`; `UserFields` is opt-in.

CI runs workspace formatting, MSRV and stable compilation, Clippy, unit and
integration tests, rustdoc, and the pinned end-to-end payload verifier. The
`Published crate verification` job additionally runs:

```sh
cargo package --workspace --exclude fast-telemetry-demo --all-features
```

Cargo stages the sibling dependencies and builds all four extracted archives with all features enabled.
This verifies the distributable contents rather than relying solely on path
dependencies in the source workspace. Each archive includes a copy of the root
Apache 2.0 license; CI checks their byte equality. Crate README links point to
published documentation or repository pages that exist outside the archive.
No registry uploads occur during these checks.

The [tracing benchmark guide](tracing-benchmark.md) links raw measurements and
states the source versions, payload policies and timing boundaries. Sender
capture measurements exclude sockets, compression, TLS, retries and collector
processing. They establish measured improvements for those fixtures. Native
parity through existing tracing macros and a 10 ns complete-trace budget have
not been achieved. Default `Full` and explicit `UserFields` results must stay
separate in release announcements.

## Release operation

Publishing requires a maintainer's release authorization and registry access.
Confirm that the final release commit has passed CI and that the version has
not already been published. Publish in dependency order:

1. `fast-telemetry-macros`
2. `fast-telemetry`
3. `fast-telemetry-export`
4. `fast-telemetry-tracing`

Wait for each dependency version to become available in the registry before
publishing its dependents. Use normal Cargo verification; do not bypass it with
`--no-verify`. After upload, compile a fresh downstream project using registry
versions and the intended feature set, and confirm the published API docs.
Update the release date and release tag through the repository's release process.

The built-in span exporter remains best effort. Acknowledgements are reported,
but failed drained batches are not durably retained or replayed automatically.
Applications requiring durable delivery must own that policy. Producers with
low traffic must flush locally before shutdown/export; cancellation of the
exporter cannot flush another live OS thread's partial native buffer. See the
[exporter guide](exporters.md) for shutdown integration.

#!/usr/bin/env bash
# Run on Adam after the coordinator's resource gate and static review.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
if [[ -n "$(git status --porcelain)" ]]; then
  echo 'Benchmark requires a clean committed candidate for reproducible provenance.' >&2
  exit 1
fi
output="${1:-crates/fast-telemetry-tracing/bench/results/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$(dirname "$output")"
mkdir "$output"
iterations="${ITERS:-10000}"
threads="${THREADS:-1 2 4 8}"
runs="${RUNS:-5}"
batch="${BATCH:-64}"
capacity="${CAPACITY:-4096}"
warmup="${WARMUP:-100}"
[[ "$runs" =~ ^[1-9][0-9]*$ ]] || { echo 'RUNS must be positive' >&2; exit 1; }
{
  date -u
  hostname
  uname -a
  rustc -Vv
  cargo -V
  git rev-parse HEAD
  git status --porcelain
  printf 'ITERS=%s THREADS=%s RUNS=%s BATCH=%s CAPACITY=%s WARMUP=%s\n' "$iterations" "$threads" "$runs" "$batch" "$capacity" "$warmup"
  printf 'RUSTFLAGS=%s CARGO_PROFILE_RELEASE_LTO=%s\n' "${RUSTFLAGS:-}" "${CARGO_PROFILE_RELEASE_LTO:-}"
} > "$output/provenance.txt"
cargo build --release -p fast-telemetry-tracing --features bench-tools --bin bench_tracing_backends > "$output/build.log" 2>&1
cargo tree -p fast-telemetry-tracing --features bench-tools > "$output/dependencies.txt"
cp Cargo.lock "$output/Cargo.lock"
binary="${CARGO_TARGET_DIR:-target}/release/bench_tracing_backends"
sha256sum "$binary" "$output/Cargo.lock" >> "$output/provenance.txt"
for ((run=1; run<=runs; run++)); do
  # Alternate order between repetitions to reduce a fixed backend-order bias.
  modes='fast otel disabled'
  (( run % 2 == 0 )) && modes='otel fast disabled'
  for workload in root attributes children; do
    for thread_count in $threads; do
      for mode in $modes; do
        name="run-${run}-${workload}-${thread_count}-${mode}"
        args=(--mode "$mode" --workload "$workload" --threads "$thread_count" --iters "$iterations" --warmup "$warmup" --batch "$batch" --capacity "$capacity")
        printf '%q ' "$binary" "${args[@]}" > "$output/$name.command"
        printf '\n' >> "$output/$name.command"
        date -u +%FT%TZ > "$output/$name.started"
        "$binary" "${args[@]}" > "$output/$name.csv" 2> "$output/$name.stderr"
      done
    done
  done
done
python3 - "$output" <<'PY'
import csv
import pathlib
import statistics
import sys
root = pathlib.Path(sys.argv[1])
rows = []
for path in sorted(root.glob('run-*.csv')):
    with path.open() as stream:
        row, = list(csv.DictReader(stream))
    row['source'] = path.name
    rows.append(row)
with (root / 'all-runs.csv').open('w') as stream:
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
metrics = ['record_cpu_ns_per_iteration', 'record_cpu_ns_per_span', 'pipeline_cpu_ns_per_iteration', 'pipeline_cpu_ns_per_span', 'record_wall_ns', 'drain_wall_ns', 'verify_wall_ns']
with (root / 'medians.csv').open('w') as stream:
    writer = csv.writer(stream)
    writer.writerow(['mode', 'workload', 'threads', 'runs'] + metrics)
    groups = sorted({(r['mode'], r['workload'], r['threads']) for r in rows})
    for group in groups:
        members = [r for r in rows if (r['mode'], r['workload'], r['threads']) == group]
        values = ['NA' if members[0][m] == 'NA' else statistics.median(float(r[m]) for r in members) for m in metrics]
        writer.writerow(list(group) + [len(members)] + values)
PY
printf 'Retained results and provenance in %s\n' "$output"

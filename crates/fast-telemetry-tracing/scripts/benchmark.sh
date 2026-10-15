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
read -r -a modes <<< "${MODES:-native native-matched native-user-fields fast direct direct-user-fields otel disabled registry}"
read -r -a dispatches <<< "${DISPATCHES:-scoped}"
(( ${#modes[@]} > 0 )) || { echo 'MODES must not be empty' >&2; exit 1; }
(( ${#dispatches[@]} > 0 )) || { echo 'DISPATCHES must not be empty' >&2; exit 1; }
[[ "$runs" =~ ^[1-9][0-9]*$ ]] || { echo 'RUNS must be positive' >&2; exit 1; }
{
  date -u
  hostname
  uname -a
  rustc -Vv
  cargo -V
  git rev-parse HEAD
  git status --porcelain
  printf 'ITERS=%s THREADS=%s RUNS=%s BATCH=%s CAPACITY=%s WARMUP=%s MODES=%s DISPATCHES=%s\n' "$iterations" "$threads" "$runs" "$batch" "$capacity" "$warmup" "${modes[*]}" "${dispatches[*]}"
  printf 'RUSTFLAGS=%s CARGO_PROFILE_RELEASE_LTO=%s\n' "${RUSTFLAGS:-}" "${CARGO_PROFILE_RELEASE_LTO:-}"
} > "$output/provenance.txt"
cargo build --release -p fast-telemetry-tracing --features bench-tools --bin bench_tracing_backends > "$output/build.log" 2>&1
cargo tree -p fast-telemetry-tracing --features bench-tools > "$output/dependencies.txt"
cp Cargo.lock "$output/Cargo.lock"
binary="${CARGO_TARGET_DIR:-target}/release/bench_tracing_backends"
sha256sum "$binary" "$output/Cargo.lock" >> "$output/provenance.txt"
for ((run=1; run<=runs; run++)); do
  # Rotate all modes between repetitions to reduce fixed backend-order bias.
  for workload in root attributes children; do
    for thread_count in $threads; do
      for ((offset=0; offset<${#modes[@]}; offset++)); do
        mode="${modes[(run - 1 + offset) % ${#modes[@]}]}"
        for dispatch in "${dispatches[@]}"; do
          name="run-${run}-${workload}-${thread_count}-${mode}-${dispatch}"
          args=(--mode "$mode" --dispatch "$dispatch" --workload "$workload" --threads "$thread_count" --iters "$iterations" --warmup "$warmup" --batch "$batch" --capacity "$capacity")
          printf '%q ' "$binary" "${args[@]}" > "$output/$name.command"
          printf '\n' >> "$output/$name.command"
          date -u +%FT%TZ > "$output/$name.started"
          "$binary" "${args[@]}" > "$output/$name.csv" 2> "$output/$name.stderr"
        done
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
metrics = ['record_cpu_ns_per_iteration', 'record_cpu_ns_per_span', 'pipeline_cpu_ns_per_iteration', 'pipeline_cpu_ns_per_span', 'pipeline_with_destroy_cpu_ns_per_iteration', 'pipeline_with_destroy_cpu_ns_per_span', 'record_wall_ns', 'drain_wall_ns', 'verify_wall_ns', 'destroy_wall_ns', 'destroy_cpu_ns', 'total_wall_ns']
medians = {}
with (root / 'medians.csv').open('w') as stream:
    writer = csv.writer(stream)
    writer.writerow(['mode', 'workload', 'threads', 'dispatch', 'runs'] + metrics)
    groups = sorted({(r['mode'], r['workload'], r['threads'], r['dispatch']) for r in rows})
    for group in groups:
        members = [r for r in rows if (r['mode'], r['workload'], r['threads'], r['dispatch']) == group]
        values = ['NA' if members[0][m] == 'NA' else statistics.median(float(r[m]) for r in members) for m in metrics]
        writer.writerow(list(group) + [len(members)] + values)
        medians[group] = dict(zip(metrics, values))
# Explicit CPU cost ratios use only full-retention modes. Bare native and
# equal-payload native are separate comparisons; controls have no span ratio.
with (root / 'comparisons.csv').open('w') as stream:
    writer = csv.writer(stream)
    ratio_metrics = ['record_cpu_ns_per_span', 'pipeline_cpu_ns_per_span', 'pipeline_with_destroy_cpu_ns_per_span']
    writer.writerow(['workload', 'threads', 'dispatch', 'numerator', 'denominator'] + ['numerator_over_baseline_' + m for m in ratio_metrics])
    for mode, workload, threads, dispatch in sorted(medians):
        if mode not in ['fast', 'direct', 'direct-user-fields']:
            continue
        baselines = ['native', 'native-matched', 'otel']
        if mode == 'direct-user-fields':
            baselines.append('native-user-fields')
        for baseline in baselines:
            key = (baseline, workload, threads, dispatch)
            if key in medians:
                writer.writerow([workload, threads, dispatch, mode, baseline] + [medians[(mode, workload, threads, dispatch)][m] / medians[key][m] for m in ratio_metrics])
PY
printf 'Retained results and provenance in %s\n' "$output"

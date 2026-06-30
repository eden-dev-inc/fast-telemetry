#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

THREADS="$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4)"
RUNS="7"
TARGET_WRITES="512000000"
BATCH_SIZES_CSV="1,2,4,8,16,32,64,128"
EXPORT_INTERVAL_MS="10"
PIN=0
CPU_LIST=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --target-writes) TARGET_WRITES="$2"; shift 2 ;;
    --batch-sizes) BATCH_SIZES_CSV="$2"; shift 2 ;;
    --export-interval-ms) EXPORT_INTERVAL_MS="$2"; shift 2 ;;
    --pin) PIN=1; shift ;;
    --cpu-list) CPU_LIST="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--threads N] [--runs N] [--target-writes N] [--batch-sizes list] [--export-interval-ms N] [--pin] [--cpu-list list]"
      echo ""
      echo "Compares counter_multi against counter_batch and counter_set across batch sizes."
      echo "Defaults: threads=logical CPUs, runs=7, target-writes=512000000, batch-sizes=1,2,4,8,16,32,64,128"
      echo "--target-writes is total counter writes per run, not outer benchmark ops."
      echo "--pin forwards taskset pinning to run-cache-bench.sh on Linux."
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

IFS=',' read -r -a BATCH_SIZES <<< "$BATCH_SIZES_CSV"
if [[ ${#BATCH_SIZES[@]} -eq 0 ]]; then
  echo "ERROR: --batch-sizes must include at least one size"
  exit 1
fi

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/counter_batch_${TIMESTAMP}_$$"
mkdir -p "$RUN_DIR"
SUMMARY_CSV="$RUN_DIR/counter-batch-summary.csv"

echo "batch_size,iters_per_thread,target_counter_writes,multi_cpu_ns_per_write,batch_cpu_ns_per_write,set_cpu_ns_per_write,multi_total_ns_per_op,batch_total_ns_per_op,set_total_ns_per_op,batch_delta_pct,set_delta_pct,multi_counter_writes_per_sec,batch_counter_writes_per_sec,set_counter_writes_per_sec,multi_cv_pct,batch_cv_pct,set_cv_pct,multi_cpu_total_seconds,batch_cpu_total_seconds,set_cpu_total_seconds,multi_avg_cores,batch_avg_cores,set_avg_cores,multi_dir,batch_dir,set_dir" > "$SUMMARY_CSV"

read_summary_field() {
  local dir="$1"
  local field="$2"
  awk -F, -v field="$field" 'NR == 2 { print $field }' "$dir/summary.csv"
}

extract_run_dir() {
  local log_file="$1"
  awk -F'Done. Results in: ' '/^Done\. Results in: / { print $2 }' "$log_file" | tail -n 1
}

run_case() {
  local entity="$1"
  local batch_size="$2"
  local iters="$3"
  local log_file="$RUN_DIR/${entity}-bs${batch_size}.log"
  local cmd=(
    "$SCRIPT_DIR/run-cache-bench.sh"
    --entity "$entity"
    --modes fast
    --threads "$THREADS"
    --iters "$iters"
    --runs "$RUNS"
    --batch-size "$batch_size"
    --export-interval-ms "$EXPORT_INTERVAL_MS"
  )

  if [[ "$PIN" == "1" ]]; then
    cmd+=(--pin)
    if [[ -n "$CPU_LIST" ]]; then
      cmd+=(--cpu-list "$CPU_LIST")
    fi
  fi

  "${cmd[@]}" | tee "$log_file" >&2
  local case_dir
  case_dir="$(extract_run_dir "$log_file")"
  if [[ -z "$case_dir" || ! -f "$case_dir/summary.csv" ]]; then
    echo "ERROR: could not find summary.csv for $entity batch_size=$batch_size"
    exit 1
  fi
  echo "$case_dir"
}

printf "\n=== counter batch benchmark harness ===\n"
printf "threads=%s runs=%s target_writes=%s batch_sizes=%s\n" "$THREADS" "$RUNS" "$TARGET_WRITES" "$BATCH_SIZES_CSV"
printf "export_interval_ms=%s pin=%s\n" "$EXPORT_INTERVAL_MS" "$PIN"
printf "results=%s\n\n" "$RUN_DIR"

for batch_size in "${BATCH_SIZES[@]}"; do
  if ! [[ "$batch_size" =~ ^[0-9]+$ ]] || [[ "$batch_size" -lt 1 ]]; then
    echo "ERROR: invalid batch size '$batch_size'"
    exit 1
  fi

  iters=$((TARGET_WRITES / THREADS / batch_size))
  if [[ "$iters" -lt 1 ]]; then
    iters=1
  fi

  printf "\n[counter-batch] batch_size=%s iters_per_thread=%s\n" "$batch_size" "$iters"
  multi_dir="$(run_case counter_multi "$batch_size" "$iters")"
  batch_dir="$(run_case counter_batch "$batch_size" "$iters")"
  set_dir="$(run_case counter_set "$batch_size" "$iters")"

  multi_ns="$(read_summary_field "$multi_dir" 20)"
  batch_ns="$(read_summary_field "$batch_dir" 20)"
  set_ns="$(read_summary_field "$set_dir" 20)"
  multi_total_ns_per_op="$(read_summary_field "$multi_dir" 25)"
  batch_total_ns_per_op="$(read_summary_field "$batch_dir" 25)"
  set_total_ns_per_op="$(read_summary_field "$set_dir" 25)"
  multi_writes="$(read_summary_field "$multi_dir" 5)"
  batch_writes="$(read_summary_field "$batch_dir" 5)"
  set_writes="$(read_summary_field "$set_dir" 5)"
  multi_cv="$(read_summary_field "$multi_dir" 23)"
  batch_cv="$(read_summary_field "$batch_dir" 23)"
  set_cv="$(read_summary_field "$set_dir" 23)"
  multi_cpu="$(read_summary_field "$multi_dir" 13)"
  batch_cpu="$(read_summary_field "$batch_dir" 13)"
  set_cpu="$(read_summary_field "$set_dir" 13)"
  multi_cores="$(read_summary_field "$multi_dir" 14)"
  batch_cores="$(read_summary_field "$batch_dir" 14)"
  set_cores="$(read_summary_field "$set_dir" 14)"
  batch_delta_pct="$(awk -v multi="$multi_ns" -v candidate="$batch_ns" 'BEGIN { if (multi == 0) print "0.00"; else printf "%.2f", ((multi - candidate) / multi) * 100.0 }')"
  set_delta_pct="$(awk -v multi="$multi_ns" -v candidate="$set_ns" 'BEGIN { if (multi == 0) print "0.00"; else printf "%.2f", ((multi - candidate) / multi) * 100.0 }')"

  echo "$batch_size,$iters,$TARGET_WRITES,$multi_ns,$batch_ns,$set_ns,$multi_total_ns_per_op,$batch_total_ns_per_op,$set_total_ns_per_op,$batch_delta_pct,$set_delta_pct,$multi_writes,$batch_writes,$set_writes,$multi_cv,$batch_cv,$set_cv,$multi_cpu,$batch_cpu,$set_cpu,$multi_cores,$batch_cores,$set_cores,$multi_dir,$batch_dir,$set_dir" >> "$SUMMARY_CSV"
done

printf "\nSummary (positive delta means the candidate was faster than counter_multi):\n"
awk -F, '
  NR == 1 {
    next
  }
  {
    printf "  batch_size=%-4s cpu_ns/write multi=%6.2f batch=%6.2f set=%6.2f total_ns/op multi=%6.2f batch=%6.2f set=%6.2f delta batch=%7.2f%% set=%7.2f%% cv=%s/%s/%s\n",
      $1, $4, $5, $6, $7, $8, $9, $10, $11, $15, $16, $17
  }
' "$SUMMARY_CSV"

printf "\nDone. Summary in: %s\n" "$SUMMARY_CSV"

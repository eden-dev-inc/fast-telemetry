#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

THREADS="$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"
ITERS="1000000"
RUNS="3"
EXPORT_INTERVAL_MS="10"
MODES_CSV="fast,otel"
SCENARIO="root"
PIN=0
CPU_LIST=""
THREAD_AFFINITY="off"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="$2"; shift 2 ;;
    --iters) ITERS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --export-interval-ms) EXPORT_INTERVAL_MS="$2"; shift 2 ;;
    --modes) MODES_CSV="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --pin) PIN=1; shift ;;
    --cpu-list) CPU_LIST="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--threads N] [--iters N] [--runs N] [--scenario name] [--export-interval-ms N] [--modes list] [--pin] [--cpu-list list]"
      echo ""
      echo "Defaults: threads=nproc, iters=1000000, runs=3"
      echo "--modes comma-separated modes: fast,otel (default: fast,otel)"
      echo "--scenario one of: root, lifecycle, pipeline, all (default: root)"
      echo "  root:      create + drop a single root span"
      echo "  lifecycle: root + child span with attributes, events, status"
      echo "  pipeline:  root + 3 sequential children (validate, db_write, notify)"
      echo "  all:       run root, lifecycle, and pipeline workloads in one invocation"
      echo "--pin runs workload under taskset and enables thread round-robin affinity"
      echo "--cpu-list explicit taskset list (example: 0-15)"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

case "$SCENARIO" in
  root|lifecycle|pipeline|all) ;;
  *)
    echo "ERROR: unsupported --scenario '$SCENARIO'"
    exit 1
    ;;
esac

if [[ "$PIN" == "1" && -z "$CPU_LIST" ]]; then
  CPU_LIST="0-$((THREADS - 1))"
fi
if [[ "$PIN" == "1" ]]; then
  THREAD_AFFINITY="round_robin"
  command -v taskset >/dev/null 2>&1 || { echo "ERROR: taskset not found (util-linux)"; exit 1; }
fi

IFS=',' read -r -a MODES_ARR <<< "$MODES_CSV"
if [[ ${#MODES_ARR[@]} -eq 0 ]]; then
  echo "ERROR: --modes must include at least one mode"
  exit 1
fi
for mode in "${MODES_ARR[@]}"; do
  case "$mode" in
    fast|otel) ;;
    *)
      echo "ERROR: unsupported mode '$mode' (expected fast,otel)"
      exit 1
      ;;
  esac
done

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/span_${SCENARIO}_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

echo "=== fast-telemetry span contention benchmark ==="
echo "scenario=$SCENARIO threads=$THREADS iters=$ITERS runs=$RUNS"
echo "thread_affinity=$THREAD_AFFINITY"
if [[ "$PIN" == "1" ]]; then
  echo "pinning=taskset cpu_list=$CPU_LIST"
fi
echo "export_interval_ms=$EXPORT_INTERVAL_MS"
echo "modes=$MODES_CSV"
echo "results=$RUN_DIR"

cargo build --release --bin bench_span_contention --features bench-tools --manifest-path "$CRATE_DIR/Cargo.toml"

BIN="$CRATE_DIR/../../target/release/bench_span_contention"
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: benchmark binary not found at $BIN"
  exit 1
fi

run_scenario() {
  local scenario_name="$1"
  local out_dir="$2"

  mkdir -p "$out_dir"
  local fast_cmd=("$BIN" --mode fast --scenario "$scenario_name" --thread-affinity "$THREAD_AFFINITY" --threads "$THREADS" --iters "$ITERS" --export-interval-ms "$EXPORT_INTERVAL_MS")
  local otel_cmd=("$BIN" --mode otel --scenario "$scenario_name" --thread-affinity "$THREAD_AFFINITY" --threads "$THREADS" --iters "$ITERS" --export-interval-ms "$EXPORT_INTERVAL_MS")

  echo ""
  echo "=== scenario=$scenario_name ==="
  for run in $(seq 1 "$RUNS"); do
    for mode in "${MODES_ARR[@]}"; do
      echo "[run $run/$RUNS] $mode scenario=$scenario_name"
      case "$mode" in
        fast)
          if [[ "$PIN" == "1" ]]; then
            taskset -c "$CPU_LIST" "${fast_cmd[@]}" | tee "$out_dir/fast-run-${run}.txt"
          else
            "${fast_cmd[@]}" | tee "$out_dir/fast-run-${run}.txt"
          fi
          ;;
        otel)
          if [[ "$PIN" == "1" ]]; then
            taskset -c "$CPU_LIST" "${otel_cmd[@]}" | tee "$out_dir/otel-run-${run}.txt"
          else
            "${otel_cmd[@]}" | tee "$out_dir/otel-run-${run}.txt"
          fi
          ;;
      esac
    done
  done
  python3 "$SCRIPT_DIR/summarize_bench.py" "$out_dir" "$MODES_CSV"
}

if [[ "$SCENARIO" == "all" ]]; then
  run_scenario "root" "$RUN_DIR/root"
  run_scenario "lifecycle" "$RUN_DIR/lifecycle"
  run_scenario "pipeline" "$RUN_DIR/pipeline"
else
  run_scenario "$SCENARIO" "$RUN_DIR"
fi

echo ""
echo "Done. Results in: $RUN_DIR"

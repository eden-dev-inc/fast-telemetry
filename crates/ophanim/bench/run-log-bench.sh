#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOGGING_CRATE_DIR="$(cd "$SCRIPT_DIR/../logging" && pwd)"
WORKSPACE_ROOT="$(cd "$LOGGING_CRATE_DIR/../../.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

THREADS="$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"
ITERS="1000000"
RUNS="3"
MODES_CSV="eden,env"
SCENARIO="minimal"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="$2"; shift 2 ;;
    --iters) ITERS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --modes) MODES_CSV="$2"; shift 2 ;;
    --scenario) SCENARIO="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--threads N] [--iters N] [--runs N] [--scenario name] [--modes list]"
      echo ""
      echo "Defaults: threads=nproc, iters=1000000, runs=3"
      echo "--modes comma-separated modes: eden,env (default: eden,env)"
      echo "--scenario one of: minimal, rich, additional (default: minimal)"
      echo "  minimal:    log with minimal context (feature only)"
      echo "  rich:       log with full context (trace, span, org, user, endpoint)"
      echo "  additional: rich context + 3 additional key-value pairs"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

case "$SCENARIO" in
  minimal|rich|additional) ;;
  *)
    echo "ERROR: unsupported --scenario '$SCENARIO'"
    exit 1
    ;;
esac

IFS=',' read -r -a MODES_ARR <<< "$MODES_CSV"
if [[ ${#MODES_ARR[@]} -eq 0 ]]; then
  echo "ERROR: --modes must include at least one mode"
  exit 1
fi
for mode in "${MODES_ARR[@]}"; do
  case "$mode" in
    eden|env) ;;
    *)
      echo "ERROR: unsupported mode '$mode' (expected eden,env)"
      exit 1
      ;;
  esac
done

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/log_${SCENARIO}_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

echo "=== eden_logger contention benchmark ==="
echo "scenario=$SCENARIO threads=$THREADS iters=$ITERS runs=$RUNS"
echo "modes=$MODES_CSV"
echo "results=$RUN_DIR"

cargo build --release --example log_contention --manifest-path "$LOGGING_CRATE_DIR/Cargo.toml" --features full

BIN="$WORKSPACE_ROOT/target/release/examples/log_contention"
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: benchmark binary not found at $BIN"
  exit 1
fi

EDEN_CMD=("$BIN" --mode eden --scenario "$SCENARIO" --threads "$THREADS" --iters "$ITERS")
ENV_CMD=("$BIN" --mode env --scenario "$SCENARIO" --threads "$THREADS" --iters "$ITERS")

for run in $(seq 1 "$RUNS"); do
  for mode in "${MODES_ARR[@]}"; do
    echo "[run $run/$RUNS] $mode"
    case "$mode" in
      eden) "${EDEN_CMD[@]}" | tee "$RUN_DIR/eden-run-${run}.txt" ;;
      env) "${ENV_CMD[@]}" | tee "$RUN_DIR/env-run-${run}.txt" ;;
    esac
  done
done

python3 "$SCRIPT_DIR/summarize_bench.py" "$RUN_DIR" "$MODES_CSV"

echo ""
echo "Done. Results in: $RUN_DIR"

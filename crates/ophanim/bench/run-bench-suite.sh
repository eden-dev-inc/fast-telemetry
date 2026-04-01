#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

THREADS="$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"
RUNS="5"
PRESET="quick"
MODES_CSV="fast,otel,atomic"
ITERS_CACHE="10000000"
ITERS_SPAN="300000"
EXPORT_INTERVAL_MS="10"
ITERS_CACHE_SET=0
ITERS_SPAN_SET=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --preset) PRESET="$2"; shift 2 ;;
    --modes) MODES_CSV="$2"; shift 2 ;;
    --iters-cache) ITERS_CACHE="$2"; ITERS_CACHE_SET=1; shift 2 ;;
    --iters-span) ITERS_SPAN="$2"; ITERS_SPAN_SET=1; shift 2 ;;
    --export-interval-ms) EXPORT_INTERVAL_MS="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--threads N] [--runs N] [--preset quick|full] [--modes list] [--iters-cache N] [--iters-span N] [--export-interval-ms N]"
      echo ""
      echo "Runs matrix workloads and generates an HTML report with SVG charts."
      echo "Default modes: fast,otel,atomic (atomic applies only to cache counter cases)"
      echo "Preset quick defaults: iters-cache=10000000, iters-span=300000"
      echo "Preset full defaults:  iters-cache=50000000, iters-span=1000000"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

if [[ "$PRESET" == "full" ]]; then
  if [[ "$ITERS_CACHE_SET" == "0" ]]; then
    ITERS_CACHE="50000000"
  fi
  if [[ "$ITERS_SPAN_SET" == "0" ]]; then
    ITERS_SPAN="1000000"
  fi
fi

mkdir -p "$RESULTS_DIR"
SINCE_EPOCH="$(date +%s)"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
SUITE_DIR="$RESULTS_DIR/suite_${TIMESTAMP}"
mkdir -p "$SUITE_DIR"

echo "=== ophanim bench suite ==="
echo "threads=$THREADS runs=$RUNS preset=$PRESET modes=$MODES_CSV"
echo "iters_cache=$ITERS_CACHE iters_span=$ITERS_SPAN export_interval_ms=$EXPORT_INTERVAL_MS"
echo "suite_dir=$SUITE_DIR"

"$SCRIPT_DIR/run-bench-matrix.sh" \
  --threads "$THREADS" \
  --runs "$RUNS" \
  --preset "$PRESET" \
  --modes "$MODES_CSV" \
  --iters-cache "$ITERS_CACHE" \
  --iters-span "$ITERS_SPAN" \
  --export-interval-ms "$EXPORT_INTERVAL_MS" \
  | tee "$SUITE_DIR/matrix.log"

python3 "$SCRIPT_DIR/render_suite_report.py" \
  --results-dir "$RESULTS_DIR" \
  --since-epoch "$SINCE_EPOCH" \
  --output "$SUITE_DIR/report.html"

echo ""
echo "Suite complete."
echo "Report: $SUITE_DIR/report.html"

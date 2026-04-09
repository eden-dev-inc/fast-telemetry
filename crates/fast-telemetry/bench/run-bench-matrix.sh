#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

THREADS="$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"
ITERS_CACHE="10000000"
ITERS_SPAN="300000"
RUNS="3"
PRESET="quick"
EXPORT_INTERVAL_MS="10"
MODES_CSV="fast,otel"
ITERS_CACHE_SET=0
ITERS_SPAN_SET=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="$2"; shift 2 ;;
    --iters-cache) ITERS_CACHE="$2"; ITERS_CACHE_SET=1; shift 2 ;;
    --iters-span) ITERS_SPAN="$2"; ITERS_SPAN_SET=1; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --preset) PRESET="$2"; shift 2 ;;
    --export-interval-ms) EXPORT_INTERVAL_MS="$2"; shift 2 ;;
    --modes) MODES_CSV="$2"; shift 2 ;;
    --help)
      echo "Usage: $0 [--threads N] [--iters-cache N] [--iters-span N] [--runs N] [--preset quick|full] [--export-interval-ms N] [--modes list]"
      echo ""
      echo "Preset quick:"
      echo "  cache entities: counter,dynamic_counter,labeled_counter,dynamic_histogram"
      echo "  cache profiles: uniform,hotspot"
      echo "  span scenarios: root,lifecycle,pipeline"
      echo "  default iters: cache=10000000 span=300000"
      echo "Preset full:"
      echo "  cache entities: all cache benchmark entities"
      echo "  cache profiles: uniform,hotspot,churn"
      echo "  span scenarios: root,lifecycle,pipeline"
      echo "  default iters: cache=50000000 span=1000000"
      echo "Modes may include metrics for cache entities with direct metrics-rs equivalents."
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

case "$PRESET" in
  quick|full) ;;
  *)
    echo "ERROR: unsupported --preset '$PRESET' (expected quick|full)"
    exit 1
    ;;
esac

if [[ "$PRESET" == "full" ]]; then
  if [[ "$ITERS_CACHE_SET" == "0" ]]; then
    ITERS_CACHE="50000000"
  fi
  if [[ "$ITERS_SPAN_SET" == "0" ]]; then
    ITERS_SPAN="1000000"
  fi
fi

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/matrix_${PRESET}_${TIMESTAMP}"
mkdir -p "$RUN_DIR/cache" "$RUN_DIR/span"

echo "=== fast-telemetry benchmark matrix ==="
echo "preset=$PRESET threads=$THREADS runs=$RUNS modes=$MODES_CSV"
echo "iters_cache=$ITERS_CACHE iters_span=$ITERS_SPAN export_interval_ms=$EXPORT_INTERVAL_MS"
echo "results=$RUN_DIR"

if [[ "$PRESET" == "quick" ]]; then
  CACHE_ENTITIES=(counter dynamic_counter labeled_counter dynamic_histogram)
  CACHE_PROFILES=(uniform hotspot)
else
  CACHE_ENTITIES=(
    counter
    distribution
    dynamic_counter
    dynamic_distribution
    dynamic_gauge
    dynamic_gauge_i64
    dynamic_histogram
    labeled_counter
    labeled_gauge
    labeled_histogram
  )
  CACHE_PROFILES=(uniform hotspot churn)
fi
SPAN_SCENARIOS=(root lifecycle pipeline)
SPAN_MODES="$(echo "$MODES_CSV" | sed -E 's/(^|,)(atomic|metrics)(,|$)/\1\3/g' | sed -E 's/,,+/,/g; s/^,//; s/,$//')"
if [[ -z "$SPAN_MODES" ]]; then
  echo "ERROR: no valid modes left for span benchmarks (span supports fast,otel)"
  exit 1
fi

for entity in "${CACHE_ENTITIES[@]}"; do
  for profile in "${CACHE_PROFILES[@]}"; do
    case_name="${entity}_${profile}"
    echo ""
    echo "[cache] case=$case_name"
    CASE_MODES="$MODES_CSV"
    if [[ "$entity" != "counter" ]]; then
      CASE_MODES="$(echo "$MODES_CSV" | sed -E 's/(^|,)atomic(,|$)/\1\2/g' | sed -E 's/,,+/,/g; s/^,//; s/,$//')"
    fi
    if [[ "$entity" == "distribution" || "$entity" == "dynamic_distribution" ]]; then
      CASE_MODES="$(echo "$CASE_MODES" | sed -E 's/(^|,)metrics(,|$)/\1\2/g' | sed -E 's/,,+/,/g; s/^,//; s/,$//')"
    fi
    if [[ -z "$CASE_MODES" ]]; then
      echo "ERROR: no valid modes left for entity=$entity after filtering unsupported modes"
      exit 1
    fi
    "$SCRIPT_DIR/run-cache-bench.sh" \
      --threads "$THREADS" \
      --iters "$ITERS_CACHE" \
      --runs "$RUNS" \
      --entity "$entity" \
      --profile "$profile" \
      --export-interval-ms "$EXPORT_INTERVAL_MS" \
      --modes "$CASE_MODES"
  done
done

for scenario in "${SPAN_SCENARIOS[@]}"; do
  echo ""
  echo "[span] scenario=$scenario"
  "$SCRIPT_DIR/run-span-bench.sh" \
    --threads "$THREADS" \
    --iters "$ITERS_SPAN" \
    --runs "$RUNS" \
    --scenario "$scenario" \
    --export-interval-ms "$EXPORT_INTERVAL_MS" \
    --modes "$SPAN_MODES"
done

echo ""
echo "Done. Matrix runs completed."
echo "Note: each invoked script writes to its own timestamped subdirectory under bench/results/."

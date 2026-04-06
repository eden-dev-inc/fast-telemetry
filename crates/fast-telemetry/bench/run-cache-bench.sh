#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
COLLECTOR_SCRIPT="$SCRIPT_DIR/run-dogstatsd-collector.sh"
COLLECTOR_URL="http://127.0.0.1:9102/metrics"

THREADS="$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)"
ITERS="10000000"
SHARDS="${THREADS}"
SHARDS_SET=0
RUNS="3"
PERF_STAT=0
PERF_RECORD=0
PERF_FREQ="99"
PIN=0
CPU_LIST=""
EXPORT_INTERVAL_MS="10"
MODES_CSV="fast,otel"
ENTITY="counter"
LABELS="16"
PROFILE="uniform"
THREAD_AFFINITY="off"
VALIDATE_EXPORT=0
COLLECTOR=0
COLLECTOR_AUTO_DOWN=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --threads) THREADS="$2"; shift 2 ;;
    --iters) ITERS="$2"; shift 2 ;;
    --shards) SHARDS="$2"; SHARDS_SET=1; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --export-interval-ms) EXPORT_INTERVAL_MS="$2"; shift 2 ;;
    --modes) MODES_CSV="$2"; shift 2 ;;
    --entity) ENTITY="$2"; shift 2 ;;
    --labels) LABELS="$2"; shift 2 ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --validate-export) VALIDATE_EXPORT=1; shift ;;
    --collector) COLLECTOR=1; shift ;;
    --collector-keep-up)
      COLLECTOR=1
      COLLECTOR_AUTO_DOWN=0
      shift
      ;;
    --pin) PIN=1; shift ;;
    --cpu-list) CPU_LIST="$2"; shift 2 ;;
    --perf)
      PERF_STAT=1
      PERF_RECORD=1
      shift
      ;;
    --perf-stat)
      PERF_STAT=1
      shift
      ;;
    --perf-record)
      PERF_RECORD=1
      shift
      ;;
    --perf-freq)
      PERF_FREQ="$2"
      shift 2
      ;;
    --help)
      echo "Usage: $0 [--threads N] [--iters N] [--shards N] [--runs N] [--entity name] [--labels N] [--profile name] [--export-interval-ms N] [--modes list] [--validate-export] [--collector|--collector-keep-up] [--pin] [--cpu-list list] [--perf] [--perf-stat] [--perf-record] [--perf-freq N]"
      echo ""
      echo "Defaults: threads=nproc, iters=10000000, shards=threads, runs=3"
      echo "--modes comma-separated modes: fast,otel,atomic (default: fast,otel)"
      echo "--entity one of: counter,distribution,dynamic_counter,dynamic_distribution,dynamic_gauge,dynamic_gauge_i64,dynamic_histogram,labeled_counter,labeled_gauge,labeled_histogram"
      echo "--labels label cardinality for labeled entities (default: 16)"
      echo "--profile access pattern: uniform,hotspot,churn (default: uniform)"
      echo "--validate-export run export/parity acceptance tests before benchmark"
      echo "--collector start local DogStatsD collector and snapshot metrics"
      echo "--collector-keep-up same as --collector but leaves collector running"
      echo "--perf enables both perf stat and perf record"
      echo "--pin runs benchmark under taskset and enables thread round-robin affinity"
      echo "--cpu-list explicit taskset list (example: 0-15)"
      exit 0
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

case "$ENTITY" in
  counter|distribution|dynamic_counter|dynamic_distribution|dynamic_gauge|dynamic_gauge_i64|dynamic_histogram|labeled_counter|labeled_gauge|labeled_histogram) ;;
  *)
    echo "ERROR: unsupported --entity '$ENTITY'"
    exit 1
    ;;
esac

case "$PROFILE" in
  uniform|hotspot|churn) ;;
  *)
    echo "ERROR: unsupported --profile '$PROFILE'"
    exit 1
    ;;
esac

if [[ "$SHARDS_SET" == "0" ]]; then
  SHARDS="$THREADS"
fi

if [[ "$PIN" == "1" && -z "$CPU_LIST" ]]; then
  CPU_LIST="0-$((THREADS - 1))"
fi
if [[ "$PIN" == "1" ]]; then
  THREAD_AFFINITY="round_robin"
fi

if [[ "$PIN" == "1" ]]; then
  command -v taskset >/dev/null 2>&1 || { echo "ERROR: taskset not found (util-linux)"; exit 1; }
fi

IFS=',' read -r -a MODES_ARR <<< "$MODES_CSV"
if [[ ${#MODES_ARR[@]} -eq 0 ]]; then
  echo "ERROR: --modes must include at least one mode"
  exit 1
fi
for mode in "${MODES_ARR[@]}"; do
  case "$mode" in
    fast|otel|atomic) ;;
    *)
      echo "ERROR: unsupported mode '$mode' (expected fast,otel,atomic)"
      exit 1
      ;;
  esac
done

if [[ "$ENTITY" != "counter" ]]; then
  for mode in "${MODES_ARR[@]}"; do
    if [[ "$mode" == "atomic" ]]; then
      echo "ERROR: mode=atomic is only valid with --entity counter"
      exit 1
    fi
  done
fi

mkdir -p "$RESULTS_DIR"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="$RESULTS_DIR/cache_${TIMESTAMP}"
mkdir -p "$RUN_DIR"

if [[ "$PERF_STAT" == "1" || "$PERF_RECORD" == "1" ]]; then
  echo "perf requested; sudo is needed"
  sudo -v || { echo "ERROR: sudo required for perf options"; exit 1; }
fi

echo "=== fast-telemetry cache contention benchmark ==="
echo "threads=$THREADS iters=$ITERS shards=$SHARDS runs=$RUNS"
echo "entity=$ENTITY labels=$LABELS profile=$PROFILE"
echo "thread_affinity=$THREAD_AFFINITY"
echo "export_interval_ms=$EXPORT_INTERVAL_MS"
echo "validate_export=$VALIDATE_EXPORT"
echo "collector=$COLLECTOR"
echo "modes=$MODES_CSV"
if [[ "$PIN" == "1" ]]; then
  echo "pinning=taskset cpu_list=$CPU_LIST"
fi
echo "results=$RUN_DIR"

cargo build --release --bin bench_cache_contention --features bench-tools --manifest-path "$CRATE_DIR/Cargo.toml"

if [[ "$VALIDATE_EXPORT" == "1" ]]; then
  echo "[validation] running dogstatsd + otel parity acceptance tests"
  cargo test -p fast-telemetry --test dogstatsd_validation_test --test otel_parity_test
fi

if [[ "$COLLECTOR" == "1" ]]; then
  if [[ ! -x "$COLLECTOR_SCRIPT" ]]; then
    echo "ERROR: collector script not found/executable at $COLLECTOR_SCRIPT"
    exit 1
  fi
  echo "[collector] starting local DogStatsD collector"
  "$COLLECTOR_SCRIPT" up
  if [[ "$COLLECTOR_AUTO_DOWN" == "1" ]]; then
    trap '"$COLLECTOR_SCRIPT" down >/dev/null 2>&1 || true' EXIT
  fi
fi

BIN="$CRATE_DIR/../../target/release/bench_cache_contention"
if [[ ! -x "$BIN" ]]; then
  echo "ERROR: benchmark binary not found at $BIN"
  exit 1
fi

FAST_CMD=("$BIN" --mode fast --entity "$ENTITY" --profile "$PROFILE" --thread-affinity "$THREAD_AFFINITY" --threads "$THREADS" --iters "$ITERS" --shards "$SHARDS" --labels "$LABELS" --export-interval-ms "$EXPORT_INTERVAL_MS")
ATOMIC_CMD=("$BIN" --mode atomic --entity "$ENTITY" --profile "$PROFILE" --thread-affinity "$THREAD_AFFINITY" --threads "$THREADS" --iters "$ITERS" --shards "$SHARDS" --labels "$LABELS" --export-interval-ms "$EXPORT_INTERVAL_MS")
OTEL_CMD=("$BIN" --mode otel --entity "$ENTITY" --profile "$PROFILE" --thread-affinity "$THREAD_AFFINITY" --threads "$THREADS" --iters "$ITERS" --shards "$SHARDS" --labels "$LABELS" --export-interval-ms "$EXPORT_INTERVAL_MS")

run_cmd() {
  if [[ "$PIN" == "1" ]]; then
    taskset -c "$CPU_LIST" "$@"
  else
    "$@"
  fi
}

send_udp_line() {
  local line="$1"
  bash -c "exec 3<>/dev/udp/127.0.0.1/8125; printf '%s\n' \"$line\" >&3; exec 3>&-"
}

for run in $(seq 1 "$RUNS"); do
  for mode in "${MODES_ARR[@]}"; do
    echo "[run $run/$RUNS] $mode"
    case "$mode" in
      fast) run_cmd "${FAST_CMD[@]}" | tee "$RUN_DIR/fast-run-${run}.txt" ;;
      atomic) run_cmd "${ATOMIC_CMD[@]}" | tee "$RUN_DIR/atomic-run-${run}.txt" ;;
      otel) run_cmd "${OTEL_CMD[@]}" | tee "$RUN_DIR/otel-run-${run}.txt" ;;
    esac
  done
done

if [[ "$COLLECTOR" == "1" ]]; then
  echo "[collector] emitting benchmark summary lines"
  for mode in "${MODES_ARR[@]}"; do
    for run_file in "$RUN_DIR"/${mode}-run-*.txt; do
      [[ -f "$run_file" ]] || continue
      run_id="$(basename "$run_file" | sed -E 's/.*-run-([0-9]+)\.txt/\1/')"
      total_ops_per_sec="$(grep -E '^total_ops_per_sec=' "$run_file" | cut -d= -f2)"
      record_ops_per_sec="$(grep -E '^record_ops_per_sec=' "$run_file" | cut -d= -f2)"
      export_avg_ms="$(grep -E '^export_avg_ms=' "$run_file" | cut -d= -f2)"

      if [[ -n "$total_ops_per_sec" ]]; then
        send_udp_line "ft.bench.total_ops_per_sec:${total_ops_per_sec}|g|#mode:${mode},entity:${ENTITY},profile:${PROFILE},run:${run_id}"
      fi
      if [[ -n "$record_ops_per_sec" ]]; then
        send_udp_line "ft.bench.record_ops_per_sec:${record_ops_per_sec}|g|#mode:${mode},entity:${ENTITY},profile:${PROFILE},run:${run_id}"
      fi
      if [[ -n "$export_avg_ms" ]]; then
        send_udp_line "ft.bench.export_avg_ms:${export_avg_ms}|g|#mode:${mode},entity:${ENTITY},profile:${PROFILE},run:${run_id}"
      fi
    done
  done

  sleep 1
  echo "[collector] scraping metrics snapshot"
  curl -fsS "$COLLECTOR_URL" > "$RUN_DIR/dogstatsd-collector-metrics.txt"
fi

if [[ "$PERF_STAT" == "1" ]]; then
  EVENTS="cycles,instructions,cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses"
  for mode in "${MODES_ARR[@]}"; do
    echo "[perf stat] $mode"
    case "$mode" in
      fast)
        if [[ "$PIN" == "1" ]]; then
          sudo perf stat -e "$EVENTS" -o "$RUN_DIR/perf-fast.txt" -- taskset -c "$CPU_LIST" "${FAST_CMD[@]}"
        else
          sudo perf stat -e "$EVENTS" -o "$RUN_DIR/perf-fast.txt" -- "${FAST_CMD[@]}"
        fi
        sudo chown "$(id -u):$(id -g)" "$RUN_DIR/perf-fast.txt"
        ;;
      atomic)
        if [[ "$PIN" == "1" ]]; then
          sudo perf stat -e "$EVENTS" -o "$RUN_DIR/perf-atomic.txt" -- taskset -c "$CPU_LIST" "${ATOMIC_CMD[@]}"
        else
          sudo perf stat -e "$EVENTS" -o "$RUN_DIR/perf-atomic.txt" -- "${ATOMIC_CMD[@]}"
        fi
        sudo chown "$(id -u):$(id -g)" "$RUN_DIR/perf-atomic.txt"
        ;;
      otel)
        if [[ "$PIN" == "1" ]]; then
          sudo perf stat -e "$EVENTS" -o "$RUN_DIR/perf-otel.txt" -- taskset -c "$CPU_LIST" "${OTEL_CMD[@]}"
        else
          sudo perf stat -e "$EVENTS" -o "$RUN_DIR/perf-otel.txt" -- "${OTEL_CMD[@]}"
        fi
        sudo chown "$(id -u):$(id -g)" "$RUN_DIR/perf-otel.txt"
        ;;
    esac
  done
fi

if [[ "$PERF_RECORD" == "1" ]]; then
  for mode in "${MODES_ARR[@]}"; do
    echo "[perf record] $mode"
    case "$mode" in
      fast)
        if [[ "$PIN" == "1" ]]; then
          sudo perf record -g -F "$PERF_FREQ" -o "$RUN_DIR/perf-fast.data" -- taskset -c "$CPU_LIST" "${FAST_CMD[@]}"
        else
          sudo perf record -g -F "$PERF_FREQ" -o "$RUN_DIR/perf-fast.data" -- "${FAST_CMD[@]}"
        fi
        sudo chown "$(id -u):$(id -g)" "$RUN_DIR/perf-fast.data"
        ;;
      atomic)
        if [[ "$PIN" == "1" ]]; then
          sudo perf record -g -F "$PERF_FREQ" -o "$RUN_DIR/perf-atomic.data" -- taskset -c "$CPU_LIST" "${ATOMIC_CMD[@]}"
        else
          sudo perf record -g -F "$PERF_FREQ" -o "$RUN_DIR/perf-atomic.data" -- "${ATOMIC_CMD[@]}"
        fi
        sudo chown "$(id -u):$(id -g)" "$RUN_DIR/perf-atomic.data"
        ;;
      otel)
        if [[ "$PIN" == "1" ]]; then
          sudo perf record -g -F "$PERF_FREQ" -o "$RUN_DIR/perf-otel.data" -- taskset -c "$CPU_LIST" "${OTEL_CMD[@]}"
        else
          sudo perf record -g -F "$PERF_FREQ" -o "$RUN_DIR/perf-otel.data" -- "${OTEL_CMD[@]}"
        fi
        sudo chown "$(id -u):$(id -g)" "$RUN_DIR/perf-otel.data"
        ;;
    esac
  done

  echo "[perf report] generating summaries"
  for mode in "${MODES_ARR[@]}"; do
    if [[ -f "$RUN_DIR/perf-$mode.data" ]]; then
      perf report -i "$RUN_DIR/perf-$mode.data" --stdio --no-children -g none --percent-limit 1 > "$RUN_DIR/perf-$mode-report.txt" 2>/dev/null || true
    fi
  done
fi

python3 "$SCRIPT_DIR/summarize_bench.py" "$RUN_DIR" "$MODES_CSV"

if ls "$RUN_DIR"/perf-*.txt >/dev/null 2>&1; then
  python3 "$SCRIPT_DIR/summarize_perf.py" "$RUN_DIR" "$MODES_CSV"
fi

echo ""
echo "Done. Results in: $RUN_DIR"

if [[ "$COLLECTOR" == "1" && "$COLLECTOR_AUTO_DOWN" == "0" ]]; then
  echo "Collector left running (requested via --collector-keep-up)."
fi

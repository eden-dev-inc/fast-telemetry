#!/usr/bin/env bash
set -euo pipefail

# Preflight check: refuses to run benches when the box is too noisy to produce
# decision-quality numbers. Captures host metadata so retroactive comparisons
# can flag dirty runs.
#
# Usage:
#   preflight.sh <host_json_out_path>
#
# Env:
#   BENCH_SKIP_PREFLIGHT=1   bypass the load-avg block (warning still printed)
#   BENCH_LOAD_THRESHOLD=N   override load-avg threshold (default cores/8)
#   BENCH_TOP_PROC_PCT=N     override per-proc warn threshold (default 5)

HOST_JSON_OUT="${1:?usage: preflight.sh <host_json_out>}"

if [[ "$(uname -s)" == "Darwin" ]]; then
  CORES="$(sysctl -n hw.ncpu)"
  CPU_BRAND="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)"
  MEM_BYTES="$(sysctl -n hw.memsize 2>/dev/null || echo 0)"
else
  CORES="$(nproc)"
  CPU_BRAND="$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed -E 's/^ +//' || echo unknown)"
  MEM_BYTES="$(awk '/MemTotal/ {print $2 * 1024}' /proc/meminfo 2>/dev/null || echo 0)"
fi

LOAD_THRESHOLD="${BENCH_LOAD_THRESHOLD:-$(awk -v c="$CORES" 'BEGIN { printf "%.2f", c / 8.0 }')}"
TOP_PROC_PCT="${BENCH_TOP_PROC_PCT:-5}"

LOAD_LINE="$(uptime | sed -E 's/.*load averages?: //' | tr -d ',')"
LOAD_1="$(awk '{print $1}' <<<"$LOAD_LINE")"
LOAD_5="$(awk '{print $2}' <<<"$LOAD_LINE")"
LOAD_15="$(awk '{print $3}' <<<"$LOAD_LINE")"

# Top non-bench CPU consumers (pcpu, comm). Trim full paths to basename.
TOP_RAW="$(ps -A -o pcpu,comm | sort -k1 -nr | awk 'NR>0 && $1+0 > 0 { sub(".*/", "", $2); print }' | head -10)"

GIT_REV="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
  GIT_DIRTY="true"
else
  GIT_DIRTY="false"
fi
RUSTC_VERSION="$(rustc --version 2>/dev/null || echo unknown)"
KERNEL="$(uname -sr)"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

echo "=== bench preflight ==="
echo "host:    $CPU_BRAND ($CORES cores)"
echo "load:    $LOAD_1 / $LOAD_5 / $LOAD_15 (1m / 5m / 15m), threshold $LOAD_THRESHOLD"
echo "git:     $GIT_REV (dirty=$GIT_DIRTY)"
echo "rustc:   $RUSTC_VERSION"

OVER_THRESHOLD="$(awk -v l="$LOAD_1" -v t="$LOAD_THRESHOLD" 'BEGIN { print (l > t) ? 1 : 0 }')"

NOISY_PROCS=""
while IFS= read -r line; do
  pcpu="$(awk '{print $1}' <<<"$line")"
  comm="$(awk '{$1=""; sub(/^ +/, ""); print}' <<<"$line")"
  over="$(awk -v p="$pcpu" -v t="$TOP_PROC_PCT" 'BEGIN { print (p+0 >= t+0) ? 1 : 0 }')"
  if [[ "$over" == "1" ]]; then
    NOISY_PROCS+="    $line"$'\n'
  fi
done <<< "$TOP_RAW"

if [[ -n "$NOISY_PROCS" ]]; then
  echo ""
  echo "noisy neighbors (>=${TOP_PROC_PCT}% CPU):"
  printf "%s" "$NOISY_PROCS"
fi

# Build host.json.
TOP_JSON_LINES=""
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  pcpu="$(awk '{print $1}' <<<"$line")"
  comm="$(awk '{$1=""; sub(/^ +/, ""); gsub(/"/, "\\\""); print}' <<<"$line")"
  TOP_JSON_LINES+="    {\"pcpu\": $pcpu, \"comm\": \"$comm\"},"$'\n'
done <<< "$TOP_RAW"
TOP_JSON_LINES="${TOP_JSON_LINES%,$'\n'}"

mkdir -p "$(dirname "$HOST_JSON_OUT")"
cat > "$HOST_JSON_OUT" <<EOF
{
  "timestamp_utc": "$TIMESTAMP",
  "kernel": "$KERNEL",
  "cpu_brand": "$CPU_BRAND",
  "cores": $CORES,
  "mem_bytes": $MEM_BYTES,
  "load_avg": { "1min": $LOAD_1, "5min": $LOAD_5, "15min": $LOAD_15 },
  "load_threshold": $LOAD_THRESHOLD,
  "git_rev": "$GIT_REV",
  "git_dirty": $GIT_DIRTY,
  "rustc": "$RUSTC_VERSION",
  "top_consumers": [
$TOP_JSON_LINES
  ]
}
EOF

echo ""
echo "host.json: $HOST_JSON_OUT"

if [[ "$OVER_THRESHOLD" == "1" ]]; then
  if [[ "${BENCH_SKIP_PREFLIGHT:-0}" == "1" ]]; then
    echo ""
    echo "WARNING: 1-min load avg $LOAD_1 exceeds $LOAD_THRESHOLD; running anyway (BENCH_SKIP_PREFLIGHT=1)."
    exit 0
  else
    echo ""
    echo "ERROR: 1-min load avg $LOAD_1 exceeds threshold $LOAD_THRESHOLD."
    echo "       The bench will measure noise, not signal. Quit the listed apps and rerun,"
    echo "       or set BENCH_SKIP_PREFLIGHT=1 to bypass."
    exit 1
  fi
fi

exit 0

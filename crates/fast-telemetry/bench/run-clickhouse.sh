#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.clickhouse.yml"
HTTP_URL="http://127.0.0.1:8123"
NATIVE_ENDPOINT="127.0.0.1:9000"

usage() {
  echo "Usage: $0 <up|down|status|scrape|smoke|bench> [cargo bench args...]"
  echo ""
  echo "Commands:"
  echo "  up      Start local ClickHouse server (HTTP :8123 + native :9000)"
  echo "  down    Stop and remove local ClickHouse server"
  echo "  status  Show container status"
  echo "  scrape  Print row counts from each otel_metrics_* table"
  echo "  smoke   Verify HTTP /ping + insert + select round-trip"
  echo "  bench   Run ClickHouse export cost Criterion benchmarks"
}

cmd="${1:-}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$cmd" in
  up)
    docker compose -f "$COMPOSE_FILE" up -d
    echo "ClickHouse started."
    echo "  HTTP:   $HTTP_URL"
    echo "  Native: $NATIVE_ENDPOINT"
    ;;
  down)
    docker compose -f "$COMPOSE_FILE" down
    ;;
  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  scrape)
    for table in otel_metrics_sum otel_metrics_gauge otel_metrics_histogram otel_metrics_exponential_histogram; do
      printf '%s: ' "$table"
      curl -fsS --data-urlencode "query=SELECT count() FROM default.$table" "$HTTP_URL/" \
        || echo "(table missing or query failed)"
    done
    ;;
  smoke)
    docker compose -f "$COMPOSE_FILE" up -d
    echo "Waiting for ClickHouse to start..."
    for i in $(seq 1 30); do
      if curl -fsS -o /dev/null "$HTTP_URL/ping" 2>/dev/null; then
        break
      fi
      sleep 1
    done

    if ! curl -fsS -o /dev/null "$HTTP_URL/ping"; then
      echo "Smoke test failed: /ping never returned 200"
      exit 1
    fi

    # Round-trip a sample row through the HTTP interface.
    curl -fsS --data 'CREATE TABLE IF NOT EXISTS default.ft_smoke (v UInt64) ENGINE = Memory' "$HTTP_URL/" >/dev/null
    curl -fsS --data 'INSERT INTO default.ft_smoke VALUES (42)' "$HTTP_URL/" >/dev/null
    COUNT=$(curl -fsS --data 'SELECT count() FROM default.ft_smoke' "$HTTP_URL/" | tr -d '[:space:]')

    if [[ "$COUNT" == "1" ]]; then
      echo "Smoke test passed: round-trip insert + select returned 1 row"
    else
      echo "Smoke test failed: expected 1 row, got '$COUNT'"
      exit 1
    fi

    curl -fsS --data 'DROP TABLE default.ft_smoke' "$HTTP_URL/" >/dev/null
    ;;
  bench)
    cargo bench -p fast-telemetry-export --features clickhouse --bench clickhouse_export -- "$@"
    ;;
  *)
    usage
    exit 1
    ;;
esac

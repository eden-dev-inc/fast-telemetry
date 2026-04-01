#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.otlp.yml"
OTLP_URL="http://127.0.0.1:4318/v1/metrics"
METRICS_URL="http://127.0.0.1:8889/metrics"

usage() {
  echo "Usage: $0 <up|down|status|scrape|smoke>"
  echo ""
  echo "Commands:"
  echo "  up      Start local OTel Collector (OTLP HTTP + Prometheus exporter)"
  echo "  down    Stop and remove local collector"
  echo "  status  Show container status"
  echo "  scrape  Fetch and print Prometheus metrics endpoint"
  echo "  smoke   Send sample OTLP metrics and verify collection"
}

cmd="${1:-}"

case "$cmd" in
  up)
    docker compose -f "$COMPOSE_FILE" up -d
    echo "OTel Collector started."
    echo "  OTLP HTTP: $OTLP_URL"
    echo "  Prometheus scrape: $METRICS_URL"
    ;;
  down)
    docker compose -f "$COMPOSE_FILE" down
    ;;
  status)
    docker compose -f "$COMPOSE_FILE" ps
    ;;
  scrape)
    curl -fsS "$METRICS_URL"
    ;;
  smoke)
    docker compose -f "$COMPOSE_FILE" up -d
    echo "Waiting for collector to start..."
    for i in $(seq 1 10); do
      if curl -fsS -o /dev/null "$METRICS_URL" 2>/dev/null; then
        break
      fi
      sleep 1
    done

    # Send a minimal OTLP ExportMetricsServiceRequest via HTTP/JSON
    # (the collector accepts JSON on the same endpoint)
    PAYLOAD='{
      "resourceMetrics": [{
        "resource": {
          "attributes": [{
            "key": "service.name",
            "value": {"stringValue": "ophanim-smoke"}
          }]
        },
        "scopeMetrics": [{
          "scope": {"name": "smoke-test"},
          "metrics": [{
            "name": "ft_smoke_counter",
            "sum": {
              "dataPoints": [{
                "asInt": "42",
                "timeUnixNano": "1700000000000000000"
              }],
              "aggregationTemporality": 2,
              "isMonotonic": true
            }
          }]
        }]
      }]
    }'

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST "$OTLP_URL" \
      -H "Content-Type: application/json" \
      -d "$PAYLOAD")

    if [[ "$HTTP_CODE" == "200" ]]; then
      echo "OTLP POST accepted (HTTP $HTTP_CODE)"
    else
      echo "OTLP POST failed (HTTP $HTTP_CODE)"
      exit 1
    fi

    sleep 2

    if curl -fsS "$METRICS_URL" | grep -q "ft_smoke_counter"; then
      echo "Smoke test passed: metric 'ft_smoke_counter' found in Prometheus output"
    else
      echo "Smoke test failed: 'ft_smoke_counter' not found in Prometheus output"
      echo "Prometheus output:"
      curl -fsS "$METRICS_URL" | head -40
      exit 1
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac

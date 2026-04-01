#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.dogstatsd.yml"
METRICS_URL="http://127.0.0.1:9102/metrics"

usage() {
  echo "Usage: $0 <up|down|status|scrape|smoke>"
  echo ""
  echo "Commands:"
  echo "  up      Start local DogStatsD collector (statsd-exporter)"
  echo "  down    Stop and remove local collector"
  echo "  status  Show container status"
  echo "  scrape  Fetch and print Prometheus metrics endpoint"
  echo "  smoke   Send sample DogStatsD lines and verify collection"
}

send_udp_line() {
  local line="$1"
  bash -c "exec 3<>/dev/udp/127.0.0.1/8125; printf '%s\n' \"$line\" >&3; exec 3>&-"
}

cmd="${1:-}"

case "$cmd" in
  up)
    docker compose -f "$COMPOSE_FILE" up -d
    echo "Collector started. UDP: 127.0.0.1:8125, scrape: $METRICS_URL"
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
    send_udp_line "ft.smoke.counter:3|c|#entity:counter,source:demo"
    send_udp_line "ft.smoke.gauge:42|g|#entity:gauge,source:demo"
    send_udp_line "ft.smoke.hist:7|h|#entity:hist,source:demo"
    sleep 1
    if curl -fsS "$METRICS_URL" | grep -Eq "ft_smoke_counter|ft_smoke_gauge|ft_smoke_hist"; then
      echo "Smoke test passed: metrics collected"
    else
      echo "Smoke test failed: expected metrics not found"
      exit 1
    fi
    ;;
  *)
    usage
    exit 1
    ;;
esac

#!/usr/bin/env bash
# Starts everything needed to try grab-mini locally:
#   infra (Kafka, Schema Registry, ksqlDB, Kafka UI, Postgres, Redis) -> seeded drivers -> the Go
#   services -> the driver location simulator -> the Rider, Driver and Ops apps.
# Ctrl-C stops everything except the Docker infra (use `docker compose down`).
#
#   scripts/dev.sh            start
#   scripts/dev.sh --reset    wipe Kafka/Postgres/Redis data first
set -euo pipefail
cd "$(dirname "$0")/.."

RESET=false
[ "${1:-}" = "--reset" ] && RESET=true

for tool in docker go pnpm; do
  command -v "$tool" >/dev/null || { echo "error: '$tool' is not installed or not on PATH" >&2; exit 1; }
done
docker info >/dev/null 2>&1 || { echo "error: the Docker daemon isn't running (start Docker Desktop)" >&2; exit 1; }

BIN=.dev/bin
LOGS=.dev/logs
mkdir -p "$BIN" "$LOGS"

PIDS=()

kill_tree() {
  local pid=$1 child
  for child in $(pgrep -P "$pid" 2>/dev/null || true); do kill_tree "$child"; done
  kill "$pid" 2>/dev/null || true
}

cleanup() {
  local code=$?
  trap - EXIT INT TERM
  echo
  echo "Stopping services and apps..."
  for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill_tree "$pid"; done
  wait 2>/dev/null || true
  echo "Done. Infra is still up; stop it with: docker compose down"
  exit "$code"
}
# Ctrl-C is a normal way to stop, so it exits 0; a startup failure keeps its own exit code.
trap cleanup EXIT
trap 'exit 0' INT TERM

start() { # start <name> <dir> <command...>
  local name=$1 dir=$2
  shift 2
  (cd "$dir" && exec "$@") >"$LOGS/$name.log" 2>&1 &
  PIDS+=($!)
}

wait_for() { # wait_for <description> <command...>
  local what=$1
  shift
  printf 'Waiting for %s' "$what"
  for _ in $(seq 1 60); do
    if "$@" >/dev/null 2>&1; then echo " ok"; return 0; fi
    printf '.'
    sleep 2
  done
  echo " timed out" >&2
  return 1
}

healthy() { [ "$(docker inspect -f '{{.State.Health.Status}}' "$1" 2>/dev/null)" = "healthy" ]; }
ksql_ready() { [ "$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' grab-mini-ksql-init 2>/dev/null)" = "exited 0" ]; }
topics_ready() { [ "$(docker inspect -f '{{.State.Status}} {{.State.ExitCode}}' grab-mini-kafka-init 2>/dev/null)" = "exited 0" ]; }

if $RESET; then
  echo "Resetting infra data..."
  docker compose down -v
fi

echo "Starting infra..."
docker compose up -d
wait_for "Kafka" healthy grab-mini-kafka
wait_for "Postgres" healthy grab-mini-postgres
wait_for "Redis" healthy grab-mini-redis
wait_for "Schema Registry" healthy grab-mini-schema-registry
wait_for "topics" topics_ready
wait_for "ksqlDB" healthy grab-mini-ksqldb
wait_for "analytics tables" ksql_ready
wait_for "Prometheus" healthy grab-mini-prometheus
wait_for "Loki" healthy grab-mini-loki
wait_for "Grafana" healthy grab-mini-grafana

echo "Building Go services..."
SERVICES="auth-service order-service matching-service trip-service driver-location-service notification-service rating-service payment-service location-gateway pricing-service driver-app-sim"
ROOT=$PWD
for svc in $SERVICES; do
  (cd "services/$svc" && go build -o "$ROOT/$BIN/$svc" .)
done

echo "Seeding drivers..."
"$BIN/matching-service" seed-drivers

# Emit JSON logs by default so Promtail/Loki can index level, service and trace_id.
export LOG_FORMAT="${LOG_FORMAT:-json}"

for svc in $SERVICES; do
  case "$svc" in
    matching-service)
      start "$svc" "services/$svc" env HEALTH_PORT=9101 "$ROOT/$BIN/$svc"
      ;;
    driver-location-service)
      start "$svc" "services/$svc" env HEALTH_PORT=9102 "$ROOT/$BIN/$svc"
      ;;
    *)
      start "$svc" "services/$svc" "$ROOT/$BIN/$svc"
      ;;
  esac
done
start rider apps/rider pnpm dev
start driver apps/driver pnpm dev
start ops apps/ops pnpm dev

wait_for "Auth Service" curl -fs localhost:3005/health
wait_for "Order Service" curl -fs localhost:3001/health
wait_for "Trip Service" curl -fs localhost:3002/health
wait_for "Notification Service" curl -fs localhost:3003/health
wait_for "Rating Service" curl -fs localhost:3004/health
wait_for "Payment Service" curl -fs localhost:3008/health
wait_for "Location Gateway" curl -fs localhost:3006/health
wait_for "Pricing Service" curl -fs localhost:3007/health
wait_for "Matching Service" curl -fs localhost:9101/readyz
wait_for "Driver Location Service" curl -fs localhost:9102/readyz
wait_for "Rider App" curl -fs localhost:5171
wait_for "Driver App" curl -fs localhost:5172
wait_for "Ops dashboard" curl -fs localhost:5173

cat <<MSG

grab-mini is up.

  Rider App    http://localhost:5171
  Driver App   http://localhost:5172/?driver=driver-1   (open as the driver the rider gets matched with)
  Ops          http://localhost:5173   (live analytics from ksqlDB)
  Grafana      http://localhost:3000   (dashboards for Prometheus metrics & Loki logs)
  Prometheus   http://localhost:9090
  Kafka UI     http://localhost:8080
  Schema Reg.  http://localhost:8081/subjects
  ksqlDB       http://localhost:8088/info

  Logs: $LOGS/<name>.log     Ctrl-C to stop.
MSG

wait


#!/usr/bin/env bash
# Starts everything needed to try grab-mini locally:
#   infra (Kafka, Kafka UI, Postgres, Redis) -> seeded drivers -> the Go
#   services -> the driver location simulator -> the Rider and Driver apps.
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
wait_for "topics" topics_ready

echo "Building Go services..."
SERVICES="order-service matching-service trip-service driver-location-service notification-service driver-app-sim"
ROOT=$PWD
for svc in $SERVICES; do
  (cd "services/$svc" && go build -o "$ROOT/$BIN/$svc" .)
done

echo "Seeding drivers..."
"$BIN/matching-service" seed-drivers

for svc in $SERVICES; do
  start "$svc" "services/$svc" "$ROOT/$BIN/$svc"
done
start rider apps/rider pnpm dev
start driver apps/driver pnpm dev

wait_for "Order Service" curl -fs localhost:3001/health
wait_for "Trip Service" curl -fs localhost:3002/health
wait_for "Notification Service" curl -fs localhost:3003/health
wait_for "Rider App" curl -fs localhost:5171
wait_for "Driver App" curl -fs localhost:5172

cat <<MSG

grab-mini is up.

  Rider App    http://localhost:5171
  Driver App   http://localhost:5172/?driver=driver-1   (open as the driver the rider gets matched with)
  Kafka UI     http://localhost:8080

  Logs: $LOGS/<name>.log     Ctrl-C to stop.
MSG

wait

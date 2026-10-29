#!/usr/bin/env bash
# Runs every service on this machine, without Docker, against infrastructure you
# already have: Kafka, Postgres and Redis. It starts dev-registry in place of
# Schema Registry. For load tests and for seeing the whole system work.
#
#   KAFKA_BROKERS=localhost:9092 REDIS_ADDR=localhost:6379 \
#   DATABASE_URL='postgres://user:pw@localhost:5432/grabmini_load' \
#   deploy/loadtest/stack.sh start        # ... and later: stop
#
# The database must exist and be empty or at least migratable; the services
# migrate it themselves. Logs go to $DIR/logs, binaries to $DIR/bin, process ids
# to $DIR/pids. Logging is at warn level: at load, a line per message would be
# the biggest cost of the whole run.
set -euo pipefail
cd "$(dirname "$0")/../.."
ROOT=$PWD
DIR=${DIR:-$ROOT/.dev/stack}
mkdir -p "$DIR/bin" "$DIR/logs"

: "${KAFKA_BROKERS:=localhost:9092}"
: "${REDIS_ADDR:=localhost:6379}"
: "${DATABASE_URL:?set DATABASE_URL (the database must exist)}"
export KAFKA_BROKERS REDIS_ADDR DATABASE_URL
export SCHEMA_REGISTRY_URL=http://localhost:8081
export LOG_LEVEL=${LOG_LEVEL:-warn}
# Tokens use the published development Ed25519 key, accepted only with APP_ENV=development.
export APP_ENV=${APP_ENV:-development}

# name  port-env  metrics-port-for-workers
SERVICES="dev-registry auth-service order-service matching-service trip-service driver-location-service notification-service rating-service location-gateway"

start() {
  [ -f "$DIR/pids" ] && { echo "already started ($DIR/pids); run stop first"; exit 1; }
  for s in $SERVICES; do
    (cd "services/$s" && go build -o "$DIR/bin/$s" .)
  done

  launch() { # name, then NAME=VALUE environment
    local name=$1; shift
    env "$@" nohup "$DIR/bin/$name" > "$DIR/logs/$name.log" 2>&1 &
    echo "$! $name" >> "$DIR/pids"
  }
  : > "$DIR/pids"
  launch dev-registry PORT=8081
  sleep 0.5
  launch auth-service PORT=3005
  launch order-service PORT=3001
  launch trip-service PORT=3002
  launch notification-service PORT=3003
  launch rating-service PORT=3004
  launch location-gateway PORT=3006
  launch matching-service HEALTH_PORT=9101 INSTANCE_ID=matching-1
  launch driver-location-service HEALTH_PORT=9102

  echo -n "waiting for the services"
  for url in localhost:8081/subjects localhost:3005/health localhost:3001/health localhost:3002/health \
             localhost:3003/health localhost:3004/health localhost:3006/health localhost:9101/readyz localhost:9102/readyz; do
    for _ in $(seq 1 60); do
      curl -fs "http://$url" >/dev/null 2>&1 && break
      echo -n "."; sleep 1
    done
    curl -fs "http://$url" >/dev/null 2>&1 || { echo; echo "$url never came up; see $DIR/logs"; exit 1; }
  done
  echo " up"
  echo "auth 3005  order 3001  trip 3002  notification 3003  rating 3004  gateway 3006  (matching metrics 9101, driver-location 9102)"
}

stop() {
  [ -f "$DIR/pids" ] || { echo "not started"; return; }
  while read -r pid name; do kill "$pid" 2>/dev/null || true; done < "$DIR/pids"
  sleep 2
  while read -r pid name; do kill -9 "$pid" 2>/dev/null || true; done < "$DIR/pids"
  rm -f "$DIR/pids"
  echo "stopped"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  *) echo "usage: $0 start|stop"; exit 2 ;;
esac

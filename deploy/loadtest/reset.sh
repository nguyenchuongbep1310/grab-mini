#!/usr/bin/env bash
# Puts the infrastructure back to empty for a clean load-test run: stops the
# stack, deletes the application's Kafka topics (and the dead-letter and retry
# topics derived from them), recreates the database, and flushes Redis.
#
#   KAFKA_BIN=/path/to/kafka/bin KAFKA_BROKERS=localhost:9092 REDIS_ADDR=localhost:6379 \
#   PSQL_ADMIN='psql -h /tmp -p 5433 -U postgres' DB=grabmini_load deploy/loadtest/reset.sh
#
# It deletes data. Point it at throwaway infrastructure only.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${KAFKA_BIN:?set KAFKA_BIN}" "${KAFKA_BROKERS:?}" "${REDIS_ADDR:?}" "${PSQL_ADMIN:?}" "${DB:?}"

deploy/loadtest/stack.sh stop >/dev/null 2>&1 || true

topics=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BROKERS" --list 2>/dev/null \
  | grep -E '^(order\.|trip\.|driver\.location)' || true)
for t in $topics; do
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BROKERS" --delete --topic "$t" >/dev/null 2>&1 || true
done
# the consumer groups too, so a new run starts from the new topics' beginning
for g in $("$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" --list 2>/dev/null | grep -E '^(matching|order|trip|driver-location|notification|rating)-|^dlq-redrive'); do
  "$KAFKA_BIN/kafka-consumer-groups.sh" --bootstrap-server "$KAFKA_BROKERS" --delete --group "$g" >/dev/null 2>&1 || true
done

# Create the topics before the services start, as the real deployment does
# (docker-compose's kafka-init, Strimzi's KafkaTopic): a consumer that starts
# before its topic exists is not assigned it when it appears later.
for t in order.created order.matched order.no_driver_found order.cancelled trip.status.changed trip.rated driver.location.updated; do
  "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$KAFKA_BROKERS" --create --if-not-exists --topic "$t" \
    --partitions "${PARTITIONS:-3}" --replication-factor 1 >/dev/null 2>&1
done

$PSQL_ADMIN -qc "drop database if exists $DB" -c "create database $DB" 2>&1 | grep -v NOTICE || true
host=${REDIS_ADDR%:*}; port=${REDIS_ADDR#*:}
redis-cli -h "$host" -p "$port" flushall >/dev/null
echo "reset: $(echo "$topics" | wc -w) topics deleted and the 6 application topics recreated (${PARTITIONS:-3} partitions), database $DB recreated, redis flushed"

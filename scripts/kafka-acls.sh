#!/usr/bin/env bash
# Creates a Kafka principal (SCRAM user) for each service and grants it only
# what that service does, nothing more. Safe to run again.
#
#   BOOTSTRAP=kafka.internal:9094 \
#   KAFKA_BIN=/opt/kafka/bin \
#   ADMIN_CONFIG=./admin.properties \
#   SECRETS_DIR=./secrets \
#   scripts/kafka-acls.sh
#
# ADMIN_CONFIG is a client properties file for a Kafka super user (security.protocol,
# sasl.mechanism, sasl.jaas.config, ssl.truststore.*). Each service's password is
# read from $SECRETS_DIR/<service>.password, and generated there if the file
# doesn't exist yet: mount that file into the service and point
# KAFKA_PASSWORD_FILE at it (see services/pkg/kafkaconn).
#
# What a service needs, and why (this table is the source of truth; the
# README and production-design.md refer to it):
#   - producing to a topic:  Write + Describe on it
#   - consuming a topic:     Read + Describe on it, and Read on its consumer
#     groups. The retry-tier groups are named "<group>.retry.<n>", so the group
#     grant is a prefix.
#   - a consumer's failure path (pkg/kafkaconsumer): Read, Write, Describe and
#     Create on "<topic>." as a prefix, which is exactly its retry topics
#     ("<topic>.retry.<n>") and dead-letter topic ("<topic>.DLQ") and nothing else.
set -euo pipefail

BOOTSTRAP=${BOOTSTRAP:?set BOOTSTRAP, e.g. localhost:9094}
KAFKA_BIN=${KAFKA_BIN:?set KAFKA_BIN to the directory holding kafka-acls.sh}
ADMIN_CONFIG=${ADMIN_CONFIG:?set ADMIN_CONFIG to the super user client properties file}
SECRETS_DIR=${SECRETS_DIR:-./secrets}

# service -> topics it produces to
declare -A PRODUCES=(
  [order-service]="order.created order.cancelled"
  [matching-service]="order.matched order.no_driver_found"
  [trip-service]="trip.status.changed trip.rated"
  [location-gateway]="driver.location.updated"
)

# service -> "group=topic,topic;group=topic" it consumes
declare -A CONSUMES=(
  [order-service]="order-service-group=order.matched,order.no_driver_found,trip.status.changed"
  [matching-service]="matching-service-group=order.created;matching-service-cancel-group=order.cancelled"
  [trip-service]="trip-service-group=order.matched;trip-driver-release-group=trip.status.changed;trip-order-cancelled-group=order.cancelled"
  [driver-location-service]="driver-location-service-group=driver.location.updated"
  [rating-service]="rating-service-group=trip.rated"
  [notification-service]="notification-service-group=order.created,order.matched,order.no_driver_found,order.cancelled,trip.status.changed,driver.location.updated,trip.rated"
)

acls()  { "$KAFKA_BIN/kafka-acls.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" "$@" >/dev/null; }
users() { "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" --command-config "$ADMIN_CONFIG" "$@" >/dev/null; }

mkdir -p "$SECRETS_DIR"
chmod 700 "$SECRETS_DIR"

services=$(printf '%s\n' "${!PRODUCES[@]}" "${!CONSUMES[@]}" | sort -u)
for svc in $services; do
  pw_file="$SECRETS_DIR/$svc.password"
  if [ ! -s "$pw_file" ]; then
    (umask 077; openssl rand -base64 33 | tr -d '\n' > "$pw_file")
    echo "generated $pw_file"
  fi
  users --alter --entity-type users --entity-name "$svc" \
        --add-config "SCRAM-SHA-512=[password=$(cat "$pw_file")]"

  principal="User:$svc"

  for topic in ${PRODUCES[$svc]:-}; do
    acls --add --allow-principal "$principal" --operation Write --operation Describe --topic "$topic"
  done

  IFS=';' read -ra groups <<< "${CONSUMES[$svc]:-}"
  for entry in "${groups[@]}"; do
    [ -n "$entry" ] || continue
    group=${entry%%=*}
    acls --add --allow-principal "$principal" --operation Read --group "$group" --resource-pattern-type prefixed
    IFS=',' read -ra topics <<< "${entry#*=}"
    for topic in "${topics[@]}"; do
      acls --add --allow-principal "$principal" --operation Read --operation Describe --topic "$topic"
      acls --add --allow-principal "$principal" --operation Read --operation Write --operation Describe --operation Create \
           --topic "$topic." --resource-pattern-type prefixed
    done
  done
  echo "configured $svc"
done

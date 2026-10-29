#!/bin/sh
# Deletes finished orders / trips older than a configurable age, in small
# batches. POSIX sh, needs only psql: the retention CronJobs run this very file
# (deploy/k8s/generate.py puts it in a ConfigMap), and scripts/test-data-ops.sh
# proves it on a real Postgres.
#
#   pg-retention.sh orders     # run against order-service's database
#   pg-retention.sh trips      # run against trip-service's database
#
# Environment:
#   DATABASE_URL or DATABASE_URL_FILE   the service's Postgres URL (required)
#   RETENTION_DAYS        age of a finished row before it goes (default 180, minimum 30)
#   MATCHED_ORDER_DAYS    orders only: age of a MATCHED order before it goes (default 365).
#                         order-service never learns that the trip finished, so MATCHED
#                         is the final state of every completed ride; it is deleted only
#                         when no trip can still be running. Must be >= RETENTION_DAYS.
#   BATCH_SIZE            rows per DELETE (default 5000)
#   BATCH_PAUSE           seconds between batches (default 0.2)
#   DRY_RUN=1             only count what would go
#
# What is never touched, whatever its age:
#   orders  PENDING (matching has not answered yet)
#   trips   ASSIGNED and IN_PROGRESS
#   outbox  (deleted by the relay when published) and outbox_dead (for a person to read)
#   driver_reviews in rating-service (the review history: see its migration)
#
# Ordering between tables: there are no foreign keys here (the services refer
# to each other by id in a TEXT column and each owns its database), so a delete
# can never be refused or cascade. The one rule is time: every threshold is at
# least 30 days, longer than Kafka keeps events (7 days) and than the dedupe
# tables keep their markers (30 days), so no redelivered event can find a
# marker gone while the row it guards is still wanted, or the reverse.
set -eu

table=${1:-}
case "$table" in orders | trips) ;; *) echo "usage: $0 orders|trips" >&2; exit 2 ;; esac

url=${DATABASE_URL:-}
if [ -z "$url" ]; then
  url=$(cat "${DATABASE_URL_FILE:?set DATABASE_URL or DATABASE_URL_FILE}")
fi
days=${RETENTION_DAYS:-180}
matched_days=${MATCHED_ORDER_DAYS:-365}
batch=${BATCH_SIZE:-5000}
pause=${BATCH_PAUSE:-0.2}
dry=${DRY_RUN:-0}

for v in "$days" "$matched_days" "$batch"; do
  case "$v" in '' | *[!0-9]*) echo "RETENTION_DAYS, MATCHED_ORDER_DAYS and BATCH_SIZE must be whole numbers" >&2; exit 2 ;; esac
done
# 30 days is the floor of the dedupe tables (DEDUPE_RETENTION_DAYS in deploy/k8s/generate.py).
if [ "$days" -lt 30 ] || [ "$batch" -lt 1 ] || [ "$matched_days" -lt "$days" ]; then
  echo "refusing: need RETENTION_DAYS >= 30, MATCHED_ORDER_DAYS >= RETENTION_DAYS, BATCH_SIZE >= 1" >&2
  exit 2
fi

# sql_X prints the statement that deletes one batch and returns how many rows
# went; count_X what would go. SKIP LOCKED: a row some other transaction
# holds is left for the next run.
sql_orders() {
  cat <<'SQL'
WITH d AS (
  DELETE FROM orders WHERE id IN (
    SELECT id FROM orders
    WHERE (status IN ('CANCELLED', 'NO_DRIVER') AND created_at < now() - make_interval(days => :days))
       OR (status = 'MATCHED'                   AND created_at < now() - make_interval(days => :matched_days))
    ORDER BY created_at
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
  ) RETURNING 1
) SELECT count(*) FROM d
SQL
}
count_orders() {
  cat <<'SQL'
SELECT count(*) FROM orders
WHERE (status IN ('CANCELLED', 'NO_DRIVER') AND created_at < now() - make_interval(days => :days))
   OR (status = 'MATCHED'                   AND created_at < now() - make_interval(days => :matched_days))
SQL
}
sql_trips() {
  cat <<'SQL'
WITH d AS (
  DELETE FROM trips WHERE id IN (
    SELECT id FROM trips
    WHERE status IN ('COMPLETED', 'CANCELLED')
      AND coalesce(completed_at, cancelled_at, created_at) < now() - make_interval(days => :days)
    ORDER BY created_at
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
  ) RETURNING 1
) SELECT count(*) FROM d
SQL
}
count_trips() {
  cat <<'SQL'
SELECT count(*) FROM trips
WHERE status IN ('COMPLETED', 'CANCELLED')
  AND coalesce(completed_at, cancelled_at, created_at) < now() - make_interval(days => :days)
SQL
}
# The "this order was cancelled" markers only matter while an order.matched for
# that order can still arrive (Kafka keeps events 7 days), far inside RETENTION_DAYS.
sql_cancelled_orders() {
  cat <<'SQL'
WITH d AS (
  DELETE FROM cancelled_orders WHERE order_id IN (
    SELECT order_id FROM cancelled_orders
    WHERE cancelled_at < now() - make_interval(days => :days)
    LIMIT :batch
    FOR UPDATE SKIP LOCKED
  ) RETURNING 1
) SELECT count(*) FROM d
SQL
}
count_cancelled_orders() {
  cat <<'SQL'
SELECT count(*) FROM cancelled_orders WHERE cancelled_at < now() - make_interval(days => :days)
SQL
}

psql_run() { # stdin: SQL
  psql "$url" -v ON_ERROR_STOP=1 -At -q \
    -v days="$days" -v matched_days="$matched_days" -v batch="$batch" -f -
}

purge() { # $1 = name of a sql_/count_ pair, which is also the table
  if [ "$dry" = 1 ]; then
    n=$("count_$1" | psql_run)
    echo "dry run: $n rows of $1 would be deleted"
    return
  fi
  total=0
  while :; do
    n=$("sql_$1" | psql_run)
    total=$((total + n))
    [ "$n" -eq 0 ] && break
    sleep "$pause" 2>/dev/null || true
  done
  echo "deleted $total rows from $1"
}

case "$table" in
  orders) purge orders ;;
  trips)
    purge trips
    purge cancelled_orders
    ;;
esac

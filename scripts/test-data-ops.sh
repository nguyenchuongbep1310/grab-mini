#!/usr/bin/env bash
# Proves the data-ops scripts on a real Postgres, end to end:
#
#   scripts/test-data-ops.sh            # retention + backup + restore drill (directory target)
#   WITH_S3=1 scripts/test-data-ops.sh   # also the S3 path the CronJobs use, against SeaweedFS as an S3 server
#
# Needs Docker and Go. It starts throwaway containers named ag-ops-* (Postgres
# on host port 35432, a second Postgres as the scratch server, optionally
# SeaweedFS as S3), applies the services' real migrations with `<service> migrate`,
# seeds rows of every status and age, runs scripts/pg-retention.sh,
# scripts/pg-backup.sh and scripts/pg-restore-drill.sh, asserts what is left,
# and removes everything it created on exit (also on failure).
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD

PG_IMAGE=${PG_IMAGE:-postgres:16-alpine}
AWS_IMAGE=${AWS_IMAGE:-amazon/aws-cli:2.17.40}
NET=ag-ops-net
PG=ag-ops-pg
SCRATCH=ag-ops-scratch
S3=ag-ops-s3
VOL=ag-ops-backup
PORT=35432
PASS=ag-ops-pass

cleanup() {
  docker rm -f "$PG" "$SCRATCH" "$S3" > /dev/null 2>&1 || true
  docker volume rm -f "$VOL" ag-ops-work > /dev/null 2>&1 || true
  docker network rm "$NET" > /dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup

pass=0
ok() { pass=$((pass + 1)); printf '  ok   %s\n' "$*"; }
die() { printf '  FAIL %s\n' "$*" >&2; exit 1; }
step() { printf '\n== %s\n' "$*"; }
# expect DESCRIPTION DB "SQL" EXPECTED
expect() {
  local got
  got=$(docker exec "$PG" psql -U postgres -d "$2" -qAt -c "$3")
  [ "$got" = "$4" ] && ok "$1 ($got)" || die "$1: got '$got', want '$4'"
}
# said DESCRIPTION PATTERN CMD...: run CMD, show its output, require PATTERN in it.
said() {
  local what=$1 pattern=$2 out
  shift 2
  out=$("$@" 2>&1) || { echo "$out" >&2; die "$what: command failed"; }
  echo "$out" | sed 's/^/       /'
  echo "$out" | grep -q -- "$pattern" && ok "$what" || die "$what: output lacks '$pattern'"
}
# in_pg_image CMD...: run in the Postgres image on the test network with the scripts mounted.
in_pg_image() {
  docker run --rm --network "$NET" -v "$ROOT:/repo:ro" -v "$VOL:/backup" "$@"
}

step "start Postgres ($PG on host port $PORT) and a second server ($SCRATCH) as the restore target"
docker network create "$NET" > /dev/null
docker volume create "$VOL" > /dev/null
docker run -d --name "$PG" --network "$NET" -p "$PORT:5432" -e POSTGRES_PASSWORD="$PASS" "$PG_IMAGE" > /dev/null
docker run -d --name "$SCRATCH" --network "$NET" -e POSTGRES_PASSWORD="$PASS" "$PG_IMAGE" > /dev/null
for c in "$PG" "$SCRATCH"; do
  for _ in $(seq 60); do docker exec "$c" pg_isready -U postgres -q && break; sleep 1; done
  # The entrypoint restarts the server once after initialising; wait for the final one.
  sleep 2
  docker exec "$c" pg_isready -U postgres -q || die "$c did not become ready"
done
for db in grab_orders grab_trips grab_ratings; do
  docker exec "$PG" psql -U postgres -qAt -c "CREATE DATABASE $db" > /dev/null
done

step "apply the real migrations with '<service> migrate'"
for pair in order-service:grab_orders trip-service:grab_trips rating-service:grab_ratings; do
  svc=${pair%%:*}
  (cd "services/$svc" && DATABASE_URL="postgres://postgres:$PASS@localhost:$PORT/${pair##*:}?sslmode=disable" go run . migrate 2>&1 | tail -n 2)
done
expect "order-service at its newest migration" grab_orders "SELECT max(version_id) FROM schema_migrations_order_service" "$(ls services/order-service/migrations | wc -l | tr -d ' ')"
expect "trip-service at its newest migration" grab_trips "SELECT max(version_id) FROM schema_migrations_trip_service" "$(ls services/trip-service/migrations | wc -l | tr -d ' ')"

step "seed orders and trips of every status and age"
docker exec -i "$PG" psql -U postgres -d grab_orders -q -v ON_ERROR_STOP=1 <<'SQL'
-- label is kept in rider_id so the assertions can name what survived
INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status, created_at)
SELECT gen_random_uuid(), r, 0, 0, 0, 0, s, now() - make_interval(days => d)
FROM (VALUES
  ('pending-old',        'PENDING',   400),   -- keep: not terminal, however old
  ('pending-new',        'PENDING',     1),
  ('cancelled-old',      'CANCELLED', 200),   -- delete
  ('cancelled-new',      'CANCELLED', 100),
  ('nodriver-old',       'NO_DRIVER', 200),   -- delete
  ('nodriver-new',       'NO_DRIVER',  10),
  ('matched-mid',        'MATCHED',   200),   -- keep: a trip could not still be running, but the longer MATCHED threshold applies
  ('matched-ancient',    'MATCHED',   400),   -- delete
  ('matched-new',        'MATCHED',    10)
) AS t(r, s, d);
-- 12000 old cancelled orders: three batches of 5000
INSERT INTO orders (id, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng, status, created_at)
SELECT gen_random_uuid(), 'bulk', 0, 0, 0, 0, 'CANCELLED', now() - interval '300 days' - (g || ' seconds')::interval FROM generate_series(1, 12000) g;
INSERT INTO outbox (source, topic, payload, created_at) VALUES ('order-service', 'order.created', 'x', now() - interval '400 days');
INSERT INTO outbox_dead (source, topic, payload, created_at, error) VALUES ('order-service', 'order.created', 'x', now() - interval '400 days', 'too large');
SQL
docker exec -i "$PG" psql -U postgres -d grab_trips -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO trips (id, order_id, driver_id, rider_id, status, created_at, completed_at, cancelled_at, rating)
SELECT gen_random_uuid(), 'order-' || o, 'd1', o, s, now() - make_interval(days => cr), now() - make_interval(days => co), now() - make_interval(days => ca), rt
FROM (VALUES
  ('completed-old',      'COMPLETED',  201, 200, NULL::int, NULL::int),   -- delete
  ('completed-rated-old','COMPLETED',  201, 200, NULL,      5),           -- delete (the rating lives on in rating-service)
  ('completed-new',      'COMPLETED',  101, 100, NULL,      NULL),        -- keep
  ('cancelled-old',      'CANCELLED',  201, NULL, 200,      NULL),        -- delete
  ('cancelled-new',      'CANCELLED',   11, NULL,  10,      NULL),        -- keep
  ('assigned-ancient',   'ASSIGNED',   400, NULL, NULL,     NULL),        -- keep: in flight
  ('inprogress-ancient', 'IN_PROGRESS',400, NULL, NULL,     NULL),        -- keep: in flight
  ('completed-no-ts',    'COMPLETED',  300, NULL, NULL,     NULL)         -- delete: falls back to created_at
) AS t(o, s, cr, co, ca, rt);
INSERT INTO cancelled_orders (order_id, cancelled_at) VALUES ('old', now() - interval '200 days'), ('new', now() - interval '3 days');
INSERT INTO processed_events (event_id, processed_at) VALUES (gen_random_uuid(), now() - interval '400 days');
INSERT INTO outbox (source, topic, payload, created_at) VALUES ('trip-service', 'trip.status.changed', 'x', now() - interval '400 days');
INSERT INTO outbox_dead (source, topic, payload, created_at, error) VALUES ('trip-service', 'trip.rated', 'x', now() - interval '400 days', 'too large');
SQL
docker exec -i "$PG" psql -U postgres -d grab_ratings -q -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO driver_ratings (driver_id, rating_count, rating_sum) VALUES ('d1', 1, 5);
INSERT INTO driver_reviews (event_id, trip_id, driver_id, rider_id, stars) VALUES (gen_random_uuid(), 't1', 'd1', 'r1', 5);
INSERT INTO rating_events (event_id) VALUES (gen_random_uuid());
SQL
expect "orders seeded" grab_orders "SELECT count(*) FROM orders" 12009
expect "trips seeded" grab_trips "SELECT count(*) FROM trips" 8

retention() { # SERVICE-DB TABLE [ENV=VALUE...]
  local db=$1 table=$2
  shift 2
  in_pg_image -e "DATABASE_URL=postgres://postgres:$PASS@$PG:5432/$db" -e BATCH_PAUSE=0 "$@" "$PG_IMAGE" sh /repo/scripts/pg-retention.sh "$table"
}

step "retention: refuses unsafe settings"
if retention grab_orders orders -e RETENTION_DAYS=7 2> /dev/null; then die "accepted RETENTION_DAYS=7"; fi
ok "RETENTION_DAYS=7 refused"
if retention grab_orders orders -e MATCHED_ORDER_DAYS=100 2> /dev/null; then die "accepted MATCHED_ORDER_DAYS < RETENTION_DAYS"; fi
ok "MATCHED_ORDER_DAYS below RETENTION_DAYS refused"
expect "nothing was deleted by the refused runs" grab_orders "SELECT count(*) FROM orders" 12009

step "retention: dry run counts and deletes nothing"
said "dry run counts 12003 (12000 bulk + cancelled-old + nodriver-old + matched-ancient)" "12003 rows of orders would be deleted" retention grab_orders orders -e DRY_RUN=1
expect "dry run deleted nothing" grab_orders "SELECT count(*) FROM orders" 12009

step "retention: orders (3 batches of 5000), default 180 days"
said "deleted 12003" "deleted 12003 rows from orders" retention grab_orders orders -e BATCH_SIZE=5000
expect "orders left" grab_orders "SELECT count(*) FROM orders" 6
expect "every PENDING order survived" grab_orders "SELECT count(*) FROM orders WHERE status='PENDING'" 2
expect "survivors are exactly the young terminal ones, mid MATCHED and PENDING" grab_orders \
  "SELECT string_agg(rider_id, ',' ORDER BY rider_id) FROM orders" \
  "cancelled-new,matched-mid,matched-new,nodriver-new,pending-new,pending-old"
expect "outbox untouched" grab_orders "SELECT count(*) FROM outbox" 1
expect "outbox_dead untouched" grab_orders "SELECT count(*) FROM outbox_dead" 1
said "a second run deletes nothing (idempotent)" "deleted 0 rows from orders" retention grab_orders orders

step "retention: trips and cancelled_orders"
said "deleted 4 trips" "deleted 4 rows from trips" retention grab_trips trips
expect "trips left" grab_trips "SELECT string_agg(rider_id, ',' ORDER BY rider_id) FROM trips" "assigned-ancient,cancelled-new,completed-new,inprogress-ancient"
expect "in-flight trips survived" grab_trips "SELECT count(*) FROM trips WHERE status IN ('ASSIGNED','IN_PROGRESS')" 2
expect "old cancelled_orders marker deleted, new kept" grab_trips "SELECT string_agg(order_id, ',') FROM cancelled_orders" new
expect "processed_events not touched here (its own CronJob)" grab_trips "SELECT count(*) FROM processed_events" 1
expect "trip outbox + outbox_dead untouched" grab_trips "SELECT (SELECT count(*) FROM outbox) || '/' || (SELECT count(*) FROM outbox_dead)" "1/1"
expect "rating-service review history untouched" grab_ratings "SELECT count(*) FROM driver_reviews" 1

step "backup: pg-backup.sh all -> directory target, twice, so 'latest' means something"
backup() { # SERVICE DB
  in_pg_image -e "DATABASE_URL=postgres://postgres:$PASS@$PG:5432/$2" -e "BACKUP_NAME=$1" -e BACKUP_TARGET=/backup/store -e BACKUP_DIR=/tmp/work "$PG_IMAGE" sh /repo/scripts/pg-backup.sh all
}
# A first dump, then more rows go, then a second dump. The drill must restore the second.
for pair in order-service:grab_orders trip-service:grab_trips rating-service:grab_ratings; do backup "${pair%%:*}" "${pair##*:}" | tail -n 1; done
sleep 1.2
docker exec "$PG" psql -U postgres -d grab_orders -qAt -c "DELETE FROM orders WHERE rider_id = 'cancelled-new'" > /dev/null
for pair in order-service:grab_orders trip-service:grab_trips rating-service:grab_ratings; do backup "${pair%%:*}" "${pair##*:}" | tail -n 1; done
n=$(docker run --rm -v "$VOL:/backup" "$PG_IMAGE" sh -c 'ls /backup/store/order-service | wc -l')
[ "$n" = 2 ] && ok "two order-service dumps in the store" || die "expected 2 dumps, found $n"

drill() { # SERVICE [ENV=VALUE...]
  local svc=$1
  shift
  in_pg_image -e "BACKUP_NAME=$svc" -e BACKUP_TARGET=/backup/store \
    -e "SCRATCH_URL=postgres://postgres:$PASS@$SCRATCH:5432/postgres" \
    -e "SOURCE_URL=postgres://postgres:$PASS@$PG:5432/grab_${svc%-service}s" "$@" "$PG_IMAGE" sh /repo/scripts/pg-restore-drill.sh
}
step "restore drill: latest dump of each service into the scratch server"
said "order-service drill passed and restored the SECOND dump (5 orders, not 6)" "orders .* 5 rows (live: 5)" drill order-service
said "trip-service drill passed" "DRILL OK" drill trip-service
said "rating-service drill passed" "DRILL OK" drill rating-service
n=$(docker exec "$SCRATCH" psql -U postgres -qAt -c "SELECT count(*) FROM pg_database WHERE datname LIKE 'restore_drill_%'")
[ "$n" = 0 ] && ok "scratch databases dropped" || die "$n scratch databases left behind"

step "restore drill: it must FAIL when something is wrong"
if drill order-service -e EXPECTED_SCHEMA_VERSION=99 > /dev/null 2>&1; then die "drill passed with a wrong schema version"; fi
ok "wrong schema version fails the drill"
if drill order-service -e MIN_ROWS=100000 > /dev/null 2>&1; then die "drill passed with too few rows"; fi
ok "too few rows fails the drill"
if drill order-service -e BACKUP_TARGET=/backup/nowhere > /dev/null 2>&1; then die "drill passed with no backups"; fi
ok "no backups fails the drill"
n=$(docker exec "$SCRATCH" psql -U postgres -qAt -c "SELECT count(*) FROM pg_database WHERE datname LIKE 'restore_drill_%'")
[ "$n" = 0 ] && ok "failed drills dropped their scratch databases too" || die "$n scratch databases left behind after failures"

if [ "${WITH_S3:-0}" = 1 ]; then
  step "S3 path as the CronJob runs it: dump (postgres image), upload (aws-cli image), drill (postgres image + aws-cli)"
  docker run -d --name "$S3" --network "$NET" chrislusf/seaweedfs:latest server -s3 -dir=/data > /dev/null
  sleep 12
  s3env=(-e AWS_ACCESS_KEY_ID=ag-ops -e AWS_SECRET_ACCESS_KEY=ag-ops -e AWS_DEFAULT_REGION=us-east-1 -e "BACKUP_S3_ENDPOINT=http://$S3:8333")
  docker run --rm --network "$NET" "${s3env[@]}" "$AWS_IMAGE" --endpoint-url "http://$S3:8333" s3 mb s3://ag-ops-backups > /dev/null
  common=(-e BACKUP_NAME=order-service -e BACKUP_TARGET=s3://ag-ops-backups/postgres -e BACKUP_DIR=/work)
  docker run --rm --network "$NET" -v "$ROOT:/repo:ro" -v ag-ops-work:/work -e "DATABASE_URL=postgres://postgres:$PASS@$PG:5432/grab_orders" "${common[@]}" "$PG_IMAGE" sh /repo/scripts/pg-backup.sh dump
  docker run --rm --network "$NET" -v "$ROOT:/repo:ro" -v ag-ops-work:/work "${s3env[@]}" "${common[@]}" --entrypoint sh "$AWS_IMAGE" /repo/scripts/pg-backup.sh upload
  docker volume rm -f ag-ops-work > /dev/null
  docker run --rm --network "$NET" -v "$ROOT:/repo:ro" "${s3env[@]}" "${common[@]}" \
    -e "SCRATCH_URL=postgres://postgres:$PASS@$SCRATCH:5432/postgres" "$PG_IMAGE" \
    sh -c 'apk add -q --no-cache aws-cli >/dev/null && sh /repo/scripts/pg-restore-drill.sh' && ok "S3 drill passed"
fi

printf '\nall %d checks passed\n' "$pass"

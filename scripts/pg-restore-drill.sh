#!/bin/sh
# Restore drill: restores the LATEST backup of one service into a scratch
# database and checks that it is a database the service could run on. A backup
# that has never been restored is not a backup: run this every quarter, and
# after any change to the backup job.
#
#   BACKUP_TARGET=s3://bucket/prefix BACKUP_NAME=order-service \
#   SCRATCH_URL=postgres://admin:...@scratch-host:5432/postgres \
#   scripts/pg-restore-drill.sh
#
# Needs psql and pg_restore (version >= the dump's server), plus the AWS CLI
# for an s3:// target. NEVER point SCRATCH_URL at production: the script creates
# a database named restore_drill_<service>_<pid> there and drops it at the end,
# and refuses to touch any other name.
#
# Environment:
#   BACKUP_NAME             order-service | trip-service | rating-service (required)
#   BACKUP_TARGET           where pg-backup.sh put the dumps (required)
#   BACKUP_S3_ENDPOINT      optional, as in pg-backup.sh
#   SCRATCH_URL             a superuser/CREATEDB URL of a scratch server (required)
#   EXPECTED_SCHEMA_VERSION the newest migration number the code has; default: read
#                           from services/<name>/migrations when run in the repo
#   MIN_ROWS                rows the main table must have (default 1; 0 for a brand new system)
#   SOURCE_URL              optional: the live database, to print its counts next to the restored
#   KEEP_SCRATCH=1          keep the restored database for inspection
#
# Checks (any failure exits non-zero):
#   1. the newest dump can be fetched and listed
#   2. pg_restore completes without error
#   3. the service's goose version table is at EXPECTED_SCHEMA_VERSION, nothing half-applied
#   4. every table the service owns exists; the main table has MIN_ROWS rows
# And prints row counts, plus how long fetch+restore took: the only RTO number
# this repository can measure (see docs/disaster-recovery.md for how to read it).
set -eu

name=${BACKUP_NAME:?set BACKUP_NAME}
target=${BACKUP_TARGET:?set BACKUP_TARGET}
scratch=${SCRATCH_URL:?set SCRATCH_URL}
min_rows=${MIN_ROWS:-1}
here=$(cd "$(dirname "$0")" && pwd)

case "$name" in
  order-service) tables="orders outbox outbox_dead"; main=orders ;;
  trip-service) tables="trips processed_events cancelled_orders outbox outbox_dead"; main=trips ;;
  rating-service) tables="driver_ratings rating_events driver_reviews"; main=driver_ratings ;;
  *) echo "BACKUP_NAME must be order-service, trip-service or rating-service" >&2; exit 2 ;;
esac
version_table="schema_migrations_$(echo "$name" | tr - _)"

expected=${EXPECTED_SCHEMA_VERSION:-}
if [ -z "$expected" ] && [ -d "$here/../services/$name/migrations" ]; then
  expected=$(ls -1 "$here/../services/$name/migrations" | sed -n 's/^0*\([0-9][0-9]*\)_.*\.sql$/\1/p' | sort -n | tail -n 1)
fi
[ -n "$expected" ] || { echo "set EXPECTED_SCHEMA_VERSION (the repo's migrations are not here)" >&2; exit 2; }

work=$(mktemp -d)
db="restore_drill_$(echo "$name" | tr - _)_$$"
cleanup() {
  rm -rf "$work"
  if [ "${KEEP_SCRATCH:-0}" != 1 ]; then
    psql "$scratch" -v ON_ERROR_STOP=1 -qAt -c "DROP DATABASE IF EXISTS $db" > /dev/null 2>&1 || true
  else
    echo "kept scratch database $db"
  fi
}
trap cleanup EXIT

# The same URL with another database name: replace the path component.
url_for() { # $1 = database
  printf '%s' "$scratch" | sed -E "s#^([a-z]+://[^/]+)/[^?]*#\\1/$1#"
}
scratch_db_url=$(url_for "$db")

fail() { echo "DRILL FAILED: $*" >&2; exit 1; }
start=$(date +%s)

# 1. fetch the newest dump (names carry a UTC timestamp, so the last sort is the newest)
case "$target" in
  s3://*)
    ep=
    [ -n "${BACKUP_S3_ENDPOINT:-}" ] && ep="--endpoint-url $BACKUP_S3_ENDPOINT"
    # shellcheck disable=SC2086
    latest=$(aws $ep s3 ls "${target%/}/$name/" | awk '{print $4}' | grep -E "^$name-.*\.dump$" | sort | tail -n 1 || true)
    [ -n "$latest" ] || fail "no dump of $name under ${target%/}/$name/"
    # shellcheck disable=SC2086
    aws $ep s3 cp --only-show-errors "${target%/}/$name/$latest" "$work/$latest"
    ;;
  *)
    latest=$(ls -1 "$target/$name" 2> /dev/null | grep -E "^$name-.*\.dump$" | sort | tail -n 1 || true)
    [ -n "$latest" ] || fail "no dump of $name under $target/$name/"
    cp "$target/$name/$latest" "$work/$latest"
    ;;
esac
echo "latest backup: $latest ($(wc -c < "$work/$latest") bytes)"
pg_restore --list "$work/$latest" > /dev/null || fail "$latest is not a readable archive"

# 2. restore into a database made for the purpose
psql "$scratch" -v ON_ERROR_STOP=1 -qAt -c "CREATE DATABASE $db" > /dev/null || fail "cannot create $db on the scratch server"
pg_restore --no-owner --no-acl --exit-on-error --dbname="$scratch_db_url" "$work/$latest" || fail "pg_restore reported errors"
end=$(date +%s)

q() { psql "$scratch_db_url" -v ON_ERROR_STOP=1 -qAt -c "$1"; }

# 3. schema version: the newest applied migration must be the code's newest, and none half-applied
got=$(q "SELECT coalesce(max(version_id), 0) FROM $version_table WHERE is_applied") || fail "no $version_table: not a $name database"
[ "$got" = "$expected" ] || fail "schema version in the backup is $got, the code expects $expected (a backup older than a migration needs 'migrate' run after the restore)"
pending=$(q "SELECT count(*) FROM $version_table WHERE NOT is_applied")
[ "$pending" = 0 ] || fail "$pending migrations recorded but not applied"
echo "schema version: $got (expected $expected)"

# 4. tables and rows
for t in $tables; do
  n=$(q "SELECT count(*) FROM $t") || fail "table $t is missing from the restore"
  src=
  if [ -n "${SOURCE_URL:-}" ]; then
    src=" (live: $(psql "$SOURCE_URL" -qAt -c "SELECT count(*) FROM $t"))"
  fi
  printf '  %-18s %s rows%s\n' "$t" "$n" "$src"
  if [ "$t" = "$main" ] && [ "$n" -lt "$min_rows" ]; then
    fail "$t has $n rows, expected at least $min_rows"
  fi
done

echo "DRILL OK: $name restored from $latest in $((end - start)) s (fetch + pg_restore, total database size $(q "SELECT pg_size_pretty(pg_database_size(current_database()))"))"

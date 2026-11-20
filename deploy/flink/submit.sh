#!/bin/sh
# Submits the Flink SQL demo jobs to the session cluster; run by the one-shot
# flink-sql-init service (docker-compose.yml, profile "flink").
#
#   submit.sh <sql-file>...
#
# A file without `SET 'pipeline.name' = '...'` holds shared definitions
# (01-sources.sql) and is put in front of every later job file, because each
# SQL client run is a new session. A file with a pipeline name is one job: it
# is submitted unless the JobManager already has a job of that name that has
# not ended (RUNNING, or RESTARTING after a TaskManager loss, ...). INSERT INTO
# has no IF NOT EXISTS, so this check is what keeps a second `make flink-up`
# from doubling every count.
#
# The SQL client takes the cluster address and the job settings (checkpoints,
# restart strategy) from this container's Flink configuration (FLINK_PROPERTIES).
# Exits non-zero, with the client's output, when a statement fails or the job
# does not reach RUNNING.
set -eu

REST="${FLINK_REST_URL:-http://flink-jobmanager:8081}"
SQL_CLIENT="${FLINK_HOME:-/opt/flink}/bin/sql-client.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
: >"$WORK/shared.sql"

[ "$#" -gt 0 ] || { echo "usage: submit.sh <sql-file>..." >&2; exit 2; }

log() { echo "flink-sql-init: $*"; }
fail() { echo "flink-sql-init: $*" >&2; exit 1; }

# pipeline_name <file>: the value of SET 'pipeline.name', or nothing.
pipeline_name() {
  sed -n "s/^[[:space:]]*[Ss][Ee][Tt][[:space:]]*'pipeline\.name'[[:space:]]*=[[:space:]]*'\([^']*\)'.*/\1/p" "$1" | head -n 1
}

# job_state <name>: the state of a job of that name that has not ended, or
# nothing. /jobs/overview is a flat list of objects; splitting it at '{' puts
# each job's "name" and "state" on one line (its "tasks" object comes after).
# An unreadable overview is an error, never "no such job": guessing wrong here
# would submit a duplicate.
job_state() {
  overview="$(curl -fsS "$REST/jobs/overview")" || fail "cannot read $REST/jobs/overview"
  printf '%s\n' "$overview" | tr '{' '\n' \
    | grep -F "\"name\":\"$1\"" \
    | sed -n 's/.*"state":"\([A-Z]*\)".*/\1/p' \
    | grep -v -x -E 'FINISHED|CANCELED|FAILED' | head -n 1 || true
}

log "waiting for the JobManager at $REST"
i=0
until curl -fsS "$REST/overview" >/dev/null 2>&1; do
  i=$((i + 1))
  [ "$i" -le 60 ] || fail "the JobManager REST API at $REST did not answer within 120 s"
  sleep 2
done

for file in "$@"; do
  [ -r "$file" ] || fail "cannot read $file"
  name="$(pipeline_name "$file")"
  if [ -z "$name" ]; then
    log "definitions from $file"
    cat "$file" >>"$WORK/shared.sql"
    printf '\n' >>"$WORK/shared.sql"
    continue
  fi

  state="$(job_state "$name")"
  if [ -n "$state" ]; then
    log "skipping $file: job '$name' is already $state"
    continue
  fi

  log "submitting $file as job '$name'"
  cat "$WORK/shared.sql" "$file" >"$WORK/job.sql"
  # The client usually exits 1 on a failed statement, but its output is
  # checked too, the way ksql-init checks the ksql CLI's.
  if "$SQL_CLIENT" -f "$WORK/job.sql" >"$WORK/out" 2>&1; then rc=0; else rc=$?; fi
  cat "$WORK/out"
  if [ "$rc" -ne 0 ] || grep -q '\[ERROR\]' "$WORK/out"; then
    fail "submitting $file failed (sql-client exit $rc)"
  fi

  # Submitted is not running: wait for the job to get its slots.
  i=0
  until [ "$(job_state "$name")" = "RUNNING" ]; do
    i=$((i + 1))
    [ "$i" -le 45 ] || fail "job '$name' did not reach RUNNING within 90 s (state: $(job_state "$name"))"
    sleep 2
  done
  log "job '$name' is RUNNING"
done

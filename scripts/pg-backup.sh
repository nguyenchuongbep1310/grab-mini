#!/bin/sh
# Logical backup of one service's Postgres database to object storage.
# POSIX sh. The backup CronJobs run this very file (deploy/k8s/generate.py puts
# it in a ConfigMap), in two containers that share an emptyDir because no
# single image has both pg_dump and the AWS CLI:
#
#   pg-backup.sh dump     # postgres image:  pg_dump -> $BACKUP_DIR/<name>-<UTC timestamp>.dump
#   pg-backup.sh upload   # aws-cli image:   copy it to $BACKUP_TARGET/<name>/
#   pg-backup.sh all      # both, for a machine that has both tools (and the local drill)
#
# Environment:
#   BACKUP_NAME          what the dump is called, e.g. order-service (required)
#   DATABASE_URL or DATABASE_URL_FILE   (dump) the database to dump
#   BACKUP_DIR           working directory (default /backup)
#   BACKUP_TARGET        s3://bucket/prefix, or a directory (the drill, or an NFS mount)
#   BACKUP_S3_ENDPOINT   optional endpoint URL for S3-compatible storage (MinIO, R2, ...)
#   AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY   for s3:// targets
#
# The dump is pg_dump's custom format (-Fc): compressed, and restorable table by
# table with pg_restore. It is a snapshot at one instant (a single transaction),
# not PITR: see docs/disaster-recovery.md. Deleting old dumps is the bucket's
# lifecycle rule, not this script's job: a script that can delete backups is a
# script that can delete all of them.
set -eu

mode=${1:-}
name=${BACKUP_NAME:?set BACKUP_NAME}
dir=${BACKUP_DIR:-/backup}
target=${BACKUP_TARGET:?set BACKUP_TARGET}

case "$name" in *[!A-Za-z0-9_-]* | '') echo "BACKUP_NAME may only contain letters, digits, - and _" >&2; exit 2 ;; esac

dump() {
  url=${DATABASE_URL:-}
  if [ -z "$url" ]; then
    url=$(cat "${DATABASE_URL_FILE:?set DATABASE_URL or DATABASE_URL_FILE}")
  fi
  mkdir -p "$dir"
  file="$dir/$name-$(date -u +%Y%m%dT%H%M%SZ).dump"
  # Written under a temporary name so an interrupted dump is never picked up as a backup.
  pg_dump --format=custom --no-owner --no-acl --file="$file.partial" "$url"
  # Proves the archive is readable and has a table of contents; a restore is the real test.
  pg_restore --list "$file.partial" > /dev/null
  mv "$file.partial" "$file"
  echo "dumped $file ($(wc -c < "$file") bytes)"
}

upload() {
  file=$(ls -1 "$dir/$name"-*.dump 2> /dev/null | sort | tail -n 1)
  [ -n "$file" ] || { echo "no dump of $name in $dir" >&2; exit 1; }
  case "$target" in
    s3://*)
      endpoint=
      [ -n "${BACKUP_S3_ENDPOINT:-}" ] && endpoint="--endpoint-url $BACKUP_S3_ENDPOINT"
      # shellcheck disable=SC2086
      aws $endpoint s3 cp --only-show-errors "$file" "${target%/}/$name/$(basename "$file")"
      ;;
    *)
      mkdir -p "$target/$name"
      cp "$file" "$target/$name/$(basename "$file").partial"
      mv "$target/$name/$(basename "$file").partial" "$target/$name/$(basename "$file")"
      ;;
  esac
  echo "uploaded $(basename "$file") to ${target%/}/$name/"
}

case "$mode" in
  dump) dump ;;
  upload) upload ;;
  all) dump; upload ;;
  *) echo "usage: $0 dump|upload|all" >&2; exit 2 ;;
esac

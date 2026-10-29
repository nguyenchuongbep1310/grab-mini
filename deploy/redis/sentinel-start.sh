#!/bin/sh
# Starts the sentinel container of one StatefulSet pod. It watches the Redis in
# its own pod first and learns the rest from the other Sentinels, so it never
# needs to be told who the master is: it asks the Redis next to it, which
# redis-start.sh pointed at the current master.
# Environment: REDIS_PASSWORD, REDIS_DOMAIN, REDIS_MASTER_NAME, SENTINEL_QUORUM
# (default 2 of 3), SENTINEL_DOWN_AFTER_MS (default 5000).
set -eu

: "${REDIS_PASSWORD:?}" "${REDIS_DOMAIN:?}"
export REDISCLI_AUTH=$REDIS_PASSWORD
name=${REDIS_MASTER_NAME:-mymaster}
fqdn=$HOSTNAME.$REDIS_DOMAIN
conf=${SENTINEL_CONF:-/sentinel/sentinel.conf}

# Wait for the Redis in this pod: its role says who the master is.
master=
tries=0
while [ -z "$master" ]; do
  out=$(timeout 3 redis-cli -h 127.0.0.1 role 2> /dev/null || true)
  case "$(echo "$out" | head -n 1)" in
    master) master=$fqdn ;;
    slave) master=$(echo "$out" | sed -n 2p) ;;
  esac
  [ -n "$master" ] && break
  tries=$((tries + 1))
  [ "$tries" -gt 120 ] && { echo "the Redis in this pod never answered" >&2; exit 1; }
  sleep 1
done

# A stable id: a restarted Sentinel (new IP) must be recognised as the same one,
# or the others keep a ghost of the old run and count it towards the quorum.
myid=$(printf '%s' "$HOSTNAME" | sha1sum | cut -c1-40)

cat > "$conf" << EOF
port 26379
dir /sentinel
requirepass $REDIS_PASSWORD
sentinel myid $myid
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $fqdn
sentinel monitor $name $master 6379 ${SENTINEL_QUORUM:-2}
sentinel auth-pass $name $REDIS_PASSWORD
sentinel down-after-milliseconds $name ${SENTINEL_DOWN_AFTER_MS:-5000}
sentinel failover-timeout $name 30000
sentinel parallel-syncs $name 1
EOF

echo "sentinel for $name, master at $master"
exec redis-sentinel "$conf"

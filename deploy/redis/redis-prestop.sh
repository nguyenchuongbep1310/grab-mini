#!/bin/sh
# preStop of the redis container: if this pod is the master, hand the role to a
# replica first, so a node drain or rolling update costs a planned failover
# (seconds) rather than waiting for Sentinel to notice a dead master (down-after
# + election). Never fails: a pod that cannot hand over is still allowed to stop.
export REDISCLI_AUTH=${REDIS_PASSWORD:-}
name=${REDIS_MASTER_NAME:-mymaster}
count=${REDIS_REPLICAS:-3}
me=$HOSTNAME
base=${me%-*}

[ "$(timeout 3 redis-cli -h 127.0.0.1 role 2> /dev/null | head -n 1)" = master ] || exit 0

i=0
while [ "$i" -lt "$count" ]; do
  peer=$base-$i
  if [ "$peer" != "$me" ]; then
    # A Sentinel on another pod (ours is stopping at the same time as this container).
    if timeout 5 redis-cli -h "$peer.$REDIS_DOMAIN" -p 26379 sentinel failover "$name" 2> /dev/null | grep -q OK; then
      # Wait until this node is no longer the master, so no write is lost to a
      # half-finished handover.
      n=0
      while [ "$n" -lt 20 ]; do
        [ "$(timeout 3 redis-cli -h 127.0.0.1 role 2> /dev/null | head -n 1)" = master ] || exit 0
        n=$((n + 1))
        sleep 1
      done
      exit 0
    fi
  fi
  i=$((i + 1))
done
exit 0

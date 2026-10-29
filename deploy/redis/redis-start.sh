#!/bin/sh
# Starts the redis container of one StatefulSet pod (deploy/k8s generate.py puts
# the three scripts in this directory in a ConfigMap; scripts/test-redis-ha.sh
# runs them on real Redis and Sentinel).
#
# Who is master is decided by the Sentinels, not by this script: it only has
# to join the cluster the way it is NOW, not the way it was at first boot.
#   1. ask the other Sentinels for the master; if one answers, follow that
#      (if it names this very pod, this pod is the master that restarted);
#   2. else ask the other Redis nodes for their role and follow the master;
#   3. else (cold start, nobody answers) redis-0 is the master and the others
#      replicate redis-0.
# Environment: REDIS_PASSWORD (from the Secret), REDIS_DOMAIN (headless Service
# domain the pods are reachable under), REDIS_MASTER_NAME, REDIS_REPLICAS,
# REDIS_MAXMEMORY.
set -eu

: "${REDIS_PASSWORD:?}" "${REDIS_DOMAIN:?}"
export REDISCLI_AUTH=$REDIS_PASSWORD
name=${REDIS_MASTER_NAME:-mymaster}
count=${REDIS_REPLICAS:-3}
me=$HOSTNAME
base=${me%-*}
fqdn=$me.$REDIS_DOMAIN
conf=${REDIS_CONF:-/conf/redis.conf}

master=
i=0
while [ "$i" -lt "$count" ] && [ -z "$master" ]; do
  peer=$base-$i
  if [ "$peer" != "$me" ]; then
    master=$(timeout 3 redis-cli -h "$peer.$REDIS_DOMAIN" -p 26379 sentinel get-master-addr-by-name "$name" 2> /dev/null | head -n 1 || true)
  fi
  i=$((i + 1))
done
if [ -z "$master" ]; then
  i=0
  while [ "$i" -lt "$count" ] && [ -z "$master" ]; do
    peer=$base-$i
    if [ "$peer" != "$me" ]; then
      out=$(timeout 3 redis-cli -h "$peer.$REDIS_DOMAIN" role 2> /dev/null || true)
      if [ "$(echo "$out" | head -n 1)" = master ]; then master=$peer.$REDIS_DOMAIN; fi
    fi
    i=$((i + 1))
  done
fi
if [ -z "$master" ] && [ "$me" != "$base-0" ]; then master=$base-0.$REDIS_DOMAIN; fi
[ "$master" = "$fqdn" ] && master=

{
  echo "port 6379"
  echo "requirepass $REDIS_PASSWORD"
  echo "masterauth $REDIS_PASSWORD"
  echo "dir /data"
  # AOF every second: a crash loses at most about a second of writes. No RDB
  # snapshots on top: they fork the process for nothing the AOF doesn't already give.
  echo "appendonly yes"
  echo "appendfsync everysec"
  echo 'save ""'
  # noeviction: a full Redis refuses writes loudly. Evicting would silently drop
  # a lease or a match claim, which is worse than an error the service retries.
  echo "maxmemory ${REDIS_MAXMEMORY:-400mb}"
  echo "maxmemory-policy noeviction"
  # Pods change IP when they restart; Sentinel and the other replicas find this one by name.
  echo "replica-announce-ip $fqdn"
  # A replica must not serve reads from a stale copy while it resyncs.
  echo "replica-serve-stale-data no"
  # If the master has no replica at all it keeps accepting writes (availability over
  # the last second of data), which is also what Sentinel failover assumes.
  if [ -n "$master" ]; then echo "replicaof $master 6379"; fi
} > "$conf"

echo "starting as $([ -n "$master" ] && echo "replica of $master" || echo master)"
exec redis-server "$conf"

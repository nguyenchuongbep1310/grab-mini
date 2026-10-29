#!/usr/bin/env bash
# Runs the Redis + Sentinel scripts of deploy/redis/ on real Redis, laid out
# the way the StatefulSet lays them out: three "pods" (a redis container and a
# sentinel container sharing a network namespace, reachable as
# redis-N.redis-headless), then breaks things and checks that it heals.
#
#   scripts/test-redis-ha.sh
#
# Needs Docker and Go. Creates containers/volumes/network named ag-ops-redis-*
# and removes them on exit. What it does NOT prove: Kubernetes scheduling,
# PVC binding, DNS of a real cluster, probes, anti-affinity (nothing here runs
# a kubelet): those are only schema-checked by deploy/verify.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$PWD

IMAGE=$(sed -n 's/^REDIS_IMAGE = "\(.*\)"/\1/p' deploy/k8s/generate.py)
[ -n "$IMAGE" ] || { echo "cannot read REDIS_IMAGE from deploy/k8s/generate.py"; exit 1; }
NET=ag-ops-redis-net
DOMAIN=redis-headless
PW=ag-ops-redis-password
export REDISCLI_AUTH=$PW

cleanup() {
  for i in 0 1 2; do docker rm -f "ag-ops-redis-$i" "ag-ops-redis-sentinel-$i" > /dev/null 2>&1 || true; done
  docker rm -f ag-ops-redis-client > /dev/null 2>&1 || true
  docker volume rm -f ag-ops-redis-data-0 ag-ops-redis-data-1 ag-ops-redis-data-2 > /dev/null 2>&1 || true
  docker network rm "$NET" > /dev/null 2>&1 || true
  rm -f "${client_bin:-/nonexistent}"
}
[ "${KEEP:-0}" = 1 ] || trap cleanup EXIT
cleanup

pass=0
ok() { pass=$((pass + 1)); printf '  ok   %s\n' "$*"; }
die() { printf '  FAIL %s\n' "$*" >&2; docker logs ag-ops-redis-0 2>&1 | tail -5 >&2 || true; exit 1; }
step() { printf '\n== %s\n' "$*"; }

# cli N [args]: redis-cli inside pod N's redis container
cli() { local n=$1; shift; docker exec -e REDISCLI_AUTH="$PW" "ag-ops-redis-$n" redis-cli "$@" 2> /dev/null; }
scli() { local n=$1; shift; docker exec -e REDISCLI_AUTH="$PW" "ag-ops-redis-$n" redis-cli -p 26379 "$@" 2> /dev/null; }
role() { cli "$1" role | head -n 1; }
wait_for() { # DESCRIPTION SECONDS CMD...
  local what=$1 secs=$2 i
  shift 2
  for ((i = 0; i < secs * 2; i++)); do "$@" > /dev/null 2>&1 && return 0; sleep 0.5; done
  die "timed out waiting for $what"
}
others_is() { [ "$(scli "$1" sentinel master mymaster | tr -d "\r" | awk "/^num-other-sentinels\$/{getline; print}")" = "$2" ]; }
master_name() { scli "$1" sentinel get-master-addr-by-name mymaster | head -n 1; }

start_pod() { # N: a pod = redis + sentinel in one network namespace
  local n=$1
  docker volume create "ag-ops-redis-data-$n" > /dev/null
  docker run -d --name "ag-ops-redis-$n" --hostname "redis-$n" --network "$NET" --network-alias "redis-$n.$DOMAIN" \
    -v "$ROOT/deploy/redis:/scripts:ro" -v "ag-ops-redis-data-$n:/data" --tmpfs /conf:uid=999,gid=1000 \
    -e REDIS_PASSWORD="$PW" -e REDIS_DOMAIN="$DOMAIN" -e REDIS_MAXMEMORY=64mb \
    --read-only --user 999:1000 "$IMAGE" sh /scripts/redis-start.sh > /dev/null
  wait_for "redis-$n to answer" 30 cli "$n" ping
  docker run -d --name "ag-ops-redis-sentinel-$n" --network "container:ag-ops-redis-$n" \
    -v "$ROOT/deploy/redis:/scripts:ro" --tmpfs /sentinel:uid=999,gid=1000 \
    -e HOSTNAME="redis-$n" -e REDIS_PASSWORD="$PW" -e REDIS_DOMAIN="$DOMAIN" -e SENTINEL_DOWN_AFTER_MS=3000 \
    --read-only --user 999:1000 "$IMAGE" sh /scripts/sentinel-start.sh > /dev/null
  wait_for "sentinel $n to answer" 30 scli "$n" ping
}
current_master() { # index of the pod whose redis says master (the one every Sentinel names)
  local h
  h=$(master_name "$1")
  echo "${h#redis-}" | cut -d. -f1
}

step "start three pods in order (like the StatefulSet), pull $IMAGE"
docker network create "$NET" > /dev/null
for n in 0 1 2; do start_pod "$n"; done
[ "$(role 0)" = master ] && ok "redis-0 is the master on a cold start" || die "redis-0 is not master"
wait_for "replicas to attach" 30 sh -c "[ \"\$(docker exec -e REDISCLI_AUTH=$PW ag-ops-redis-0 redis-cli info replication | tr -d '\r' | sed -n 's/^connected_slaves://p')\" = 2 ]"
ok "redis-1 and redis-2 are replicas of redis-0 (connected_slaves:2)"
wait_for "sentinels to see each other" 30 others_is 0 2
ok "each Sentinel sees 2 others (quorum 2 of 3)"
[ "$(scli 1 sentinel ckquorum mymaster | head -n 1)" != "" ] && scli 1 sentinel ckquorum mymaster | grep -q OK && ok "SENTINEL CKQUORUM is OK" || die "ckquorum"
for n in 0 1 2; do
  [ "$(master_name "$n")" = "redis-0.$DOMAIN" ] || die "sentinel $n names '$(master_name "$n")' as master"
done
ok "all three Sentinels name redis-0.$DOMAIN (a hostname, not an IP)"
replicas_known() { [ "$(scli 0 sentinel replicas mymaster | tr -d "\r" | grep -c "^ip\$")" = 2 ]; }
wait_for "Sentinel to discover both replicas (INFO polling, up to 10 s)" 30 replicas_known && ok "Sentinel knows both replicas"

step "security and persistence settings really applied"
if docker exec ag-ops-redis-0 redis-cli ping 2>&1 | grep -q NOAUTH; then ok "no password -> NOAUTH (Redis)"; else die "Redis answered without a password"; fi
if docker exec ag-ops-redis-0 redis-cli -p 26379 ping 2>&1 | grep -q NOAUTH; then ok "no password -> NOAUTH (Sentinel)"; else die "Sentinel answered without a password"; fi
[ "$(cli 0 config get appendonly | tail -n 1)" = yes ] && ok "appendonly yes" || die "appendonly"
[ "$(cli 0 config get maxmemory-policy | tail -n 1)" = noeviction ] && ok "maxmemory-policy noeviction" || die "policy"

step "replication carries writes"
cli 0 set k1 v1 > /dev/null
wait_for "k1 on both replicas" 10 sh -c "[ \"\$(docker exec -e REDISCLI_AUTH=$PW ag-ops-redis-1 redis-cli get k1)\" = v1 ] && [ \"\$(docker exec -e REDISCLI_AUTH=$PW ag-ops-redis-2 redis-cli get k1)\" = v1 ]"
ok "k1 reached both replicas"

step "unplanned failure: kill pod 0 (redis + sentinel)"
t0=$(date +%s)
docker kill ag-ops-redis-0 ag-ops-redis-sentinel-0 > /dev/null
wait_for "a new master" 60 sh -c "h=\$(docker exec -e REDISCLI_AUTH=$PW ag-ops-redis-1 redis-cli -p 26379 sentinel get-master-addr-by-name mymaster | head -n 1); [ -n \"\$h\" ] && [ \"\$h\" != redis-0.$DOMAIN ]"
new=$(current_master 1)
t1=$(date +%s)
ok "Sentinel promoted redis-$new after $((t1 - t0)) s (down-after 3 s in this test; the manifest uses 5 s)"
wait_for "the new master to accept writes" 30 cli "$new" set k2 v2
ok "the new master accepts writes; k1 survived: $(cli "$new" get k1)"
[ "$(cli "$new" get k1)" = v1 ] || die "k1 lost in the failover"

step "pod 0 comes back: it must rejoin as a replica of the NEW master, not reclaim the role"
docker start ag-ops-redis-0 > /dev/null
wait_for "redis-0 to answer" 30 cli 0 ping
docker start ag-ops-redis-sentinel-0 > /dev/null
wait_for "redis-0 to become a replica" 30 sh -c "[ \"\$(docker exec -e REDISCLI_AUTH=$PW ag-ops-redis-0 redis-cli role | head -n 1)\" = slave ]"
ok "redis-0 restarted as a replica of redis-$new"
wait_for "k2 on redis-0 (resync)" 20 sh -c "[ \"\$(docker exec -e REDISCLI_AUTH=$PW ag-ops-redis-0 redis-cli get k2)\" = v2 ]"
ok "redis-0 caught up (k2 present)"
wait_for "3 sentinels again, no ghost" 30 others_is "$new" 2
ok "Sentinel count is back to 3 (stable myid: no ghost left by the restart)"

step "planned handover: preStop on the master (a node drain)"
m=$(current_master 0)
t0=$(date +%s)
docker exec -e REDIS_DOMAIN="$DOMAIN" -e REDIS_PASSWORD="$PW" "ag-ops-redis-$m" sh /scripts/redis-prestop.sh
t1=$(date +%s)
[ "$(role "$m")" = slave ] && ok "preStop handed the master role away from redis-$m in $((t1 - t0)) s" || die "redis-$m is still master after preStop"

step "the real Go client (services/pkg/redisconn) follows a failover through these Sentinels"
client_bin=$(mktemp)
(cd services/pkg/redisconn && GOOS=linux GOARCH="$(docker version -f '{{.Server.Arch}}')" CGO_ENABLED=0 go test -c -o "$client_bin" .)
chmod +x "$client_bin"
out=$(docker run --rm --name ag-ops-redis-client --network "$NET" -v "$client_bin:/redisconn.test:ro" \
  -e TEST_REDIS_SENTINEL_ADDRS="redis-0.$DOMAIN:26379,redis-1.$DOMAIN:26379,redis-2.$DOMAIN:26379" \
  -e REDIS_PASSWORD="$PW" -e REDIS_SENTINEL_PASSWORD="$PW" "$IMAGE" /redisconn.test -test.run TestTheClientFollowsAFailover -test.v 2>&1) || { echo "$out"; die "Go client failover test"; }
echo "$out" | sed 's/^/       /'
echo "$out" | grep -q -- '--- PASS: TestTheClientFollowsAFailover' && ok "TestTheClientFollowsAFailover passed against the 3-node Sentinel set" || die "Go client failover test did not pass"

printf '\nall %d checks passed\n' "$pass"

# Runbook

What breaks, where to look, what to do. Each section below corresponds to an alert in
[deploy/monitoring/alerts.rules.yaml](../deploy/monitoring/alerts.rules.yaml): the alert's `runbook`
field points to the section heading (do not rename a heading without updating the alert).

> **Scope of verification.** The project's tools (`dlq-redrive`, Postgres queries, Redis keys,
> metrics, logs) have been tried in a local test environment. The `kubectl` commands and the Strimzi/KEDA
> resources have **not been run on a real cluster**: the manifests have passed schema checks and cross-checks
> (`deploy/verify.sh`) but have not been applied. Try them in staging the first time you use them.

## How to investigate an incident

1. **What does the alert say?** The alert name tells you which system is affected. `severity: page` means riders
   are affected or data is at risk right now.
2. **Which pod is unhealthy?** Each service's `/readyz` returns JSON saying which dependency is failing
   (admin port `9090`, not the API port):
   ```bash
   kubectl -n grab-mini port-forward deploy/trip-service 9090 &
   curl -s localhost:9090/readyz    # {"checks":{"kafka":"ok","postgres":"ok","redis":"connection refused"},"status":"unavailable"}
   ```
3. **Where did a specific order go?** Logs are JSON, and every line has `service`; lines inside a span also have
   `trace_id`. Search by `orderId`, or by `trace_id` to see what every service did for the same request
   (HTTP, outbox, each Kafka hop, and all retries are in one trace).
4. **Consumer state:**
   ```bash
   kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --command-config ops.properties --describe --group matching-service-group
   ```
   (`ops.properties` holds the operator's credentials; see `deploy/kafka-secure` and the Strimzi `KafkaUser/ops`.)

Dashboards worth having: lag per group, `kafka_consumer_messages_total` by `result`,
`outbox_oldest_pending_age_seconds`, `matching_latency_seconds`, `http_requests_total` by route and code.

---

## Consumer lag

**Alerts:** `KafkaConsumerLagGrowing`, `KafkaLocationConsumerLagGrowing`.
**Meaning:** a consumer group is processing slower than messages arrive. Riders see slow or missing status updates.

Lag on `*.retry.N` groups is normal (messages waiting until they are due); the alerts exclude them.

**Check in this order:**

1. Is the lag really growing, or was there just a spike? Look at `kafka_consumergroup_lag` by `partition`:
   - **One partition lagging, the others not:** a "heavy" message or a hot key. Look in the logs for the
     `topic`, `partition` and `offset` of the message being processed.
   - **All partitions lagging:** the consumers lack capacity, or a dependency is slow.
2. Is the handler slow? `histogram_quantile(0.95, sum by (le, group) (rate(kafka_consumer_handle_duration_seconds_bucket[5m])))`.
   A sudden increase means Postgres or Redis is slow (see `/readyz`, and the [Pods not ready](#pods-not-ready) section).
3. Is there a retry storm? `rate(kafka_consumer_messages_total{result="retried"}[5m])`. If so, the cause
   is a failing dependency: see [Dead letters](#dead-letters).
4. Is KEDA scaling? `kubectl -n grab-mini get scaledobject`, then `describe` the relevant one.
   The replica count is **capped by the partition count** (6, 12 or 48 depending on the topic): adding replicas beyond the partition count
   does not help. If replicas are already at the maximum and there is still lag, you need to increase the partition count (it can only be increased, and it changes
   the key → partition mapping, so ordering may be temporarily off during the change) or make the handler faster.
5. Constant rebalancing? The log shows repeated `consumer stopped` / `consumer running`, or pods are restarting: check
   `kubectl get pods` (RESTARTS) and `kubectl describe pod` (OOMKilled?).

**Do not:** reset offsets to "skip" the lag. A skipped message is an event that is never processed
(an order that is never matched, a rating that is never counted).

## Dead letters

**Alerts:** `KafkaMessagesDeadLettered` (page, any message), `KafkaConsumerFailureRatioHigh`.
**Meaning:** a message could not be processed after all retries, so it was parked in `<topic>.DLQ`. That event
**has not been processed**.

1. **See what it is.** Each dead letter is JSON: `originalTopic`, `error`, `retries`, `valueText` (if it is text)
   and `valueBase64` (the original bytes). Read it as the operator:
   ```bash
   kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --consumer.config ops.properties \
     --topic order.created.DLQ --from-beginning --max-messages 5
   ```
   Or preview without changing anything: `dlq-redrive -topic order.created -dry-run`.
2. **Classify by `error` and `retries`:**
   | Sign | Common cause | What to do |
   |---|---|---|
   | `retries: 0`, error `not a Schema Registry Avro message` or `isn't readable with the ... schema` | Someone wrote a message in the wrong format, or made an incompatible schema change | Fix the writer. For a genuinely corrupt message, **do not redrive**: it will land in the DLQ again |
   | `retries: 3` (all tiers), `postgres`/`redis` connection error | A dependency was down for a while | Wait for the dependency to recover, then redrive |
   | Business error repeating for the same message | A bug in the handler | Fix the code, deploy, then redrive |
3. **Redrive** after the cause has been fixed. Redrive pushes messages back to the original topic with the **exact key and bytes**; the consumer
   processes them again (consumers are idempotent, so processing twice is safe):
   ```bash
   # see what it would do
   dlq-redrive -topic order.created -dry-run
   # a single cause only, at most 100 messages at a time
   dlq-redrive -topic order.created -error "connection refused" -max 100
   # everything
   dlq-redrive -topic order.created
   ```
   It needs the operator's credentials (the services' principals **cannot** read the DLQ, by design), configured with
   the same `KAFKA_*` variables as every service (`KAFKA_BROKERS`, `KAFKA_SECURITY_PROTOCOL`, `KAFKA_USERNAME`,
   `KAFKA_PASSWORD_FILE`, `KAFKA_CA_FILE`). On Kubernetes, run it as a Job using the `dlq-redrive` image with the `ops` user's secret.
   - Running it again sends only what is new: the position is remembered in the consumer group `dlq-redrive-<topic>`
     (each `-error` filter has its own group, because a group cannot skip messages).
   - A message that fails again lands in the DLQ once more as a new dead letter.
4. **Verify afterwards:** `kafka_consumer_messages_total{result="dead_lettered"}` stops increasing and lag returns to 0.

**`driver.location.updated` specifically:** dead letters here do not page (a stale location is worthless).
Do not redrive: a fresh location arrives within seconds, and a redrive would be discarded anyway because of the old `timestamp`.

## Kafka broker down

**Alerts:** `KafkaBrokersMissing`, `KafkaUnderReplicatedPartitions`, `KafkaPublishErrors`, `KafkaPublishSlow`.

Setup: 3 brokers, one per zone, `replication.factor=3`, `min.insync.replicas=2`
(location topic: `1`), unclean leader election disabled.

- **Losing 1 broker** (or 1 zone): the system **can still write** (2 in-sync replicas remain), but there is no redundancy left. Losing one
  more means producers get `NotEnoughReplicas`. Treat this as an incident to handle immediately even if users have not noticed anything.
- **Losing 2 brokers:** the business topics cannot be written.
  - Order Service and Trip Service: events sit in the `outbox` table (committed together with the state change) and are **not lost**.
    `outbox_oldest_pending_age_seconds` grows; see [Outbox backlog](#outbox-backlog). When the brokers return, the relay catches up and sends them.
  - Matching Service and Location Gateway publish directly, **with no outbox**. The gateway returns 503 and the driver resends the
    location a few seconds later. Matching: if `order.matched` cannot be published, the handler returns an error and releases the driver (`ReleaseMatch`),
    and the message goes down the retry path. If neither the retry topic nor the DLQ can be written either, the consumer **holds on to the partition**
    (log `cannot put a failed message on a retry or dead-letter topic`, lag grows) rather than committing and losing the message,
    and resumes once Kafka is writable again.

**Check:**
```bash
kubectl -n grab-mini get kafka,kafkanodepool,pods -l strimzi.io/cluster=grab-mini
kubectl -n grab-mini describe kafka grab-mini           # Ready condition, reason
kubectl -n grab-mini logs grab-mini-broker-1            # example broker pod
```
`kafka_topic_partition_under_replicated_partition` tells you which topic and partition; `kafka_brokers` gives the broker count.

**Fix:** brokers usually come back on their own (pod restart, node move). If not:
1. Is the pod `Pending`? Usually insufficient resources or the PVC cannot attach in that zone
   (`kubectl describe pod`): a broker must run in the zone where its disk is.
2. Disk full? `kubectl exec ... -- df -h /var/lib/kafka`. Lower `retention.ms` on the largest topic (usually
   `driver.location.updated`) or expand the PVC.
3. **Do not** enable `unclean.leader.election.enable` to "get it running again": it trades acknowledged data for availability.

**After recovery:** URP returns to 0, `outbox_oldest_pending_age_seconds` returns to 0, lag falls. No redrive is needed: no
message is dead-lettered merely because a broker was briefly down (they are retried).

## Outbox backlog

**Alerts:** `OutboxBacklog` (an event waiting more than 30 seconds), `OutboxEventQuarantined`.
**Meaning:** state has been saved but **not announced**: riders and other services do not know about it yet.

1. **Look at the table** (the Order or Trip Postgres):
   ```sql
   SELECT source, count(*) AS waiting, min(created_at) AS oldest FROM outbox GROUP BY source;
   SELECT id, source, topic, failed_at, left(error, 120) FROM outbox_dead ORDER BY failed_at DESC LIMIT 20;
   ```
2. **Is the relay running?** In the order-service/trip-service logs, search for `[outbox:...] publish failed`: the reason is right there.
   - `connection refused` / `broker not available`: see [Kafka broker down](#kafka-broker-down).
   - `Topic Authorization Failed`: the service's ACL was changed or is missing (its own `KafkaUser` needs `Write` on the topic).
     Fix the ACL; the relay will catch up and send the events by itself, nothing more to do.
   - `Schema Registry`: unrelated to the relay (the payload was already Avro-encoded at request time); this error shows up at order creation.
3. **Is the relay stuck on a lock?** Only one relay runs per source at a time (advisory lock). If a hung pod holds
   an open transaction, the other pods skip:
   ```sql
   SELECT pid, state, now() - xact_start AS open_for, left(query, 80) FROM pg_stat_activity WHERE state <> 'idle' ORDER BY xact_start;
   ```
   A relay transaction open for a very long time: restart that pod.
4. **`outbox_dead`** (alert `OutboxEventQuarantined`): the relay has **set aside** an event that Kafka permanently rejects
   (usually too large for `message.max.bytes`, or an invalid topic), so that the events behind it are not blocked.
   That event **will not be sent**: the state is saved but no one is notified.
   - Read `error`, `topic` and the size (`length(payload)`). The payload is binary Avro, so it cannot be read as text.
   - Fix the cause (usually a bug that produces an oversized payload).
   - To resend after the fix: copy the row back into `outbox`, then delete it from `outbox_dead`:
     ```sql
     BEGIN;
     INSERT INTO outbox (source, topic, key, payload, headers, created_at)
       SELECT source, topic, key, payload, headers, now() FROM outbox_dead WHERE id = <id>;
     DELETE FROM outbox_dead WHERE id = <id>;
     COMMIT;
     ```
     Only do this when Kafka will accept it now. Otherwise, make a business decision: reconcile the state by hand.

## Slow matching

**Alerts:** `MatchLatencySLOBreach` (p95 above 3 seconds), `NoDriverRatioHigh` (more than 30% of orders have no driver).

Match time = from when `order.created` is produced to when `order.matched` is published
(`matching_latency_seconds`). When no driver is available, Matching retries `MATCH_RETRIES` (default 3) times, `MATCH_RETRY_DELAY_MS`
(default 2000 ms) apart: an order for which **no driver is found** takes about 6 seconds before reporting
`order.no_driver_found`, and that time counts toward the latency. That is why the two alerts often fire together.

1. **Lag of `matching-service-group`** on `order.created`: see [Consumer lag](#consumer-lag). Orders are waiting in the topic.
2. **Redis slow or down:** the matching-service `/readyz` (admin port). Matching depends on Redis for `drivers:geo`
   and driver state.
3. **A real driver shortage, or bad data?** Is `rate(location_updates_total{result="applied"}[5m])` steady?
   If locations are not being updated, Driver Location Service or Location Gateway is broken
   (`location_reports_total` by `result`: `published`, `stationary`, `too_fast`, `invalid`, `error`).
4. **Driver stuck in a state.** Matching only accepts drivers that are `AVAILABLE` (or `ASSIGNED` with an expired lease, default 120 seconds),
   and never one with a `driver:<id>:suspended` key (an admin rejected or suspended them: see [Driver approval and suspension](#driver-approval-and-suspension)).
   A driver that is `ON_TRIP` with no running trip is stuck. Check both sources:
   ```sql
   -- Trip Service Postgres: does the driver have a running trip?
   SELECT driver_id, id, status FROM trips WHERE driver_id = 'driver-7' AND status IN ('ASSIGNED','IN_PROGRESS');
   ```
   ```bash
   redis-cli GET driver:driver-7:status      # AVAILABLE | ASSIGNED | ON_TRIP
   redis-cli GET driver:driver-7:assigned_until
   ```
   If there is **no running trip** but Redis still says `ON_TRIP`, the `trip-driver-release-group` consumer is behind or broken
   (its lag); once that is fixed, it releases the driver by itself. Release by hand only when you are sure no trip remains:
   `redis-cli SET driver:driver-7:status AVAILABLE`, then `redis-cli DEL driver:driver-7:assigned_until`.
   (A driver who is suspended stays unmatchable whatever the status says; the next release back to `AVAILABLE` records `OFFLINE` for them.)
5. **Stale locations:** `drivers:geo` keeps each driver's latest location; a driver who stops sending locations stays in the GEO set with the
   old location. There is no TTL: an offline driver can still be matched. (This is a known limitation, see production-design.)

## Booking errors

**Alerts:** `OrderCreationErrorBudgetBurn` (page), `HTTPServerErrorsHigh`.
Target: 99.9% of `POST /orders` succeed. The alert fires when the error budget is burning 14 times too fast, over both the 5-minute and the 1-hour window.

`POST /orders` does: token validation, Avro encoding (needs the **Schema Registry**), a Postgres write (order + outbox in one transaction).
A 5xx means one of those three is broken. Tell them apart using the order-service logs (`failed to encode order.created`
→ Schema Registry; `failed to save order` → Postgres) or `/readyz` (Postgres, Kafka).

- **Schema Registry down:** each service remembers its schema ids after the first registration, so **running** pods keep accepting orders
  normally. The 500 errors show up on **newly started** pods (which could not register) and on consumers that meet a `schema id` they have never
  seen (a new schema version). So a registry problem usually shows up after a deploy or scale-out, not right
  when the registry dies. Restore the registry, then check `/readyz`.
- **Postgres down:** see [Pods not ready](#pods-not-ready); Postgres failover is the provider's/DBA's job.
- **A sudden rise in 401s** (not 5xx, but users see errors): `JWT_JWKS_URL` is unreachable (look for `auth: refreshing signing keys`
  in the logs, and check the egress NetworkPolicy allows the issuer), the issuer just rotated its key (services fetch the new key when they
  meet an unknown `kid`, at most once every 15 seconds), or tokens expired en masse. See [security.md](security.md).

## Surge pricing

**Alerts:** `SurgePricingFailOpen` (the surge multiplier could not be read, price falls back to 1.0), `SurgeSamplerStalled` (pricing-service has stopped sampling supply/demand).

Surge **never blocks booking**: any Redis read error yields a multiplier of 1.0, so the consequence of an incident is *prices not going up*, not bookings failing.

1. **Look at the current multiplier of a cell:** cell = `floor(lat/degrees):floor(lng/degrees)` (degrees = `SURGE_CELL_DEGREES`, or `GEO_SHARD_DEGREES`, default 0.05).
   ```bash
   redis-cli SMEMBERS surge:cells                      # cells with demand, in the form CAR|215:2134
   redis-cli GET surge:mult:CAR:215:2134               # the multiplier; no key = 1.0
   redis-cli TTL surge:mult:CAR:215:2134               # at most 30 seconds, refreshed every 5 seconds
   redis-cli MGET surge:demand:CAR:215:2134:<unix minute/60>
   ```
2. **`SurgePricingFailOpen`:** Redis is slow/down (300 ms read timeout). See [Pods not ready](#pods-not-ready), and the `/readyz` of pricing-service and order-service.
3. **`SurgeSamplerStalled`:** `pricing_ticks_total` is not increasing. Only one instance holds the `surge:sampler:lock` lock each cycle; check that the
   pricing-service pods are running, the `surge sample failed` log, and `pricing_sample_errors_total` (`driverlocation.Search` errors, for example a radius exceeding `maxCells` when `GEO_SHARD_DEGREES` is too small).
   A lagging `pricing-service-group` consumer makes *demand* undercounted: see [Consumer lag](#consumer-lag).
4. **Riders report a 409 when booking:** by design. The quote has expired and the multiplier has drifted by more than `SURGE_QUOTE_TOLERANCE` (0.2); the app shows the new price for confirmation.
   Many **400 "quote is invalid"** errors mean `QUOTE_SECRET` differs between order-service and pricing-service (or someone is forging quotes).
5. **Abnormally high prices:** `pricing_surge_multiplier_max` / `pricing_surging_cells`; the cap is `SURGE_MAX` (3.0). To turn surge off in an emergency:
   set `SURGE_MAX=1` for pricing-service and restart, or `redis-cli DEL` the `surge:mult:*` keys (they are recreated at the next sampling if the configuration has not changed).
6. **Old data:** orders created before migration 00006 have `price` NULL and `surge_multiplier` 1.0. Old `order.created` messages (schema v2) are read with `surgeMultiplier` 1.0.

## Routing

**Alert:** `PricingRoutingFailOpen` (ticket: `pricing_routing_fail_open_total{reason="timeout"|"error"}` has been increasing for 10 minutes).

Pricing asks Routing Service for the distance of each quote and **never blocks a quote on it**: when Routing is down or slow (300 ms), the fare is computed on the straight-line distance,
which is shorter than the road distance, so **fares are lower than they should be** while this lasts, and the Rider App's distance and ETA (which call Routing directly) do not show.
Bookings and matching are not affected. Routing has no Kafka, database or Redis: the usual causes are the pods, the network or tokens.

1. **Which reason?**
   ```bash
   kubectl -n grab-mini port-forward deploy/pricing-service 9090 &
   curl -s localhost:9090/metrics | grep pricing_routing        # fail_open_total by reason, and the duration histogram
   ```
   `timeout`: Routing is up but slower than 300 ms (look at its CPU and `http_request_duration_seconds` for `GET /route`). `error`: it is down, refusing connections, or answering 401/429/5xx.
   `disabled` means `ROUTING_URL` is empty or the request had no token (not a fault, never alerts). `unsupported` is a trip above 200 km (422), also not a fault.
2. **Is Routing running?** `kubectl -n grab-mini get pods -l app.kubernetes.io/name=routing-service`, then its `/readyz` through the admin port (see [Pods not ready](#pods-not-ready)).
   It has no dependency, so `/readyz` fails only while it drains. Check `ROUTING_URL` in the `grab-mini-config` ConfigMap (`http://routing-service:3009`).
3. **Can Pricing reach it?** The NetworkPolicies `allow-egress-pricing-service-to-routing-service` and `allow-ingress-routing-service-from-pricing-service` must exist.
   From a pricing pod: a request to `$ROUTING_URL/health` should answer `{"status":"ok"}`.
4. **401s:** Routing verifies the rider's own token with the JWKS (`JWT_JWKS_URL`); if the identity provider is unreachable from Routing the error is a 401 for everyone. See the egress policy `allow-egress-jwks`.
   **429s:** a single caller is over 10 requests a second (see `http_requests_total{route="GET /route",code="429"}`). `routing_refused_total{reason}` counts the 400s and 422s.
5. **Overloaded?** It scales on CPU (2 to 12 pods); `kubectl -n grab-mini get scaledobject routing-service`. The model is a few arithmetic operations, so sustained saturation means a loop of requests: look at who calls (`routing_routes_total`).
6. **Switching it off on purpose:** set `ROUTING_URL` to an empty string for pricing-service and restart; quotes then use the straight line, the counter rises with `reason="disabled"` and nothing alerts.
7. **The numbers look wrong, not missing:** the model is `detour-1.3`, an estimate (no map, no traffic): see [production-design §17](../specs/ride-booking-mini/production-design.md). It is not a fault to fix here.

## Redis failover

**Alerts:** `RedisNotExactlyOneMaster` (0 masters for more than 1 minute: failover did not complete; 2 masters: split brain), `RedisMemoryNearLimit`.

Redis runs as the `redis` StatefulSet (3 pods, each pod: `redis` + `sentinel` + `exporter`; see [disaster-recovery.md](disaster-recovery.md#0-redis-sentinel-losing-a-node-not-a-region)).
Services find the master through Sentinel (`REDIS_SENTINEL_ADDRS`), so a failover **does not require** restarting services; there are only a few seconds of errors, which consumers retry.

1. **Who is the master?** (the password is in the `grab-mini-redis` Secret, key `password`)
   ```bash
   P=$(kubectl -n grab-mini get secret grab-mini-redis -o jsonpath='{.data.password}' | base64 -d)
   for i in 0 1 2; do echo -n "redis-$i: "; kubectl -n grab-mini exec redis-$i -c redis -- env REDISCLI_AUTH="$P" redis-cli role | head -1; done
   kubectl -n grab-mini exec redis-0 -c sentinel -- env REDISCLI_AUTH="$P" redis-cli -p 26379 sentinel get-master-addr-by-name mymaster
   kubectl -n grab-mini exec redis-0 -c sentinel -- env REDISCLI_AUTH="$P" redis-cli -p 26379 sentinel ckquorum mymaster
   ```
   `ckquorum` must return `OK`. If not, a majority of Sentinels is down: see which pods are `NotReady` (`kubectl get pods -l app.kubernetes.io/name=redis -o wide`),
   and whether the pods are on the same node/zone (anti-affinity requires different nodes, topologySpread requires different zones).
2. **0 masters:** Sentinel needs `down-after-milliseconds` (5 s) and then an election. Longer than a minute: `kubectl logs redis-0 -c sentinel` (`+sdown`, `+odown`, `-failover-abort-*`).
   Usually quorum is lost (2/3 Sentinels). Restore the lost pods; **do not** run `SLAVEOF NO ONE` on a node by hand unless you are sure all three Sentinels are gone.
3. **2 masters (split brain):** data written to the old master **will be lost** when it is demoted. Find the master Sentinel recognizes (step 1), and `kubectl delete pod` the other one:
   on restart, `redis-start.sh` asks Sentinel and joins as a replica. Driver locations heal themselves within a few seconds; leases/`match:<orderId>` written during the split-brain window may be lost.
4. **Planned failover:** `kubectl drain` or a rollout runs `preStop` (`redis-prestop.sh`): the master asks Sentinel to hand over the role before shutting down (measured at ~12 s). The `redis` PDB allows only 1 pod to be down at a time.
5. **Memory nearly full:** `maxmemory` 400 MB, policy `noeviction`: when full, **writes fail** (leases are not silently evicted). Common cause: keys with no TTL
   (see Slow matching, and "No TTL for locations" below). `redis-cli --bigkeys`, `INFO memory`. Increase `maxmemory` and `limits.memory` in `deploy/k8s/generate.py` (keep `limits` ≥ 1.8x `maxmemory` for AOF rewrite).
6. **A Redis pod does not come up after losing its PVC / a corrupt AOF:** Redis data is not critical (it heals itself): delete that pod's PVC
   (`kubectl delete pvc data-redis-N`, then delete the pod) so it resyncs from the master. **Do not** do this to the current master.
7. **Rotating the Redis password:** change the `grab-mini-redis` Secret, then restart the StatefulSet (one pod at a time, `kubectl rollout restart sts/redis`) **and** every service that uses Redis
   (the password is only read at startup). There is a window in which services cannot connect; do it during off-peak hours.

Dev (`docker-compose.yml`) is still a single Redis with no password, and `REDIS_SENTINEL_ADDRS` is empty.

## Backup and restore

**Alerts:** `PostgresBackupStale` (no successful backup for 24 hours; the job runs every 6 hours), `DataOpsJobFailed` (a `backup-*` or `retention-*` job failed).

Context: the `backup-<service>` CronJobs (every 6 hours) run `pg_dump -Fc` and push the result to `BACKUP_TARGET` (ConfigMap `grab-mini-data-ops`);
`retention-order-orders` and `retention-trip-trips` (daily at 03:43) delete finished orders/trips older than `RETENTION_DAYS` (180). RPO/RTO figures: [disaster-recovery.md](disaster-recovery.md#1-postgres).

1. **A job failed:** `kubectl -n grab-mini get jobs | grep -E 'backup|retention'`, `kubectl -n grab-mini logs job/<name> -c dump` (or `-c upload`, `-c retention`).
   - `dump` failed: cannot connect to the DB (Secret `grab-mini-db-<service>`), or `pg_dump` is an older version than the server (image `postgres:16-alpine` in `generate.py`; upgrade it along with the server).
   - `upload` failed: the `grab-mini-backup` Secret (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), `BACKUP_TARGET` still set to `s3://CHANGE-ME-...`, write permission on the bucket, or a NetworkPolicy blocking egress to S3.
   - Run it again now: `kubectl -n grab-mini create job --from=cronjob/backup-order-service backup-order-manual-$(date +%s)`.
2. **Restore drill (every quarter, and after every change to the backup job).** You need a *scratch* Postgres (not production) and `psql`, `pg_restore` (version ≥ server), and the AWS CLI:
   ```bash
   export BACKUP_TARGET=s3://<bucket>/postgres   # as in ConfigMap grab-mini-data-ops
   export SCRATCH_URL=postgres://admin:...@scratch-host:5432/postgres
   for s in order-service trip-service rating-service; do
     BACKUP_NAME=$s scripts/pg-restore-drill.sh || echo "DRILL FAILED: $s"
   done
   ```
   The script fetches the latest backup, restores it into a `restore_drill_*` DB (dropped automatically), and checks: the archive is readable, `pg_restore` reports no errors, `schema_migrations_<service>` is at the latest
   version in the code, all of the service's tables exist, the main table has ≥ `MIN_ROWS` rows; it prints row counts and timings. Set `SOURCE_URL` to print the live DB's row counts alongside.
   **Record the timing and size in the drill ticket**: that is the only real RTO figure you have.
3. **Real restore** (data loss): (a) stop writes: scale the service to 0 (or pause KEDA); (b) create a new DB, `pg_restore --no-owner --dbname=<url> <file>` from the latest backup
   (or the managed service's PITR, which is preferable because it loses less data); (c) if the backup is older than the current migration, run the `<service>-migrate` Job;
   (d) point the `grab-mini-db-<service>` Secret at the new DB and bring the service back; (e) reconcile as in step 9 of the failover procedure.
   Data from the time of the dump to the time of the failure **is lost** (except what is still in Kafka's 7 days and can be replayed: see "Replay a topic"; but idempotent consumers only help with events that had already been committed to the outbox).
4. **Previewing retention / accidental deletion:** the CronJob has no dry-run mode; from a machine with `psql`, run
   `DRY_RUN=1 DATABASE_URL=... sh scripts/pg-retention.sh orders` (or `trips`) to count the rows that would be deleted. Deleted rows can only be recovered from a backup
   (restore into a scratch DB as in step 2, then `COPY` them out). To keep data longer: increase `HISTORY_RETENTION_DAYS` in `deploy/k8s/generate.py` (not below 30; the script refuses).
5. **Removing old backups** is the bucket's lifecycle rule (for example 35 days), not the job's. Give the job's credential write permission without delete permission:
   a compromised job then cannot delete the backups.

## Payments

**Alerts:** `PaymentConsumerLagHigh` (ticket), `PaymentDeadLettered` (page), `LedgerUnbalancedTransactionRejected` (page).
**Meaning:** Payment Service is not keeping up with, or cannot process, an `order.created` / `trip.status.changed` /
`trip.rated` event. Consequence: a trip **has not been charged yet** (fare, tip or cancellation fee), or the driver has not been credited yet.
No money is charged incorrectly: each charge is a single `payments` row per `(trip_id, kind)` and one balanced ledger
transaction, so reprocessing is always safe.

**Check in this order:**

1. **Lag:** see [Consumer lag](#consumer-lag), group `payment-service-group` (one group reads all four topics). The replica count is capped
   by the partition count.
2. **Dead letters:** `PaymentDeadLettered` has a `topic` label. Inspect the contents with `dlq-redrive -topic <topic> -dry-run`.
   Two common causes:
   - `no price recorded yet for order ...`: `trip.status.changed` arrived before `order.created` was recorded (they are two different topics,
     so there is no ordering). The consumer has already retried through the retry tiers (5s, 1 minute, 10 minutes). If it still fails, `order.created`
     really is missing (it was dead-lettered, or the order was created before Payment Service existed): look at `order.created.DLQ`, redrive it
     **first**, then redrive `trip.status.changed`.
   - `invalid payment event` / `invalid amount`: the event can never be charged (missing driver, negative tip, a currency other than VND).
     Redriving is pointless; fix the sender.
3. **Payment provider** errors or timeouts: `payment_provider_errors_total` increases and the event goes down the retry path. The idempotency
   keys (`fare:<tripId>`, `tip:<tripId>`, `cancellation_fee:<tripId>`) make sure a retry does not charge twice.
   Redrive once the provider is healthy again.
4. **`LedgerUnbalancedTransactionRejected`:** the code built a transaction that does not sum to 0, and it was rejected **before being written**.
   That is a bug, not a data incident. The event involved is in the DLQ; fix the code, deploy, then redrive.
5. **Check the ledger with SQL** (must return 0 rows and a total of 0):
   ```sql
   SELECT txn_id, SUM(amount) FROM ledger_entries GROUP BY txn_id HAVING SUM(amount) <> 0;
   SELECT COALESCE(SUM(amount), 0) FROM ledger_entries;
   ```
6. **A trip with no payment?** `SELECT * FROM payments WHERE trip_id = '...'`. No rows and nothing in the DLQ:
   check `payments_skipped_total{reason}` (order with no price, free trip, cancelled by the driver, `CANCELLATION_FEE_VND=0`, a rider cancel within `CANCELLATION_GRACE_SECONDS` of the assignment, or `no_assignment`: the order's `order.matched` was never seen).
   A `DECLINED` row means the provider declined the rider's card; that row has no ledger entries and the system **does not
   retry it automatically** (collecting the debt is the operator's job).

**Do not:** edit or delete `ledger_*` (a trigger blocks it; the ledger is append-only). To correct a mistake, add a reversing transaction.

## Promotions

**Alerts:** `PromotionConsumerLagHigh` (ticket), `PromotionDeadLettered` (ticket), `PromotionRedeemedOverLimit` (ticket), `PromoTokensRefused` (ticket).
**Meaning:** promotions touch pricing, but they fail softly. Validation is a read; a booking never calls Promotion Service (Order Service verifies the signed token itself), so
**Promotion Service being down only stops riders from applying a new code**, it never blocks a booking, and an order that already carries a discount is charged net by Payment Service regardless.
What can go wrong is bookkeeping: a redemption recorded late or not at all, so a limit is enforced late.

1. **Validation fails (riders cannot apply a code).** Look at the `/readyz` of promotion-service (see [Pods not ready](#pods-not-ready)); the 4xx answers are not faults, they carry a `code`:
   `not_found` (also a disabled code), `not_started`, `expired`, `min_fare`, `wrong_vehicle`, `limit_reached`, `already_used`. `promotion_validations_total{result}` counts each.
2. **`PromotionConsumerLagHigh`:** see [Consumer lag](#consumer-lag), group `promotion-service-group` (one group reads `order.created`, `order.cancelled`, `trip.status.changed` and `order.no_driver_found`). While it lags, redemptions are counted too low, so
   validation lets more riders through a limit (they will show as `OVER_LIMIT` once recorded). It is safe to wait for it to catch up.
3. **`PromotionDeadLettered`:** `topic` says which event. An order has its discount and was charged net, but the redemption was not recorded (or not released). Inspect with `dlq-redrive -topic <topic> -dry-run`. Common causes:
   - `order booked with unknown promotion "CODE"`: Promotion Service has no such code. The usual reason is a different `PROMO_SECRET` or database between environments, or promotions restored from an older backup. Re-create the promotion with the same code, then redrive.
   - `invalid promotion event`: the event is malformed (no `orderId` or `riderId`, a negative discount); redriving is pointless, fix the sender.
   Redriving is safe: the redemption is unique per `(code, order_id)` and the event is deduplicated by `eventId`.
4. **`PromotionRedeemedOverLimit`:** more than 10 orders in an hour were recorded past a limit. A few are expected (two orders in flight can both pass validation), a stream is not. `reason` says which limit.
   ```sql
   SELECT code, over_limit_reason, count(*) FROM redemptions WHERE status = 'OVER_LIMIT' AND redeemed_at > now() - interval '1 hour' GROUP BY 1, 2 ORDER BY 3 DESC;
   SELECT rider_id, count(*) FROM redemptions WHERE code = 'WELCOME10' GROUP BY 1 ORDER BY 2 DESC LIMIT 10;   -- one rider hammering a code?
   ```
   If a code is being abused, disable it (`POST /promotions/{code}/disable`, admin token): new validations stop at once. Orders that already carry the discount keep it; there is no clawback.
5. **`PromoTokensRefused`:** Order Service refuses tokens as *invalid* (not merely expired or for another price: those are riders' own doing and never alert). The cause is almost always `PROMO_SECRET` differing between order-service and promotion-service
   (a rotation that reached one of them). Compare the `grab-mini-promo` Secret both Deployments mount and restart both. `orders_promo_total{result}` shows `invalid`, `expired`, `wrong_price`, `wrong_rider`, `wrong_vehicle`, `used` and `unchecked` (Redis down: the discount was still applied).
6. **Limits look wrong.** Counts are `redemptions` with `status = 'REDEEMED'`. An order that ends without a ride gives its redemption back (`RELEASED`, no longer counted) on any of `order.cancelled` (a rider cancelled a PENDING order), `trip.status.changed` with status CANCELLED
   (a trip cancelled after the match, by the rider or the driver) and `order.no_driver_found`. A redemption that is still `REDEEMED` for such an order means that event was not applied: check `PromotionDeadLettered` and `PromotionConsumerLagHigh` first (a dead-lettered release is applied when redriven).
   A release that arrives before the order's `order.created` is remembered in `promotion_cancelled_orders` and applied when the redemption is recorded. To give one back by hand:
   `UPDATE redemptions SET status = 'RELEASED', released_at = now() WHERE code = '...' AND order_id = '...';`.
7. **Reading the money.** A promo order's payment is `price - discount` (`order_prices.discount` in Payment Service); `payments_skipped_total{reason="zero_amount"}` counts free rides. Rotating `PROMO_SECRET` invalidates only tokens issued in the last 10 minutes.

## Chat

**Alerts:** `ChatConsumerLagHigh` (ticket), `ChatDeadLettered` (ticket), `ChatPushDeadLettered` (ticket), `ChatRateLimitedSpike` (ticket), `ChatTripLookupFailing` (ticket).
**Meaning:** chat is a convenience, not part of the booking: nothing else waits for it, so none of these pages. Chat Service learns who may talk from `trip.status.changed` and, for a trip with no event yet (assigned, not picked up: Trip Service publishes nothing at creation), by asking Trip Service with the caller's token. So what can go wrong is a trip it has not learned (or learned late), a push that did not go out, or abuse.
**Messages are personal data.** Do not paste message text into tickets, chat or dashboards, and do not log it; the service logs ids and lengths only. The text is also in Kafka (7 days), the DLQ and retry topics, `outbox_dead` and the backups: see [production-design §19](../specs/ride-booking-mini/production-design.md).

1. **A rider or driver gets "no such trip" (404 `unknown_trip`) when opening the chat.** Trip Service itself answered 404 to the lookup (`chat_trip_lookups_total{result="not_found"}`): the trip id is wrong, or the caller is not on it (Trip Service shows a trip only to its rider and driver). Check `GET /trips/{id}` with the same token. A trip is also learned from its events (`ChatConsumerLagHigh`, `chat_trip_events_total{result}`).
   **503 `trip_lookup_unavailable`:** the trip is not in `chat_trips` yet and Trip Service could not answer within `TRIP_LOOKUP_TIMEOUT_MS` (default 500) or answered an error: it is down, slow, or the network policy chat-service to trip-service is missing. `chat_trip_lookups_total{result="error"}` rises and `ChatTripLookupFailing` opens after 10 minutes; the log line `lookup in trip service failed` names the trip id and the error (never a token). Trips chat already knows are not affected, and the app can retry. Fix Trip Service (see its section); a wrong `TRIP_SERVICE_URL` also shows here. `found` / `forbidden` are normal.
   ```sql
   SELECT trip_id, rider_id, driver_id, status, updated_at FROM chat_trips WHERE trip_id = '...';
   ```
   A trip with an empty `rider_id` comes from events written before they carried the rider: the driver's send is 409 `no_recipient` until a later event names the rider.
2. **403 `not_participant` for someone who should be on the trip.** The token's subject differs from `rider_id` / `driver_id` in `chat_trips` (a driver token carrying a rider's id is refused too). Compare them; do not edit the row unless the trip event itself was wrong.
3. **409 `chat_closed` too early or too late.** The chat closes `CHAT_GRACE_MINUTES` (default 10) after the trip ended, measured from the end event's own time. `SELECT status, updated_at FROM chat_trips ...` shows what the service holds; a consumer that lags more than the grace period makes an ended trip look closed as soon as it is learned. History stays readable.
4. **`ChatConsumerLagHigh`:** see [Consumer lag](#consumer-lag), group `chat-service-group` (it reads `trip.status.changed` only). Safe to wait for: events are applied in any order and twice, and nothing is lost.
5. **`ChatDeadLettered`:** a trip event could not be applied (no `tripId` or `driverId`, or an `eventId` that is not a UUID). Inspect with `dlq-redrive -topic trip.status.changed -dry-run`; an event that is malformed will not succeed by redriving, fix the sender. The DLQ topic holds trip events, not message text.
6. **`ChatPushDeadLettered`:** Notification Service could not push chat messages (the dead letters are `chat.message.sent` in `notification-service-group`), almost always because Redis was unreachable ([Redis failover](#redis-failover)). The messages are saved: the other person sees them when they open the chat. Redriving (`dlq-redrive -topic chat.message.sent`) is optional and pushes old messages as if new; the DLQ **holds message text**, so let it expire rather than keep it.
7. **`ChatRateLimitedSpike`:** more than 100 sends in 10 minutes were refused with 429. The limit is per sender (20 a minute by default, burst 5) and per instance. The service does not log each refusal (that would let a flood fill the logs); find the sender in the gateway or ingress access logs by the 429s on `POST /trips/*/messages`, and look for a client retrying in a loop. `chat_messages_rejected_total{reason}` splits the refusals (`rate_limited`, `chat_closed`, `invalid_text`, `not_participant`, `unknown_trip`, `trip_lookup_unavailable`, ...). To loosen the limit, set `CHAT_RATE_PER_MINUTE` and restart.
8. **Messages are not reaching the other person.** The message is in `chat_messages` and in the outbox (`outbox_oldest_pending_age_seconds{source="chat-service"}`, see [Outbox backlog](#outbox-backlog)) before it is published; then Notification Service pushes it only to a recipient with an open socket (`notification_pushes_total`). With no socket the app shows it when the chat is opened or reloaded.
9. **Retention.** Messages older than `CHAT_RETENTION_DAYS` (default 30) and ended trips that old are deleted hourly by every replica (`chat_pruned_total{table}`, and a log line with counts). To delete sooner for a legal request, `DELETE FROM chat_messages WHERE trip_id = '...'` is safe (history and the cursor just end); the copies listed above are not touched by it.

## Driver approval and suspension

**Alerts:** `DriverReviewConsumerLagHigh` (ticket), `DriverReviewDeadLettered` (page). Also `OutboxBacklog` with `source="profile-service"` and `HTTPServerErrorsHigh` for the API.
**Meaning:** an admin's decision on a driver reaches Matching Service as `driver.application.reviewed` (group `matching-service-driver-review-group`), which keeps who is eligible in Redis. If it does not arrive, or is not applied, an **approved driver gets no trips** or a **suspended driver still gets trips**. The second is the one that matters. Profile Service consumes nothing.
**Personal data.** Names, phones and plates are in Profile Service's Postgres only (and its backups). Do not paste them into tickets or chat, and do not look for them in logs, Kafka or Redis: they are not there, only driver ids, statuses and the admin's reason. See [production-design §20](../specs/ride-booking-mini/production-design.md).

What Matching holds for a driver (`redis-cli`):
```bash
redis-cli MGET driver:driver-7:status driver:driver-7:vehicle    # AVAILABLE | ASSIGNED | ON_TRIP | OFFLINE | (nil), and CAR | BIKE
redis-cli EXISTS driver:driver-7:suspended                       # 1 = out of service: every claim refuses them
redis-cli GET driver:driver-7:review_at                          # microseconds of the last result applied (date -r $((N/1000000)))
```
What Profile Service holds (its Postgres; no names or plates needed to answer this):
```sql
SELECT driver_id, status, vehicle_type, reviewed_by, reviewed_at, status_at FROM driver_profiles WHERE driver_id = 'driver-7';
SELECT id, topic, created_at FROM outbox WHERE source = 'profile-service' ORDER BY id;   -- not yet published
```

### A driver is approved but gets no trips

1. **Is the profile `APPROVED`?** (SQL above.) `PENDING` means nobody approved yet; a vehicle change by an approved driver puts them back to `PENDING` on purpose (the reason says so).
2. **Did the event leave Profile Service?** `outbox_oldest_pending_age_seconds{source="profile-service"}` and the query above: a row that stays means the relay is stuck ([Outbox backlog](#outbox-backlog)).
3. **Did Matching apply it?** Lag of `matching-service-driver-review-group` ([Consumer lag](#consumer-lag)), `matching_driver_reviews_total{result}` (`available`, `approved_busy`, `stale`...) and the Matching log line `driver=<id> review APPROVED (event <id>): <result>`:
   - `available`: they are `AVAILABLE` now. They also need a **location** (`driver.location.updated` from the Driver App) inside the match radius: approval does not give a position.
   - `approved_busy`: they were `ASSIGNED` or `ON_TRIP`; they are `AVAILABLE` when that ends.
   - `stale`: a newer result was applied first. Compare `driver:<id>:review_at` with the event's `occurredAt`; if the newer one is a suspension nobody meant, approve again in the Ops dashboard (a new, later result).
4. **Is the driver already `AVAILABLE` with the right vehicle?** `MGET` above. A driver with a different vehicle type than the order is correctly skipped.
5. **Last resort, to apply the approval by hand** (only if the profile is `APPROVED` and the driver has no trip): `redis-cli DEL driver:driver-7:suspended`, `redis-cli SET driver:driver-7:vehicle CAR`, `redis-cli SET driver:driver-7:status AVAILABLE`.

### A suspended driver still gets trips

1. **Is the profile `SUSPENDED` (or `REJECTED`, or `PENDING` after a vehicle change)?** If it is `APPROVED`, the suspension never happened: check the admin's click went through (409 `invalid_transition` means the driver was not `APPROVED`).
2. **Is `driver:<id>:suspended` set?** If not, the event has not been applied: see lag and `DriverReviewDeadLettered` below. **Stop the trips now** with `redis-cli SET driver:driver-7:suspended 1` and, if `driver:driver-7:status` is `AVAILABLE`, `redis-cli SET driver:driver-7:status OFFLINE`. The claim scripts read the key on every claim, so this takes effect at once.
3. **The key is set and the driver still has a trip.** A suspension never cancels a trip in progress: the driver finishes the one they are on and is then `OFFLINE` (`driver-7` released to `AVAILABLE` records `OFFLINE`). To take them off the road immediately, cancel their trip as the operator (Trip Service), do not edit Redis.
4. **A different service sets the status.** Only `pkg/driverstatus` honours the key. Anything that writes `driver:<id>:status` itself (a script, a new service) can bypass it: put the key back and find the writer.

### Alerts

- **`DriverReviewConsumerLagHigh`:** more than 10 results waiting for 5 minutes on a topic that normally has none: the consumer is stuck. Look at the Matching pods, Redis ([Redis failover](#redis-failover)) and the handler errors (`kafka_consumer_messages_total{group="matching-service-driver-review-group",result="retried"}`). Applying is idempotent and safe out of order, so waiting is safe; what it costs is described above. Until it clears, apply urgent suspensions by hand (step 2 above).
- **`DriverReviewDeadLettered` (page):** a result could not be applied after its retries: Matching rejects only results that can never succeed (no `driverId`, a status that is not `APPROVED`, `REJECTED` or `SUSPENDED`, an `APPROVED` without a vehicle type, an `occurredAt` that is not a time); anything else is retried first (Redis down). Inspect with `dlq-redrive -topic driver.application.reviewed -dry-run` (the DLQ holds ids, the status and the reason, no name, phone or plate). **Apply the decision by hand first** (the two procedures above), then fix the sender and redrive only if the event is now valid; redriving a result older than a later one is harmless (it is ignored as `stale`).
- **`OutboxBacklog` for `profile-service`:** decisions are saved but not published, so nothing downstream changes. See [Outbox backlog](#outbox-backlog).

### Rebuild Matching's view after Redis lost its data

**When:** after a Redis restore, a flush, or a failover to a replica that missed writes. Those lose `driver:<id>:suspended`, `:status`, `:vehicle` and `:review_at`: suspended drivers may no longer be blocked and approved drivers are not `AVAILABLE`. Nothing does this by itself: **run the resync as soon as Redis is back**, suspensions are the urgent part.

**How:** Profile Service re-publishes the current status of every `APPROVED`, `REJECTED` or `SUSPENDED` driver (`PENDING` and drivers without an application emit nothing) as `driver.application.reviewed`, `reviewedBy` `resync`, without changing any profile. Either with an admin token:
```bash
curl -sS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" https://<host>/driver-applications/resync                       # every driver
curl -sS -X POST -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' -d '{"driverId":"driver-7"}' https://<host>/driver-applications/resync   # one driver
# {"queued":412,"counts":{"APPROVED":390,"REJECTED":9,"SUSPENDED":13},"skipped":0}
```
or, without a token, from a pod or a machine that can reach Profile Service's Postgres and the Schema Registry (same `DATABASE_URL`/`SCHEMA_REGISTRY_URL` as the service; it only queues the events, the running service publishes them): `kubectl -n grab-mini exec deploy/profile-service -- /app resync [--driver driver-7]` (the image has no shell and the binary is `/app`; locally `go run ./services/profile-service resync`) (use the CLI for a very large fleet: the HTTP response must finish within the server's 30 s write timeout, although the run itself does not stop). A second resync while one runs is refused (409 `resync_running`; the command exits with an error): wait and repeat if unsure. It is safe to run again: Matching applies the same state.

**Verify:** `profile_resync_events_total{status}` and `profile_resync_runs_total{result="completed"}` increased; `outbox_oldest_pending_age_seconds{source="profile-service"}` returns to 0; the lag of `matching-service-driver-review-group` drains ([Consumer lag](#consumer-lag)); then for a sample driver `redis-cli MGET driver:driver-7:status driver:driver-7:vehicle` (`AVAILABLE` and `CAR` for an approved one, `OFFLINE` for a suspended or rejected one) and `redis-cli EXISTS driver:driver-7:suspended` (1 for those two); and the public view `GET /drivers/driver-7/public` (200 only for an approved driver). `matching_driver_reviews_total{result}` shows `available`, `offline`, `approved_busy`, `after_trip`; `stale` for a driver whose Redis kept a newer result.

**Limits:** the resync restores *decisions*. A driver on a trip whose `ON_TRIP` was lost is made `AVAILABLE` (nothing here knows about trips) and may be offered another order; Trip Service's own release later sets them free as usual. A driver who applied or was decided while Redis was down is applied once the consumer reaches the event (it retries while Redis is down); a resync run before that covers the decisions already saved. Replaying the topic ([Replay a topic](#replay-a-topic-reprocess-from-the-beginning)) also works within its 7 days but is no longer the procedure.

### Profile Service errors seen by the apps

- **403 `forbidden`:** the id in the path is not the token's subject; the app is calling for someone else.
- **404 on `/drivers/<id>/public`:** the driver is not `APPROVED` (or unknown): the same answer on purpose. A rider whose driver was suspended mid-trip sees only the id until the trip ends.
- **409 `invalid_transition`:** the admin acted on a driver who is not in the right status (someone else decided first, or the driver is `PENDING` again after a vehicle change); the body has the current status.
- **429 `rate_limited` on the public view:** more than `PUBLIC_RATE_PER_MINUTE` (120) lookups a minute from one caller on one instance. `profile_requests_rejected_total{reason}` splits the refusals.
- **A driver cannot apply (422 `incomplete_profile`, 409 `suspended`):** the first needs a name, vehicle type and plate saved; the second is a decision only an admin can lift (approve).
- **Development.** `profile-service seed-dev` (run by `scripts/dev.sh`) approved `driver-1..5` and gave `rider-1..3` profiles, once; it published nothing, so these drivers are eligible because `matching-service seed-drivers` put them in Redis. Re-seeding Redis does not undo a suspension (a suspended driver is recorded `OFFLINE`).

## Pods not ready

**Alerts:** `DeploymentReplicasUnavailable`.

1. `kubectl -n grab-mini get pods`, then `kubectl -n grab-mini describe pod <pod>` (events: `Unhealthy`, `OOMKilled`, `ImagePullBackOff`).
2. Pod `Running` but `0/1 Ready`: `/readyz` (admin port) says which dependency is failing; see the top of this document.
   A service is not ready when its **dependency** is failing, and that is the correct behavior: it leaves rotation without being restarted.
   A pod is never restarted because Postgres is down (the `/livez` liveness probe depends on nothing external).
3. Rollout stuck after a deploy: `kubectl rollout status deploy/<name>`; if the new pods are not `Ready`, the old pods are kept
   (`maxUnavailable: 0`), so users are not affected. Use `kubectl rollout undo deploy/<name>` to go back, provided the
   migration that ran is backward compatible (see below).

---

## Common procedures

### Deploy

1. Build and push the images; set the tag: `kustomize edit set image ghcr.io/example/grab-mini/order-service=<registry>/order-service:<tag>` (for each service).
2. **Migrations first**: run the `*-migrate` Jobs and wait for them to finish (`kubectl wait --for=condition=complete job/order-service-migrate`).
   Migrations only go forward, and must be compatible with **both the old and the new version** of the code (add columns with a default; do not drop or rename columns
   while the old version is still running), because a rolling update runs both versions side by side.
3. `kubectl apply -k deploy/k8s/base` (or the environment's overlay). Watch `kubectl rollout status`.
4. Changing the **Avro schema**: only add fields with defaults (the registry runs `BACKWARD`). Register the schema in CI before deploying.

### Replay a topic (reprocess from the beginning)

This is only possible because every consumer is idempotent (developer guide: "Replay a topic and see nothing counted twice"). Pause KEDA first,
otherwise it will scale back up:
```bash
kubectl -n grab-mini annotate scaledobject/rating-service autoscaling.keda.sh/paused-replicas=0 --overwrite
kubectl -n grab-mini scale deploy/rating-service --replicas=0
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --command-config ops.properties \
  --group rating-service-group --reset-offsets --to-earliest --topic trip.rated --execute
kubectl -n grab-mini annotate scaledobject/rating-service autoscaling.keda.sh/paused-replicas-   # remove the pause
```
Do not replay `driver.location.updated` (old locations are discarded because of `timestamp`) or any topic with non-idempotent side effects.

### Rotate secrets

- **A service's Kafka password:** delete the Secret Strimzi created for that `KafkaUser` so it is regenerated, then restart the service's pods
  (the password is only read at startup). Do one service at a time.
- **Token signing key (Ed25519):** only the issuer holds the private key. Rotating it does not break live tokens: give the issuer the new
  key and keep the old public key in `JWT_PREVIOUS_PUBLIC_KEYS` until the last token signed with it expires (`TOKEN_TTL` + 10 minutes of cache).
  Services that use JWKS need no restart. Details in [security.md](security.md).
- **Quote and promo secrets (`QUOTE_SECRET`, `PROMO_SECRET`):** each is shared by two services (Pricing and Order; Promotion and Order). Change the Secret (`grab-mini-quote`, `grab-mini-promo`) and restart **both** services together; tokens and quotes issued
  before the switch stop verifying (at most 60 seconds of quotes, 10 minutes of promo tokens), and riders just ask again. Until both have restarted, bookings carrying one are refused (400), which is `PromoTokensRefused` for promo tokens.
- **Schema Registry password:** change the `grab-mini-schema-registry` Secret, then restart (it is read at startup).
- **Postgres password:** change it in the `grab-mini-db-<service>` Secret, then restart; the service reads `DATABASE_URL` at startup. The `backup-*`/`retention-*`
  CronJobs read the same Secret on every run, so they need nothing.
- **Redis password:** see [Redis failover](#redis-failover), step 7.

### Manual scaling

KEDA controls the replica count; `kubectl scale` is overwritten by KEDA. Change `minReplicaCount`/`maxReplicaCount` in the `ScaledObject`
(via `deploy/k8s/generate.py`), or pause it temporarily with the `autoscaling.keda.sh/paused-replicas` annotation as above.
The useful replica count of a consumer does not exceed the partition count of the topic it reads.

### What is missing (to know when on call)

- **No outbox for Matching and Location Gateway**: if Kafka fails at exactly the wrong moment, a match may not be announced
  (there is already a lease so the driver does not get stuck, and the `order.created` message goes through retry), while driver locations heal themselves within a few seconds.
- **No TTL for locations in Redis GEO** (see Slow matching).
- **Notification Service is best-effort**: a message sent while nobody is subscribed is lost; the app recovers over REST after reconnecting.
- **Backup and DR**: see [disaster-recovery.md](disaster-recovery.md). Postgres backup and the restore drill have been run against a real Postgres on a dev machine
  (`scripts/test-data-ops.sh`), but **not on a real cluster**: the CronJob has not been run by a kubelet, no backup has gone to a real S3, and there has been no drill with production-sized data.
  The failover manifests (MirrorMaker 2, S3 archive) have still not been tried.

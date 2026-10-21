# Backup and disaster recovery

Scope: loss of a region, or loss of data. Losing a single broker or a single zone is already covered by the 3-broker, 3-zone cluster (see the
"Kafka broker down" runbook); this document does not repeat that.

## How much each kind of data matters

| Data | Lives in | If lost | Protection |
|---|---|---|---|
| Orders, trips, ratings, outbox | Postgres (order, trip, rating) | **Truly lost**: it is the source of truth | Postgres backups + PITR (below) |
| Business events (`order.*`, `trip.*`) | Kafka, retained 7 days | Committed data can be rebuilt from Postgres; a consumer more than 7 days behind loses events | MirrorMaker 2 to the standby region; S3 archive |
| Driver location, driver status, leases | Redis (one master + replica, monitored by Sentinel) | **Self-healing**: drivers report again every few seconds; when a lease expires the driver can receive orders again | No backup needed |
| Avro schemas | Schema Registry (Kafka topic `_schemas`) | New producers/consumers cannot decode messages | Lives in Kafka: replicated by MirrorMaker 2 if `_schemas` is added to `topicsPattern`; or re-register from `services/pkg/events/schemas/` (services register themselves on startup) |

Proposed targets (adjust to the real contract): **RPO** (maximum data loss) of a few seconds for Postgres with a synchronous replica/PITR, and tens of seconds for
Kafka to the standby region (MirrorMaker 2 replicates asynchronously); **RTO** (recovery time) of tens of minutes, most of it the manual steps below.

## 0. Redis: Sentinel (losing a node, not a region)

Redis needs no backup (see the table above) but it does need **availability**: when the master dies, Matching and the Driver Location Service stop. Every service uses
`pkg/redisconn`: set `REDIS_SENTINEL_ADDRS` (ConfigMap, a comma-separated list of Sentinels) and `REDIS_MASTER_NAME` and the client asks Sentinel for the current
master and **follows failovers on its own**, with no service restart. Leave them empty and it uses the single-node `REDIS_ADDR` as before. Passwords go through
`REDIS_PASSWORD(_FILE)` and `REDIS_SENTINEL_PASSWORD(_FILE)`.

Verified: `TestTheClientFollowsAFailover` (`pkg/redisconn`) runs against a real master, a real replica and a real Sentinel, requests
`SENTINEL FAILOVER`, waits for Sentinel to switch masters, then requires the same client to be able to write to the new master while the old master rejects writes. Not verified:
a Sentinel deployment with all 3 nodes (quorum 2), and the failover latency when the master dies abruptly (`down-after-milliseconds` + election, typically a few seconds to ten-odd seconds).
The manifests do **not** deploy Redis/Sentinel (as with Postgres, use a managed service or an operator); only the client-side configuration is in the repo.

Sentinel adds availability, not capacity: there is still a single master. Replicas replicate asynchronously, so a failover can lose the last few seconds of writes
(location heals itself; but a lease or a `match:<orderId>` written last may also be lost, and the consequences of that, for example an order being matched twice, are **untested**).

## 1. Postgres

Do not self-manage this with a `pg_dump` CronJob: use a managed service (RDS/Cloud SQL/...) with **automated backups + PITR + a read replica in another region** enabled, or
an operator (CloudNativePG, Crunchy) with `barman`/`pgBackRest` shipping WAL to S3. A daily `pg_dump` is only a second layer of insurance and does not give PITR.

**Restore drills** are mandatory every quarter: build a database from a backup, run `order-service migrate` to make sure the schema matches, and check row counts. A backup
that has never been test-restored is not a backup.

Data cleanup: `deploy/k8s/base/retention/` has two CronJobs that delete dedupe rows older than 30 days (`processed_events` for trip, `rating_events` for
rating) in batches of 10,000 rows. 30 days is a lower bound: Kafka retains 7 days and a DLQ may wait on a human; below that, a late redrive could
be processed twice. The `orders` and `trips` tables are never deleted automatically: archiving them is a product and legal decision. `outbox` rows delete themselves once published;
`outbox_dead` is kept for a human to inspect (see the runbook) and cleaned up by hand after handling.

## 2. Kafka to the standby region (`deploy/k8s/dr/mirrormaker2.yaml`)

Apply it in the cluster of the **standby region**. It replicates `order.*` and `trip.*` (not `driver.location.*`: telemetry), keeps topic names unchanged
(`IdentityReplicationPolicy`) so consumers do not have to change configuration, and syncs consumer group offsets (`sync.group.offsets`).

Limitations to be aware of:

- Asynchronous: if the primary region dies, events not yet replicated are lost (RPO > 0). Postgres is the source of truth to reconcile against.
- Offsets are translated **approximately**, so consumers may reprocess a few messages just before the cutover point. The consumers are already idempotent (eventId,
  `match:<orderId>`); this is exactly why they need to be idempotent.
- There is no measurement of replication lag yet, and the manifest has only had its schema validated; it has **not yet been run on a real cluster**.

## 3. Cold archive on S3 (`deploy/k8s/dr/s3-archive.yaml`)

The Kafka Connect S3 sink writes the business topics to S3 as Avro, partitioned by hour. It is independent of both Kafka clusters and is meant for auditing and rebuilding
read models. It has also not yet been run on a real cluster; the S3 sink plugin is under the Confluent Community License.

## 4. Failover procedure

Only when the primary region will not recover soon. Each step should be rehearsed beforehand:

1. **Decide and announce.** Failing over has a cost (some unreplicated events are lost, offsets are approximate): it is decided by someone with the authority to do so.
2. **Stop writes in the primary region if it is still reachable** (scale to 0), to avoid both regions accepting orders.
3. **Postgres**: promote the replica in the standby region (or restore from PITR to the most recent point). Record the timestamp.
4. **Kafka**: check how far MirrorMaker 2 has caught up (the connector's `kafka_consumer_lag`, or compare end offsets); stop MirrorMaker 2.
5. **Schema Registry**: make sure all schemas are present (the `_schemas` topic was replicated, or re-register them).
6. **Redis**: bring up a fresh, empty instance. Drivers will report their locations again on their own; for the first few seconds Matching may return `NO_DRIVER`.
7. **Start the services** in the standby region (`kubectl apply -k deploy/k8s/base`, replace `DATABASE_URL`, set the Kafka bootstrap through the ConfigMap). Run
   `migrate` first.
8. **Switch DNS/gateway** to the standby region.
9. **Once stable**: reconcile the orders in Postgres against the Kafka events; `PENDING` orders that are too old need handling by hand (cancel or re-match). Run `dlq-redrive`
   if any dead letters appeared.

Returning to the primary region (failback) is a failover in the opposite direction: do it calmly, follow the steps above in reverse, and make sure the data in the two
regions has not diverged.

## Not done yet

- No failover or restore drill has been run; every RPO/RTO above is a target, not a measurement.
- Redis has no backup (deliberately) and the manifests do not deploy Redis/Sentinel; Redis Cluster cannot be used (see docs/performance.md).
- Multi-region active-active: not done. Two regions both writing to Postgres and both matching drivers would need a separate design for conflicts.

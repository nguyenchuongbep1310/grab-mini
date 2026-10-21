# What breaks at 10x

An honest estimate, not a benchmark. **Nothing here has been run on real infrastructure.** What I have measured is in [performance.md](performance.md), on one 4-core machine that also ran every service, Kafka, Postgres, Redis and the load generator, so its absolute numbers say little about production hardware.

Two baselines are used below:

- **Design assumptions** (from [production-design.md](../specs/ride-booking-mini/production-design.md)): about 500 orders/s and 25,000 driver location reports/s. 10x is 5,000 orders/s and 250,000 locations/s.
- **Measured on the single box**: ~250 orders/s matched 100% with p99 0.1 s; ~5,000 location messages/s through one consumer; at 400 orders/s the machine, not Matching, saturated.

The list is ordered by what I expect to hurt first. Each item says why, what I would measure to confirm, and what I would do.

## 1. Redis: search cost and a single instance

**Why.** `GEOSEARCH` cost grows with the number of drivers inside the radius, not with total drivers (measured: 1 million drivers, 3 km radius, about 90 queries/s on one Redis core; at 100,000 drivers and 1 km, about 9,300/s). Matching, Driver Location Service, Pricing's supply sampler and every claim share one Redis. The 250,000 locations/s at 10x is `GEOADD` at roughly the measured 100,000/s per core, so location writes alone need more than a core.

**Measure.** Redis CPU, `GEOSEARCH` latency percentiles by density (city centre vs. suburbs), commands/s per service.

**Do.** Keep the expanding-ring search (already done). Split the keyspace by geographic cell across several Redis nodes; the cell sharding exists behind `GEO_SHARD_DEGREES` but is off, does not make a single query cheaper, and the scripts need hash tags before they can run on Redis Cluster. The alternative is to partition matching by city so each city has its own Redis. Moving supply sampling off the matching Redis is a cheap early win.

## 2. Order Service and Postgres

**Why.** Every order is a Postgres write plus an outbox write plus the relay's delete. At 5,000 orders/s that is more than 10,000 writes/s to one primary, and the order database also serves reads for every client reconnect. In the 400 orders/s run, Order Service was already the busiest process (about 0.8 core, handler p95 160 ms), though the machine was shared.

**Measure.** Postgres write IOPS and WAL volume, connection pool wait, p99 insert latency, `outbox` table size and dead tuples.

**Do.** Pool and batch writes, keep the outbox table narrow, tune autovacuum for the outbox table, and put reads on replicas. If one primary is still not enough, shard orders by rider or region. Postgres is not deployed by the manifests (a managed service is assumed), so failover and sizing are outside this repository.

## 3. The outbox relay: one publisher per source

**Why.** Per source, one relay publishes at a time (an advisory lock), polling every second in batches of 100. The ceiling is roughly batch size × polls per second if rows are always waiting, plus Kafka round-trip time, and it adds up to a second of latency to every event. This guarantees commit order for a key but caps throughput and makes one relay a bottleneck.

**Measure.** The existing outbox backlog gauge (age of the oldest waiting event) and its alert, relay publish latency, rows published per second.

**Do.** Cheap first: a shorter interval and larger batches. Then run one relay per hash range of the key, which keeps per-key order and removes the global limit. At that point, consider change data capture (see [ADR 0004](adr/0004-transactional-outbox.md)).

## 4. Matching parallelism and partitions

**Why.** Matching parallelism is bounded by the partition count of `order.created` (12 in the manifests). The single-box run showed 12 vs. 24 workers made no difference because the machine was saturated, so I do not know the real per-partition rate. Increasing partitions later re-maps keys and breaks per-order ordering during the change.

**Measure.** Handler time per message, consumer lag per partition, CPU per worker, on real hardware.

**Do.** Choose the partition count from measured per-worker throughput with headroom for 10x *before* it holds data. Spread the `order.created` partitions evenly over instances; watch for skewed keys (should not occur with order ids).

## 5. Notification: Redis work per location update

**Why.** Notification instances share a consumer group, so Kafka splits the location topic between them, which scales. But every `driver.location.updated` message costs a Redis routing lookup (driver to rider) and a pub/sub publish to the rider's channel and to the driver's own channel ([ADR 0008](adr/0008-notification-fan-out-and-recovery.md)), whether or not anyone is watching. At 250,000 reports/s that is hundreds of thousands of extra Redis operations per second on top of item 1, which shares the same Redis. Socket count per instance and memory per socket are untested.

**Measure.** Redis commands/s caused by Notification, pub/sub throughput, sockets per instance, memory per socket, push latency end to end.

**Do.** Skip drivers with no active trip before touching Redis, cache the driver-to-rider mapping in the instance (invalidated by trip events), and give pub/sub its own Redis so it cannot slow matching.

## 6. Location Gateway: in-memory state

**Why.** Rate limiting and the "has not moved" filter are kept in each gateway's memory, per driver. If requests for one driver reach different instances, limits are looser than configured and the stationary filter misses. State also grows with the number of drivers online and is only dropped for idle ones.

**Measure.** Gateway CPU and memory at 250,000 reports/s, the share of reports dropped as stationary, requests per instance.

**Do.** Route by driver (hash or sticky) at the load balancer, which keeps the in-memory design. Only move the state into Redis if routing by driver is not possible, accepting one more round trip per report.

## 7. Kafka itself

**Why.** At 250,000 location messages/s the location topic dominates broker network and disk. Local tests used one broker; the manifests describe a multi-broker cluster with `min.insync.replicas` 2 for transactional topics, but I have not run it. The location topic uses acks=1 and 1-day retention to keep that cost down.

**Measure.** Broker bytes in/out, request queue time, disk throughput, under-replicated partitions, producer batch size and linger (the writers use a 2 ms batch timeout, a latency choice that costs throughput).

**Do.** Raise the batch timeout for the location producers, compress, add brokers and move partitions, and keep location traffic on its own brokers if it affects order latency.

## 8. Unbounded growth

- **`processed_events`** (Trip Service) and **`rating_events`** (Rating Service) hold one row per handled event forever. There is no pruning job. Because Kafka retention is 7 days, rows older than that are not needed for dedupe, so a retention job or time-partitioned tables solve it. This is cheap and I would do it before anything above.
- **`outbox`** rows are deleted when published, but a long relay outage or a quarantined event leaves them behind; the backlog alert is the guard.
- **Redis match records** (`order:{id}:match`) expire after 24 hours.

## 9. Things that are correct but untested at scale

- **Retry topics.** A retried message ties up a worker while it waits for its delay ([ADR 0006](adr/0006-retry-tiers-and-dead-letter-topics.md)); a dependency outage at high load would put a large share of traffic through the tiers, so check worker count and retry-topic partitions.
- **Schema Registry** is a startup dependency of every producer and consumer; its availability and cache behaviour under restarts of many pods at once is untested.
- **Single region.** Cross-region replication, failover of Kafka and Postgres, and data residency are out of scope; [disaster-recovery.md](disaster-recovery.md) covers losing a node, not a region.

## What I would do first

1. Run the load test on real, separately sized infrastructure and re-measure everything in [performance.md](performance.md); until then, every number above is a hypothesis.
2. Add pruning for the dedupe tables (cheap, removes a certain future problem).
3. Cut the per-location-update Redis work in Notification (5); it shares the Redis that item 1 depends on.
4. Decide the Redis strategy (1) by measuring density, because it is the hardest to change after the data layout is in use.

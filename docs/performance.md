# Performance: what was measured, what was found

The numbers were measured with `deploy/loadtest` (see its [README](../deploy/loadtest/README.md)) on **a single 4-core machine** that was also running 9 Go processes, a single-broker Kafka, Postgres, Redis and
the load generator. Load average during the runs was 8–13. So: **the absolute numbers only say what this machine did; the value of measuring is what it uncovered.**

## Four performance bugs found (fixed)

| Symptom | Cause | Fix | Before → after |
|---|---|---|---|
| Every synchronous publish took ≥ 1 s | `kafka.Writer` defaults to `BatchTimeout` = 1 s: batching waits the full 1 s before sending | `kafkaconn.DefaultBatchTimeout` = 2 ms for every writer | 1.001 s → 10.8 ms per publish (acks=all) |
| Location consumer at only ~2,300 msg/s, lag growing | Synchronous commit of every message, sequential per reader | `Config.CommitInterval` (background commit, batched by time) | ~2,300 → ~5,000 msg/s, lag ≤ 24 |
| Matching ~50 orders/s | The consume loop processed one message at a time, sequentially | `Config.Workers` / `KAFKA_CONSUMER_WORKERS`: one worker per partition, still correctly ordered within a partition | ~50 → a few hundred/s, bounded by the machine |
| Matching spent many Redis commands when nearby drivers were already busy | One sequential `EVAL` per candidate (up to 50 round trips per order) | `driverstatus.ClaimFirstForOrder`: try the whole list in **one** script | 1 round trip per order |

Two small changes came with these: the event's `OccurredAt` is written as `RFC3339Nano` (the matching-latency metric previously rounded down to the second), and `Reader.MaxWait`
is set to 500 ms because `Reader.Close()` waits for the in-flight fetch (the default of 10 s made shutdown slow).

## Final results (12 partitions, 12 workers, 3,000 drivers reporting location every 4 s ≈ 750 reports/s)

| Orders/s placed | Matched | Matching latency p99 | Notes |
|---|---|---|---|
| 150 | 100% | 0.1 s | `deploy/loadtest/result-150.json` |
| 250 | 100% | 0.1 s | location backlog ≤ 3 messages |
| 400 | ~93%, queue growing, timeouts | 30–48 s | **machine saturated**, not Matching |

At 400 orders/s Matching used ~0.06 core and its handler p95 was ~24 ms; Order Service used ~0.8 core, the `order-service-group` handler p95 was 160 ms, and load
average was 11 on 4 cores. Raising partitions/workers from 12 to 24 did not change the result (~190 orders/s matched), meaning the limit is the machine's CPU, not the
consumer structure. **There is no measurement on real infrastructure yet**; the design targets of 500 orders/s and 25,000 locations/s have not been verified.

## Redis GEO: is sharding needed?

`GEOSEARCH ... ASC COUNT 50` scans **every driver within the radius** and only then sorts (COUNT does not stop early). The cost is proportional to the number of candidates in the
radius, not to the total number of online drivers. Measured on single-threaded Redis, 1 key:

| Drivers (in a ~30 km box) | Radius | Candidates | Queries/s (1 Redis core) |
|---|---|---|---|
| 100,000 | 1 km | ~100 | 9,300 |
| 100,000 | 3 km | ~3,300 | 1,350 |
| 1,000,000 | 1 km | ~1,100 | 820 |
| 1,000,000 | 3 km | ~31,000 | **~90** |

`GEOADD`: ~100,000/s; 1 million drivers take ~100 MB.

Conclusions:

1. **Location updates are not the problem** (25,000/s takes only ~1/4 of one core).
2. **Search is the cost**, and it depends on density × radius. Splitting the key into shards does not reduce the cost of *each query* in a
   dense city; it only spreads the load across multiple nodes.
3. The cheapest lever is **searching in expanding rings** (1 km → 3 km → the configured radius), done in `matching-service/redis.go`: most orders have
   a free driver very close by, so they only pay the cost of the small ring. The result is still the nearest free driver within the radius.
4. **Sharding by geographic cell exists, off by default** (`GEO_SHARD_DEGREES`, e.g. `0.05` ≈ 5.5 km; see `pkg/driverlocation`). Each cell is a key
   `drivers:geo:<i>:<j>`; a driver who crosses a boundary is removed from the old cell inside the same update script; a search scans the cells the circle touches
   (pipelined) and then merges by distance. Tests confirm the results are **identical** with and without sharding, including drivers right at a boundary and drivers who just crossed cells.
   **It does not make each query cheaper**: measured with 1 million drivers split into 0.05° cells, a 3 km query into one cell (27,700 drivers) still takes ~9 ms (versus ~11 ms
   on a single key), and a query touches 1–4 cells. Its value is **spreading load across many keys**, so it is only useful when those keys live on multiple nodes
   (Redis Cluster) or multiple Redis instances; on a single Redis it is not worth enabling. Constraints:
   - Matching and Driver Location Service must use the same value (the ConfigMap shares `GEO_SHARD_DEGREES`); changing the value requires rebuilding the GEO set
     (drivers refill it themselves within a few seconds of location reports), because the stored locations are in the old layout.
   - The radius must not exceed ~64 cells; beyond that `Search` returns an error instead of silently missing drivers (increase the cell size or reduce the radius).
   - On Redis Cluster, the cell keys and the scripts that build keys dynamically need hash tags; **not done yet**, so it cannot be used with Cluster.
   - `ClaimFirstForOrder` also builds `driver:<id>:*` keys inside the script, with the same Cluster limitation.

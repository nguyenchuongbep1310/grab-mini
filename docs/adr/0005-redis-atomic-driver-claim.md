# 0005. Claim drivers atomically in Redis, with a lease

Status: accepted

## Context

Matching runs in several instances and several partitions. Two orders near each other can pick the same nearest driver at the same moment; a plain "read status, then write" lets both succeed. Also, a matcher can die after claiming a driver and before publishing `order.matched`, which would strand the driver as ASSIGNED forever.

## Decision

Driver state lives in Redis (status, vehicle type, location in a GEO set). Matching asks `GEOSEARCH` for nearby candidates, then runs one Lua script (`driverstatus.ClaimFirstForOrder`) that walks the candidate list and claims the first driver that is `AVAILABLE` and of the right vehicle type, all inside Redis. Redis runs scripts atomically, so two claims cannot interleave.

- The claim stores the order id, so a re-delivered `order.created` gets the same driver back (idempotent, [0003](0003-at-least-once-and-idempotent-consumers.md)).
- The claim is a **lease**: the driver is ASSIGNED only until a deadline, measured with Redis's own clock (`TIME`) to avoid clock skew between matchers. Trip Service ends the lease by moving the driver to `ON_TRIP`; if no trip ever follows, the lease expires and the driver is claimable again.
- `order.cancelled` releases a just-claimed driver.
- Searching runs in expanding rings (1 km, 3 km, configured radius) because search cost grows with the number of candidates in the radius, not with total drivers ([performance.md](../performance.md)).

## Alternatives considered

- **Postgres row locks (`SELECT ... FOR UPDATE SKIP LOCKED`).** Correct and durable, but location search is not what Postgres is good at at 25,000 updates/s, and it puts write load on the order database.
- **Optimistic check-and-set per candidate.** One round trip per candidate (measured up to 50 per order before the single-script version).
- **Single matcher.** Trivially correct, no horizontal scaling.

## Consequences

- Good: no double assignment and no stranded drivers, tested with concurrent claims; one Redis round trip per order.
- Bad: Redis is not the system of record and is a dependency of the matching path. Status, leases and locations are treated as self-healing: drivers report again within seconds and leases expire ([disaster-recovery.md](../disaster-recovery.md)). The client follows Sentinel failovers (tested against a real master, replica and Sentinel), but the manifests do not deploy Redis/Sentinel. What an in-flight claim does when the master is lost mid-match has not been exercised.
- Bad: the script builds keys inside Lua, so it cannot run unchanged on Redis Cluster (hash tags are not implemented). Sharding by geographic cell exists behind `GEO_SHARD_DEGREES`, off by default.

## Revisit when

One Redis can't hold the search load (see the measurements in [performance.md](../performance.md)): move to Cluster with hash-tagged keys, or partition matching by city.

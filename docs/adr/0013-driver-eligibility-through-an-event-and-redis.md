# 0013. Driver eligibility: Profile Service decides, Matching enforces through an event

Status: accepted

## Context

A driver should be matched only after an admin has approved them, and must stop being matched when suspended. Profile Service owns the profile and the decision. Matching owns driver availability in Redis and must stay fast and independent of Profile Service.

## Decision

Profile Service publishes `driver.application.reviewed` (key `driverId`; status `APPROVED`, `REJECTED` or `SUSPENDED`; no personal data beyond the vehicle type and the admin's reason) through the outbox, in the same transaction as the status change. Matching consumes it in its own group and applies it with one Lua script (`driverstatus.ApplyReview`).

- **Stale results are ignored.** Each result carries a time, and Profile Service forces it to be strictly later than the driver's previous result. Matching keeps the last applied time per driver and drops older or repeated results, so redelivery and reordering are safe.
- **Approval** makes the driver eligible (available with the vehicle type) only if they are free; a driver on a trip is never touched. **Rejection and suspension** set a `suspended` flag that the claim scripts respect, and take the driver offline only if they are free, so a trip in progress finishes. The flag also stops the post-trip release from making them available again.
- **A vehicle change** by an approved driver sends them back to review and publishes a `SUSPENDED` result in the same transaction, so nobody is matched with a vehicle that was not approved.
- **Matching never requires a profile.** Drivers seeded into Redis (development, e2e, load test) stay matchable. Eligibility is a block list in effect, not an allow list.
- The Driver App gate is cosmetic. Matching is the real gate.

## Alternatives considered

- **Matching asks Profile Service on every claim.** Exact, but puts a synchronous dependency on the hottest path ([ADR 0001](0001-kafka-event-driven-architecture.md), [ADR 0005](0005-redis-atomic-driver-claim.md)).
- **Profile Service writes the driver's Redis keys itself.** No event, but couples it to Matching's key layout and skips idempotency.
- **Allow list (no profile, no match).** Safer by default, but breaks every existing flow and the load test.

## Consequences

- Good: Matching's path is unchanged and still one Redis script; a suspension takes effect as soon as the event is consumed; ordering is handled by the event's time, not by Kafka ordering luck.
- Bad: **eligibility lives only in Redis, and restoring it is a manual step.** If Redis loses data the `suspended` flag and the driver's status and vehicle are lost, and a suspended driver could become matchable again until the loss is repaired. The repair is the resync: an operator runs `POST /driver-applications/resync` or `profile-service resync`, and Profile Service re-publishes the current status of every approved, rejected or suspended driver as `driver.application.reviewed` (dated after the driver's last result, the rows unchanged). Matching applies it unchanged (no new consumer code): without `review_at` it applies; with Redis intact it is idempotent and does not make a busy driver available or lift a suspension. It is deliberately not periodic. The window is real: from the loss until someone runs the resync, eligibility is wrong (approved drivers get no trips; a suspended driver whose status survived on a replica may still be claimed). Resync restores the *decisions*, not the driver's activity: a driver on a trip whose `ON_TRIP` was lost becomes `AVAILABLE`, and a driver who applied or was decided during an outage is covered only once the event reaches the topic (the consumer retries while Redis is down) or the next resync covers them.
- Bad: between the decision and the consumer applying it there is a short window where a suspended driver can still be claimed.
- Bad: the public driver view returns 404 for every non-approved driver (so it cannot reveal who applied), so a rider whose driver is suspended mid-trip sees only an id until the trip ends.

## Revisit when

Redis can no longer be treated as rebuildable state by an operator action: run the resync periodically (it is idempotent), or move eligibility into the database that Matching can read.

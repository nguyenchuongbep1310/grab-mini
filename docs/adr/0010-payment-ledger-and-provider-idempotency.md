# 0010. Settle rides in a double-entry ledger, behind an idempotent provider interface

Status: accepted

## Context

A completed trip must charge the rider exactly once, split the money between the driver and the platform, and leave a record that can be audited. Events arrive at least once and out of order ([ADR 0003](0003-at-least-once-and-idempotent-consumers.md), [ADR 0006](0006-retry-tiers-and-dead-letter-topics.md)); a card processor can time out after it has already charged.

## Decision

`payment-service` consumes `order.created` (keeps a local copy of the price locked at booking), `trip.status.changed` (fare on `COMPLETED`, optional fee when a rider cancels after a driver was assigned) and `trip.rated` (tip), and publishes `payment.recorded` through the outbox ([ADR 0004](0004-transactional-outbox.md)).

- **Double-entry ledger in Postgres.** Accounts `RIDER`, `DRIVER_PAYABLE`, `PLATFORM_REVENUE`; a transaction is signed entries that sum to exactly zero. Balance is checked in code before anything is written and again by a deferred constraint trigger at commit; the ledger is append-only. Balances are the sum of entries, never stored.
- **Integer money.** Amounts are `BIGINT` whole VND. The commission (default 20%, `PLATFORM_COMMISSION_BPS`) rounds down and the driver gets the remainder, so no dong is created or lost. Tips are never commissioned.
- **Idempotent at three levels.** Events are deduped by `eventId` in the same transaction as the payment, ledger entries and outbox row; a charge is unique per `(trip, kind)`; the provider call is idempotent on a key (`fare:<tripId>`).
- **Pluggable provider.** `PaymentProvider` is an interface; only a sandbox fake exists (riders whose id starts with `decline-` are declined). The service never sees card data. A decline is a normal answer and is recorded; an error means "unknown, retry", and the retry tiers plus the idempotency key make that safe.
- **Ordering by retry, not by coupling.** `trip.status.changed` can arrive before `order.created`. The handler then fails transiently and the event goes through the first retry tier, rather than adding the price to other services' events or calling Order Service synchronously.

## Alternatives considered

- **Add `price` to `trip.status.changed`.** Needs changes in Matching, Trip and three schemas to carry a number Order Service already published.
- **Call Order Service for the price.** Makes payment depend synchronously on another service ([ADR 0001](0001-kafka-event-driven-architecture.md)).
- **A single `balance` column per account.** Simpler, but it can drift from its history and is hard to audit.
- **Kafka exactly-once.** Cannot cover the provider call or Postgres, so idempotency keys are needed regardless.

## Consequences

- Good: a replay, a crash after the provider call, or a duplicate event cannot double-charge or unbalance the books. Covered by unit tests (money, policy, ledger, provider) and integration tests against real Postgres and Kafka.
- Bad: the provider call is outside the database transaction, so a crash leaves a charge at the provider with no local row until the retry. This relies on the provider honouring the idempotency key, which the fake does and a real one must.
- Bad: the early-trip-event case makes retries part of the normal path, so `PaymentDeadLettered` can page when `order.created` is genuinely missing (see the runbook).
- Bad: not implemented: real payouts to drivers (the payable balance is only recorded), refunds or reversing entries, debt collection after a decline, and reconciliation with the provider's reports. The stored price comes from a float in the event, converted once to an integer.

## Revisit when

A real provider is connected: add reconciliation, reversing entries and payouts, and test the idempotency-key contract against that provider.

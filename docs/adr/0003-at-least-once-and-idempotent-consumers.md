# 0003. At-least-once delivery with idempotent consumers, not Kafka exactly-once

Status: accepted

## Context

A consumer can crash after doing its side effect but before committing its offset, so the message is delivered again. A producer can crash after publishing but before recording that it did. Duplicates are unavoidable unless something prevents them.

## Decision

Every hop is at-least-once, and every consumer makes duplicates harmless. Kafka's transactional/idempotent-producer exactly-once is not used.

How each consumer is idempotent (all tested):

- **Trip Service**: `INSERT INTO processed_events (event_id) ... ON CONFLICT DO NOTHING` in the same transaction as the state change; a conflict means "already done".
- **Rating Service**: dedupes by `eventId` in the same transaction as the rating.
- **Matching Service**: the claim is keyed by order (`order:{id}:match`, 24 h), so re-processing `order.created` returns the same driver instead of claiming another.
- **State machines**: transitions are guarded by the current status (a second `COMPLETED` is a no-op).
- **Driver location**: an update older than the stored one is dropped, so replays cannot move a driver backwards.
- **Producers**: the outbox relay ([0004](0004-transactional-outbox.md)) is at-least-once, `eventId` travels in the event.

## Alternatives considered

- **Kafka EOS (transactions).** It only covers Kafka-to-Kafka read-process-write. Our side effects are Postgres rows and Redis keys, which a Kafka transaction cannot include, so we would still need idempotent writes. It adds latency and operational constraints for little gain here.
- **Dedupe in a shared service.** One more dependency on the hot path; local dedupe in the consumer's own transaction is cheaper and cannot disagree with its own state.

## Consequences

- Good: simple failure model (crash anywhere, replay is safe); works across Kafka, Postgres and Redis.
- Bad: every new consumer must be written idempotently; this is a discipline, not something the platform enforces. Reviewing consumers against this rule is part of the review checklist.
- Bad: `processed_events` grows without bound unless pruned. There is no retention job yet (see [scaling.md](../scaling.md)).

## Revisit when

A pipeline is purely Kafka-to-Kafka and a duplicate is expensive (for example money movement), where EOS or an idempotency-key store is justified.

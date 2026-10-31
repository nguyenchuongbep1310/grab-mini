# 0012. Chat: durable messages, best-effort push, and no message text in logs

Status: accepted

## Context

Rider and driver need to coordinate pickup. A message must not be lost, but live delivery over a phone connection is unreliable by nature. Message text is personal data.

## Decision

- **Durable first.** `chat-service` writes the message to Postgres and a `chat.message.sent` row to the outbox in one transaction ([ADR 0004](0004-transactional-outbox.md)). Notification Service consumes the event and pushes it to the **other** participant through the existing WebSocket and Redis pub/sub path ([ADR 0008](0008-notification-fan-out-and-recovery.md)).
- **Push is a hint, REST is the truth.** The apps load history by REST, merge pushes by message id, and reload history when the socket reconnects.
- **Who may talk** comes from `trip.status.changed` (participants and status), not from a call to Trip Service. The chat is open while the trip is active and read-only for a short grace period after it ends.
- **Sends are idempotent.** An optional `Idempotency-Key` makes a retried send return the saved message instead of duplicating it.
- **Abuse limits.** 500 characters, no control or bidi characters, 20 messages a minute per sender (in memory, per instance).
- **Privacy.** Message text is never logged (ids and lengths only; a test enforces it), and messages are pruned after 30 days.

## Alternatives considered

- **Push only, no storage.** Simple, but a message sent while the other phone is offline would vanish.
- **Ask Trip Service who the participants are on every send.** Exact, but makes chat depend on Trip Service being up.
- **A third-party chat service.** Out of scope for a project about doing this with Kafka.

## Consequences

- Good: no lost messages; a flaky connection costs a delay, not data; chat works while Trip Service is down.
- Bad: **the text is copied** into Kafka (7 days), the dead-letter topics, the outbox dead-letter table and Postgres backups. There is no per-user deletion API.
- Bad: the rate limit is per instance, so the real limit is about the configured value times the number of replicas unless requests for one sender land on one instance.
- Bad: not implemented: read receipts, typing indicators, attachments, moderation beyond length and character checks.

## Revisit when

Chat is used for disputes or support: add deletion and export, moderation, and a shared (Redis) rate limit.

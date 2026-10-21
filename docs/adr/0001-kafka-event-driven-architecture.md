# 0001. Kafka as the backbone: services communicate through events

Status: accepted

## Context

Booking a ride touches several concerns: persisting the order, finding a driver, creating a trip, pricing, notifying two clients, rating afterwards. Matching can take seconds and may fail; notification must not slow down a booking; analytics must see everything without anyone writing integration code for it.

## Decision

The order, matching, trip, rating and notification services never call each other. Each publishes facts about what happened (`order.created`, `order.matched`, `trip.status.changed`, ...) and reacts to the facts it cares about, each in its own consumer group. The only synchronous calls are the clients' REST requests and the matching/pricing reads of Redis.

## Alternatives considered

- **Synchronous REST between services.** Simplest to debug, but Order Service would need to know about, and wait for, Matching, and one slow dependency would back up booking requests. Retrying and ordering would become each caller's problem.
- **A work queue (RabbitMQ/SQS).** Fine for "one consumer per message". Here several independent consumers need the same event (Trip, Notification, ksqlDB all read `order.matched`), and replaying history for a new consumer or a fix is useful. A log gives both.

## Consequences

- Good: a service can be down or slow without losing events or blocking producers; consumers scale by adding instances up to the partition count; a new consumer (ksqlDB analytics was added late) needs no change in producers.
- Good: the event log is the audit trail and the replay source.
- Bad: eventual consistency. A rider sees "searching" until the matched event is consumed and pushed. Every handler must tolerate duplicates and, with retry topics, out-of-order delivery ([0003](0003-at-least-once-and-idempotent-consumers.md), [0006](0006-retry-tiers-and-dead-letter-topics.md)).
- Bad: harder to debug than a call stack. Mitigated with trace ids carried through events (`pkg/tracing`), structured logs with topic/partition/offset, and Kafka UI.
- Bad: operating Kafka is a cost a monolith with a job table would not have. This is a learning project, so that cost is part of the point; for a real product of this size I would say so.

## Revisit when

A flow needs an immediate, consistent answer from another service (for example payment authorisation before confirming a trip). That one call should be synchronous, with the event published after it succeeds.

# 0008. Fan out WebSocket pushes through Redis pub/sub; treat push as best-effort

Status: accepted

## Context

Clients hold WebSocket connections to Notification Service. With several instances, the instance that consumes an event is usually not the one that holds the recipient's socket. Also, a phone can lose its connection at any moment.

## Decision

The instances share one consumer group (`notification-service-group`), so Kafka splits the partitions between them. For each event the consuming instance looks up the recipient (routing keys in Redis, kept 6 hours) and publishes the push to a Redis channel for that recipient (`channelFor(kind, id)`); whichever instance holds the socket is subscribed to that channel and writes to it. Instances are therefore stateless apart from their in-memory socket map and can scale out.

Push is best-effort: Redis pub/sub does not store messages. The source of truth is REST. After a reconnect, the apps fetch current state (`GET /orders/{id}`, `GET /trips/by-order/{id}`, `GET /drivers/{id}/active-trip` for drivers and `GET /riders/{id}/latest-order` for riders) and render from that. A missed push costs at most a delay.

## Alternatives considered

- **Sticky routing of users to instances.** Fewer hops, but needs a load balancer that routes by user and does not survive an instance dying.
- **One consumer group, partition ownership = socket ownership.** Elegant, but ties the partition count to the connected users and re-balances sockets on every deploy.
- **Durable per-user queues (Redis streams).** Gives replay, costs complexity; REST recovery already covers it.

## Consequences

- Good: scales out; any instance can serve any socket; loss of a push is recoverable.
- Bad: no delivery guarantee for pushes and no per-user ordering guarantee across instances. Apps must treat pushes as hints to render, and REST as truth.
- Bad: **every location update costs Redis work** even when nobody is watching: a routing lookup (driver to rider) and a publish to the rider's channel and to the driver's own channel. At 25,000 messages/s that is a Redis load comparable to the location writes themselves, and it is the first notification bottleneck ([scaling.md](../scaling.md)).
- Bad: not load-tested with many concurrent sockets; the numbers in [performance.md](../performance.md) do not cover it.

## Revisit when

Location traffic makes the per-message Redis work too expensive: push only for drivers on an active trip, and keep the driver-to-rider mapping in the instance's memory instead of looking it up per message.

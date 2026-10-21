# 0006. Retry topics with growing delay, then a dead-letter topic

Status: accepted

## Context

A handler fails for different reasons. A database that is down for a minute needs time; a message that cannot be decoded will never succeed. Retrying in place blocks the partition, so every message behind the bad one waits.

## Decision

`services/pkg/kafkaconsumer` is the loop every service uses:

- **Permanent error** (for example an undecodable message, `eventcodec.DecodeError`): straight to `<topic>.DLQ`.
- **Transient error**: move the message to `<topic>.retry.1`, commit, and carry on with the partition. Retry readers wait until the message is due: 5 s, then 1 min, then 10 min, and after the last tier the message goes to the DLQ.
- Handlers see the original topic name even when reading from a retry topic.
- `dlq-redrive` is an operator tool that sends DLQ messages back to their source topic once the cause is fixed (`-dry-run`, `-error` filter, `-max`). It needs operator credentials: service principals can write their DLQs but not read them (ACLs in `scripts/kafka-acls.sh`).
- Metrics and an alert exist for DLQ depth; the runbook has the procedure.

## Alternatives considered

- **Retry in place with backoff.** Simple and ordered, but a poison or slow message stalls the whole partition. Still available as a fallback when no tiers are configured.
- **A single DLQ after N immediate retries.** Does not give a dependency time to recover; fast retries just burn the budget.
- **Library-level retry (Spring-Kafka style non-blocking retries).** Same idea; hand-written here to understand it.

## Consequences

- Good: one bad or slow message does not stop the partition. Tested end to end with a handler that always fails (lands in the DLQ after the tiers) and with an undecodable message (DLQ immediately).
- Bad: a retried message is processed after later ones, so ordering is lost for it. Handlers must guard transitions by current state ([0002](0002-partition-keys-and-ordering.md), [0003](0003-at-least-once-and-idempotent-consumers.md)).
- Bad: the delay is a property of the retry reader waiting, not of the broker; a tier's reader occupies a worker while waiting.
- Bad: a redrive of messages that fail for a still-unfixed reason just refills the DLQ. The tool is manual on purpose.

## Revisit when

Messages sit for long periods in retry tiers, or order-dependent topics need retry without reordering (then use a per-key blocking retry with a bounded budget).

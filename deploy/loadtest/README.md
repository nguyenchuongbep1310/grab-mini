# Load test

Measures the real path of a ride (book → match a driver → trip → pickup → complete → rate) together with the driver location stream, **without Docker**: it runs
the real Go services against an existing Kafka broker, Postgres and Redis.

```
export KAFKA_BIN=/path/to/kafka/bin KAFKA_BROKERS=localhost:9092 REDIS_ADDR=localhost:6380 \
       PSQL_ADMIN='psql -h /tmp -p 5433 -U postgres' DB=grabmini_load \
       DATABASE_URL='postgres://postgres@/grabmini_load?host=/tmp&port=5433' \
       PARTITIONS=12 KAFKA_CONSUMER_WORKERS=12

deploy/loadtest/reset.sh        # DELETES the app's topics, recreates the DB, FLUSHES Redis. Point it only at disposable infrastructure.
deploy/loadtest/stack.sh start  # build + run dev-registry (a fake Schema Registry) and 8 services
go build -o /tmp/loadtest ./services/loadtest   # (workspace: cd services/loadtest && go build)
/tmp/loadtest -redis $REDIS_ADDR -drivers 3000 -orders-per-sec 250 -duration 40s -drain 8s -poll-interval 1s
deploy/loadtest/stack.sh stop
```

Run `reset.sh` **before every measurement**: a backlog left over from the previous run skews the numbers, and the services must start after the topics
exist (a consumer that starts before its topic is never assigned that topic).

The load generator is open loop (it places orders at a fixed rate even if the server is slow, so queues show up instead of being hidden) and prints two groups of numbers:
the client side (latency of each operation through the gateway) and the server side (quantiles computed from `/metrics` deltas: match latency, lag, CPU per process).
`-out file.json` saves the full result.

## Reading the results

- `create_to_matched` p50 ≈ `-poll-interval` is normal: the client polls once a second. The real figure is the server-side `match latency`.
- A `timeout:` on `create_to_matched`, or `NOT cleared within the drain` on the backlog, means the system **could not keep up** with that load.
- Compare **relative** results between runs on the same machine only. The whole stack, the broker, Postgres, Redis and the load generator share a few cores,
  so the absolute numbers are not production capacity. Results and how to interpret them: [docs/performance.md](../../docs/performance.md).

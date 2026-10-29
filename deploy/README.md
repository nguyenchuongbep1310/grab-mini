# Deploying grab-mini

```
deploy/
  docker/Dockerfile         one multi-stage file for every Go service
  k8s/generate.py           writes k8s/base from one description (edit this, not the YAML)
  k8s/check.py              cross-checks the rendered manifests against each other and the Go source
  k8s/base/                 Kustomize base: services, migrations, scaling, Kafka, network, monitoring, redis, backup, retention
  redis/                    start/stop scripts of the Redis and Sentinel containers (put in a ConfigMap by generate.py)
  monitoring/               alert rules (alerts.rules.yaml) and their tests (alerts.test.yaml)
  kafka-secure/             a local Kafka with TLS, SASL/SCRAM and ACLs, for trying the security model
  verify.sh                 everything above that can be checked without a cluster
docs/runbook.md             what to do when an alert fires
```

## What the base deploys

| | |
|---|---|
| Services | order, trip, rating, payment, notification, location-gateway, pricing (HTTP) and matching, driver-location (workers). **Auth Service is not deployed**: it hands a token to anyone and refuses to run in production. Put a real identity provider in front and have the services verify its tokens. |
| Per pod | non-root, read-only filesystem, no capabilities; probes on a separate **admin port** (`9090`: `/livez`, `/readyz`, `/metrics`) that no ingress should route; `maxUnavailable: 0`; spread over zones and nodes; a PodDisruptionBudget; a 5 s `preStop` sleep so the pod leaves the endpoints before it drains |
| Scaling | KEDA scales consumers on **Kafka lag** (replicas capped at the partition count) and APIs on CPU; the location gateway scales on CPU only |
| Migrations | one Job per database-owning service (`<service> migrate`); the Deployments run with `RUN_MIGRATIONS=false` |
| Kafka | a Strimzi cluster: 3 KRaft controllers and 3 brokers across zones, replication factor 3, `min.insync.replicas` 2 (1 for locations), no unclean leader election, no topic auto-creation; the six topics; a `KafkaUser` per service whose ACLs are read from `scripts/kafka-acls.sh`, so the two cannot disagree |
| Network | ingress **and egress** denied by default; the ingress controller may reach the APIs' public port; Prometheus may reach the admin port; each workload may reach only DNS, Kafka, and the Postgres, Redis, Schema Registry, OTLP collector and JWKS endpoints it uses (see [docs/security.md](../docs/security.md)) |
| Monitoring | PodMonitors and a PrometheusRule made from `monitoring/alerts.rules.yaml` |
| Redis | a StatefulSet of 3 pods, each `redis` + `sentinel` + `redis_exporter` (scripts in `redis/`), AOF on a 5Gi PVC, password from a Secret, one pod per node and per zone, a PodDisruptionBudget; the services find the master through the Sentinels. Proven on real Redis by `scripts/test-redis-ha.sh`. |
| Data ops | `retention-*` CronJobs (finished orders and trips older than 180 days, in batches: `scripts/pg-retention.sh`); `backup-<service>` CronJobs (`pg_dump` every 6 h to object storage: `scripts/pg-backup.sh`); a restore drill (`scripts/pg-restore-drill.sh`). Proven on real Postgres by `scripts/test-data-ops.sh`. See `docs/disaster-recovery.md`. |

## Needs in the cluster

[Strimzi](https://strimzi.io) (Kafka, KafkaNodePool, KafkaTopic, KafkaUser), [KEDA](https://keda.sh), the
Prometheus Operator (PodMonitor, PrometheusRule) with kube-state-metrics, and an ingress controller in the
`ingress-nginx` namespace. Prometheus is expected in the `monitoring` namespace. Not included, and set per
environment in `k8s/base/config.yaml`: **Postgres**, **Schema Registry**, an OpenTelemetry collector,
the identity provider that serves the JWKS, and the ingress routes (route only the APIs' public paths, never `/metrics`). The base *does* deploy Redis; to use a
managed one instead, drop `redis/` from the kustomization, empty `REDIS_SENTINEL_ADDRS` and set `REDIS_ADDR`.
Set `BACKUP_TARGET` in `grab-mini-data-ops` (`k8s/base/dataops/config.yaml`; it ships as `s3://CHANGE-ME-...` so the
backup job fails loudly until you do). Development (`docker-compose.yml`) keeps one Redis with no password.

## Secrets you create

| Secret | Keys | Used by |
|---|---|---|
| `grab-mini-schema-registry` | `username`, `password`, `ca.crt` (the CA that signed the registry's certificate) | every service |
| `grab-mini-db-order-service`, `-trip-service`, `-rating-service`, `-payment-service` | `url` (a Postgres URL) | that service and its migration Job |
| `grab-mini-redis` | `password` (used for Redis and its Sentinels) | the Redis pods and every service that uses Redis |
| `grab-mini-backup` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (write-only on the bucket is enough) | the `backup-*` CronJobs |
| `keda-kafka-auth` | `username` (`keda`), `password`, `ca.crt` | the KEDA Kafka scaler |

No service holds token key material: they verify with the public keys at `JWT_JWKS_URL` (ConfigMap `grab-mini-config`;
point it at your identity provider). The private signing key (`JWT_PRIVATE_KEY_FILE`, an Ed25519 PEM) belongs to the
issuer's own deployment only, which this base does not contain; `check.py` fails if a service in this base mounts one.

Strimzi creates the rest: each service's Kafka password (a Secret named after its `KafkaUser`) and the cluster CA
(`grab-mini-cluster-ca-cert`). Services read every secret from a mounted file (`*_FILE`), and start only if it
is there: with `APP_ENV=production` there are no development defaults.

## Apply

```bash
# 1. set the image of each service
for s in order-service trip-service rating-service payment-service notification-service location-gateway pricing-service matching-service driver-location-service; do
  (cd deploy/k8s/base && kustomize edit set image ghcr.io/example/grab-mini/$s=$REGISTRY/$s:$TAG)
done
# 2. the cluster, the topics and the users, and wait for Kafka
kubectl apply -k deploy/k8s/base --selector 'app.kubernetes.io/component in (config)'   # or apply everything; Strimzi/KEDA CRDs must exist first
kubectl -n grab-mini wait kafka/grab-mini --for=condition=Ready --timeout=600s
# 3. migrations, then the rollout
kubectl -n grab-mini wait --for=condition=complete job --all --timeout=300s
kubectl apply -k deploy/k8s/base
```

Build an image: `docker build -f deploy/docker/Dockerfile --build-arg SERVICE=order-service -t $REGISTRY/order-service:$TAG .`
from the repository root (`SERVICE` is a directory under `services/`; `dlq-redrive` builds the same way).

## Changing it

Edit `k8s/generate.py` (or the access table in `scripts/kafka-acls.sh`, or `monitoring/alerts.rules.yaml`), run
`python3 deploy/k8s/generate.py`, and commit the generated files with it. `deploy/verify.sh` fails if they are out of date.

## What `verify.sh` proves, and does not

It proves the manifests are valid Kubernetes and CRD objects (kubeconform, strict), agree with one another and
with the code (every environment variable is one a service reads; every consumer group has ACLs and every scaler
names a real group; every topic has a KafkaTopic; names, ports and probes line up), that each alert parses and
behaves on synthetic data (`promtool test rules`), and that every service builds the way the Dockerfile builds it
(static, CGO off).

It does **not** prove that a cluster accepts the manifests, that Strimzi or KEDA behave as configured, that the
Docker image builds (the Dockerfile has not been built: there was no Docker daemon), or that the alert thresholds
are right for real traffic. Try it in a staging cluster first, and tune the thresholds against real numbers.

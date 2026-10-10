# Deploying grab-mini

```
deploy/
  docker/Dockerfile         one multi-stage file for every Go service
  k8s/generate.py           writes k8s/base from one description (edit this, not the YAML)
  k8s/check.py              cross-checks the rendered manifests against each other and the Go source
  k8s/base/                 Kustomize base: services, migrations, scaling, Kafka, network, monitoring
  monitoring/               alert rules (alerts.rules.yaml) and their tests (alerts.test.yaml)
  kafka-secure/             a local Kafka with TLS, SASL/SCRAM and ACLs, for trying the security model
  verify.sh                 everything above that can be checked without a cluster
docs/runbook.md             what to do when an alert fires
```

## What the base deploys

| | |
|---|---|
| Services | order, trip, rating, notification, location-gateway, pricing (HTTP) and matching, driver-location (workers). **Auth Service is not deployed**: it hands a token to anyone and refuses to run in production. Put a real identity provider in front and have the services verify its tokens. |
| Per pod | non-root, read-only filesystem, no capabilities; probes on a separate **admin port** (`9090`: `/livez`, `/readyz`, `/metrics`) that no ingress should route; `maxUnavailable: 0`; spread over zones and nodes; a PodDisruptionBudget; a 5 s `preStop` sleep so the pod leaves the endpoints before it drains |
| Scaling | KEDA scales consumers on **Kafka lag** (replicas capped at the partition count) and APIs on CPU; the location gateway scales on CPU only |
| Migrations | one Job per database-owning service (`<service> migrate`); the Deployments run with `RUN_MIGRATIONS=false` |
| Kafka | a Strimzi cluster: 3 KRaft controllers and 3 brokers across zones, replication factor 3, `min.insync.replicas` 2 (1 for locations), no unclean leader election, no topic auto-creation; the six topics; a `KafkaUser` per service whose ACLs are read from `scripts/kafka-acls.sh`, so the two cannot disagree |
| Network | ingress denied by default; the ingress controller may reach the APIs' public port; Prometheus may reach the admin port |
| Monitoring | PodMonitors and a PrometheusRule made from `monitoring/alerts.rules.yaml` |

## Needs in the cluster

[Strimzi](https://strimzi.io) (Kafka, KafkaNodePool, KafkaTopic, KafkaUser), [KEDA](https://keda.sh), the
Prometheus Operator (PodMonitor, PrometheusRule) with kube-state-metrics, and an ingress controller in the
`ingress-nginx` namespace. Prometheus is expected in the `monitoring` namespace. Not included, and set per
environment in `k8s/base/config.yaml`: **Postgres**, **Redis**, **Schema Registry**, an OpenTelemetry collector,
and the ingress routes (route only the APIs' public paths, never `/metrics`).

## Secrets you create

| Secret | Keys | Used by |
|---|---|---|
| `grab-mini-jwt` | `secret` (at least 32 bytes) | every API service |
| `grab-mini-db-order-service`, `-trip-service`, `-rating-service` | `url` (a Postgres URL) | that service and its migration Job |
| `keda-kafka-auth` | `username` (`keda`), `password`, `ca.crt` | the KEDA Kafka scaler |

Strimzi creates the rest: each service's Kafka password (a Secret named after its `KafkaUser`) and the cluster CA
(`grab-mini-cluster-ca-cert`). Services read every secret from a mounted file (`*_FILE`), and start only if it
is there: with `APP_ENV=production` there are no development defaults.

## Apply

```bash
# 1. set the image of each service
for s in order-service trip-service rating-service notification-service location-gateway pricing-service matching-service driver-location-service; do
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

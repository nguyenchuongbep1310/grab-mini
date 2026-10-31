#!/usr/bin/env python3
"""Generates deploy/k8s/base/** from one description of the system.

    python3 deploy/k8s/generate.py            # write the files
    python3 deploy/k8s/generate.py --check    # fail if the committed files differ

The services are nine near-identical Deployments, and the Kafka users must
match scripts/kafka-acls.sh, so they are generated rather than copied: edit
this file (or the ACL table in that script), regenerate, commit both.
Needs only Python 3 and PyYAML.
"""
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "deploy" / "k8s" / "base"
CLUSTER = "grab-mini"
KAFKA_BOOTSTRAP = f"{CLUSTER}-kafka-bootstrap:9093"
ADMIN_PORT = 9090  # /livez, /readyz and /metrics: never exposed by an ingress
IMAGE_REPO = "ghcr.io/example/grab-mini"  # replaced per environment: kustomize edit set image

# ---------------------------------------------------------------- topics ----
# name: (partitions, retention, min in-sync replicas). The location topic is
# telemetry: a day of retention and one replica enough to ack; the rest are
# business events and keep a week, with two replicas required.
DAY = 24 * 3600 * 1000
TOPICS = {
    "order.created": (12, 7 * DAY, 2),
    "order.matched": (12, 7 * DAY, 2),
    "order.no_driver_found": (6, 7 * DAY, 2),
    "order.cancelled": (6, 7 * DAY, 2),
    "trip.status.changed": (12, 7 * DAY, 2),
    "trip.rated": (6, 7 * DAY, 2),
    "payment.recorded": (6, 7 * DAY, 2),
    "driver.location.updated": (48, 1 * DAY, 1),
}

# ------------------------------------------------------------- services ----
# kind: "api" has a public port; "worker" has only the admin port.
# secrets: what it mounts besides its Kafka credentials.
SERVICES = {
    "order-service": dict(port=3001, kind="api", db=True, redis=True, quote=True, scale=("lag", 2, 6)),
    "trip-service": dict(port=3002, kind="api", db=True, redis=True, scale=("lag", 2, 12)),
    "rating-service": dict(port=3004, kind="api", db=True, redis=False, scale=("lag", 2, 6)),
    # Settles rides: fare, tip and cancellation fee into a double-entry ledger. No Redis.
    "payment-service": dict(port=3008, kind="api", db=True, redis=False, scale=("lag", 2, 6)),
    "notification-service": dict(port=3003, kind="api", db=False, redis=True, scale=("lag", 2, 24), origins=True),
    "location-gateway": dict(port=3006, kind="api", db=False, redis=False, scale=("cpu", 3, 30)),
    "matching-service": dict(port=None, kind="worker", db=False, redis=True, scale=("lag", 2, 12)),
    "driver-location-service": dict(port=None, kind="worker", db=False, redis=True, scale=("lag", 3, 24)),
    # Surge pricing: no database; quotes are signed with the secret it shares with order-service.
    "pricing-service": dict(port=3007, kind="api", db=False, redis=True, quote=True, scale=("lag", 2, 6)),
}
LAG_THRESHOLD = {"driver.location.updated": "5000"}  # messages behind per replica before scaling out; default 100

# Which services have migrations (and so a migration Job).
MIGRATING = ["order-service", "trip-service", "rating-service", "payment-service"]

# Dedupe tables grow one row per event and are only read for a redelivery, so
# rows older than anything Kafka or the DLQ can still hand back are dead
# weight. Kafka keeps events 7 days (TOPICS) and a DLQ record may wait for a
# person, so 30 days is the floor: shortening it risks a late redrive being
# processed twice. name: (service, table, column).
DEDUPE_RETENTION_DAYS = 30
RETENTION = {
    "trip-processed-events": ("trip-service", "processed_events", "processed_at"),
    "rating-rating-events": ("rating-service", "rating_events", "processed_at"),
    # Safe to prune: a payment is unique per (trip, kind), so a very late redelivery is still refused.
    # The ledger, payments and order_prices (a late DLQ redrive still needs the price) are never pruned.
    "payment-payment-events": ("payment-service", "payment_events", "processed_at"),
}
POSTGRES_IMAGE = "postgres:16-alpine"
REDIS_IMAGE = "redis:7.4.12-alpine"
REDIS_EXPORTER_IMAGE = "oliver006/redis_exporter:v1.66.0-alpine"
AWS_CLI_IMAGE = "amazon/aws-cli:2.17.40"

# Orders and trips are history after a while. Finished rows older than this go
# (scripts/pg-retention.sh, which also refuses less than 30). Backups are the archive.
HISTORY_RETENTION_DAYS = 180
MATCHED_ORDER_DAYS = 365  # see the header of scripts/pg-retention.sh: MATCHED is the last state order-service sees
HISTORY_RETENTION = {  # CronJob suffix: (service, argument of pg-retention.sh)
    "order-orders": ("order-service", "orders"),
    "trip-trips": ("trip-service", "trips"),
}

# One pg_dump CronJob per service database (they may be one Postgres or three).
# Every 6 hours is the RPO of this layer; managed-Postgres PITR is the better one.
BACKUP_SCHEDULE = "7 */6 * * *"
BACKUP_SCRATCH_SIZE = "10Gi"  # the dump is staged on an emptyDir before the upload

# Redis: 3 pods, each a redis + a sentinel (+ exporter); quorum 2 of 3.
REDIS_REPLICAS = 3
REDIS_STORAGE = "5Gi"
REDIS_PORT, SENTINEL_PORT, EXPORTER_PORT = 6379, 26379, 9121
REDIS_MASTER_NAME = "mymaster"


def parse_acl_table():
    """The access table in scripts/kafka-acls.sh: the single source of truth."""
    text = (ROOT / "scripts" / "kafka-acls.sh").read_text()

    def block(name):
        m = re.search(r"declare -A %s=\((.*?)\n\)" % name, text, re.S)
        if not m:
            sys.exit(f"cannot find {name} in scripts/kafka-acls.sh")
        return dict(re.findall(r'\[([\w-]+)\]="([^"]*)"', m.group(1)))

    produces = {s: v.split() for s, v in block("PRODUCES").items()}
    consumes = {}
    for svc, v in block("CONSUMES").items():
        groups = []
        for entry in v.split(";"):
            group, topics = entry.split("=")
            groups.append((group, topics.split(",")))
        consumes[svc] = groups
    return produces, consumes


def labels(name, component):
    return {
        "app.kubernetes.io/name": name,
        "app.kubernetes.io/part-of": "grab-mini",
        "app.kubernetes.io/component": component,
    }


def meta(name, component="service", extra=None):
    m = {"name": name, "labels": labels(name, component)}
    m.update(extra or {})
    return m


# ---------------------------------------------------------- the services ----
def deployment(name, spec):
    api = spec["kind"] == "api"
    volumes = [
        {"name": "kafka-user", "secret": {"secretName": name, "items": [{"key": "password", "path": "password"}]}},
        {"name": "kafka-ca", "secret": {"secretName": f"{CLUSTER}-cluster-ca-cert", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
    ]
    mounts = [
        {"name": "kafka-user", "mountPath": "/etc/kafka/user", "readOnly": True},
        {"name": "kafka-ca", "mountPath": "/etc/kafka/ca", "readOnly": True},
    ]
    env = [
        {"name": "KAFKA_USERNAME", "value": name},
        {"name": "KAFKA_PASSWORD_FILE", "value": "/etc/kafka/user/password"},
        {"name": "KAFKA_CA_FILE", "value": "/etc/kafka/ca/ca.crt"},
        {"name": "HEALTH_PORT", "value": str(ADMIN_PORT)},
        {"name": "INSTANCE_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        # Migrations run in a Job before the rollout, not at every pod's start.
        {"name": "RUN_MIGRATIONS", "value": "false"},
    ]
    if api:
        env.append({"name": "PORT", "value": str(spec["port"])})
    if spec.get("db"):
        volumes.append({"name": "db", "secret": {"secretName": f"{CLUSTER}-db-{name}", "items": [{"key": "url", "path": "url"}]}})
        mounts.append({"name": "db", "mountPath": "/etc/secrets/db", "readOnly": True})
        env.append({"name": "DATABASE_URL_FILE", "value": "/etc/secrets/db/url"})
    if spec.get("redis"):  # one password for Redis and its Sentinels (see redis_stateful_set)
        volumes.append({"name": "redis", "secret": {"secretName": f"{CLUSTER}-redis", "items": [{"key": "password", "path": "password"}]}})
        mounts.append({"name": "redis", "mountPath": "/etc/secrets/redis", "readOnly": True})
        env.append({"name": "REDIS_PASSWORD_FILE", "value": "/etc/secrets/redis/password"})
        env.append({"name": "REDIS_SENTINEL_PASSWORD_FILE", "value": "/etc/secrets/redis/password"})
    # The API services verify tokens with the issuer's public keys (JWT_JWKS_URL
    # in the ConfigMap): no key material, and no way to mint a token, is mounted.
    # The signing key belongs to the issuer only; see deploy/README.md.
    # Every service talks to the Schema Registry, which wants credentials and
    # (since they travel in a header) https.
    volumes.append({"name": "schema-registry", "secret": {"secretName": f"{CLUSTER}-schema-registry", "items": [
        {"key": "username", "path": "username"}, {"key": "password", "path": "password"}, {"key": "ca.crt", "path": "ca.crt"}]}})
    mounts.append({"name": "schema-registry", "mountPath": "/etc/secrets/schema-registry", "readOnly": True})
    env += [
        {"name": "SCHEMA_REGISTRY_USER_FILE", "value": "/etc/secrets/schema-registry/username"},
        {"name": "SCHEMA_REGISTRY_PASSWORD_FILE", "value": "/etc/secrets/schema-registry/password"},
        {"name": "SCHEMA_REGISTRY_CA_FILE", "value": "/etc/secrets/schema-registry/ca.crt"},
    ]
    if spec.get("quote"):  # signs (pricing) or checks (order) surge quotes: the same secret
        volumes.append({"name": "quote", "secret": {"secretName": f"{CLUSTER}-quote", "items": [{"key": "secret", "path": "secret"}]}})
        mounts.append({"name": "quote", "mountPath": "/etc/secrets/quote", "readOnly": True})
        env.append({"name": "QUOTE_SECRET_FILE", "value": "/etc/secrets/quote/secret"})
    if spec.get("origins"):
        env.append({"name": "ALLOWED_ORIGINS", "valueFrom": {"configMapKeyRef": {"name": "grab-mini-config", "key": "ALLOWED_ORIGINS"}}})

    ports = [{"name": "admin", "containerPort": ADMIN_PORT}]
    if api:
        ports.insert(0, {"name": "http", "containerPort": spec["port"]})

    container = {
        "name": name,
        "image": f"{IMAGE_REPO}/{name}:latest",
        "ports": ports,
        "envFrom": [{"configMapRef": {"name": "grab-mini-config"}}],
        "env": env,
        "volumeMounts": mounts,
        # /livez never depends on anything outside the process; /readyz does,
        # and takes the pod out of rotation without restarting it.
        "startupProbe": {"httpGet": {"path": "/livez", "port": "admin"}, "periodSeconds": 2, "failureThreshold": 30},
        "livenessProbe": {"httpGet": {"path": "/livez", "port": "admin"}, "periodSeconds": 10, "failureThreshold": 3},
        "readinessProbe": {"httpGet": {"path": "/readyz", "port": "admin"}, "periodSeconds": 5, "failureThreshold": 2, "timeoutSeconds": 3},
        # The pod leaves the endpoints a moment after SIGTERM; keep serving
        # until it has, then the app drains (readiness turns 503 at once).
        "lifecycle": {"preStop": {"sleep": {"seconds": 5}}},
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
        "securityContext": {
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "runAsNonRoot": True,
            "capabilities": {"drop": ["ALL"]},
        },
    }
    pod_labels = labels(name, "service")
    if api:
        pod_labels["grab-mini/public"] = "true"  # NetworkPolicy lets the ingress reach these
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": meta(name),
        "spec": {
            "replicas": spec["scale"][1],
            "revisionHistoryLimit": 3,
            # A new pod must be ready before an old one goes: with idempotent
            # consumers and compatible schemas, old and new run side by side safely.
            "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1}},
            "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
            "template": {
                "metadata": {"labels": pod_labels},
                "spec": {
                    "serviceAccountName": "grab-mini",
                    "automountServiceAccountToken": False,
                    "terminationGracePeriodSeconds": 30,
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                    "topologySpreadConstraints": [
                        {"maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone", "whenUnsatisfiable": "ScheduleAnyway",
                         "labelSelector": {"matchLabels": {"app.kubernetes.io/name": name}}},
                        {"maxSkew": 1, "topologyKey": "kubernetes.io/hostname", "whenUnsatisfiable": "ScheduleAnyway",
                         "labelSelector": {"matchLabels": {"app.kubernetes.io/name": name}}},
                    ],
                    "containers": [container],
                    "volumes": volumes,
                },
            },
        },
    }


def service(name, spec):
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": meta(name),
        "spec": {
            "selector": {"app.kubernetes.io/name": name},
            "ports": [{"name": "http", "port": spec["port"], "targetPort": "http"}],
        },
    }


def pdb(name, spec):
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": meta(name),
        "spec": {"minAvailable": 1, "selector": {"matchLabels": {"app.kubernetes.io/name": name}}},
    }


def migration_job(name):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": meta(f"{name}-migrate", "migration", {"annotations": {
            # Argo CD runs this before the rollout; with plain kustomize, apply and wait for it first.
            "argocd.argoproj.io/hook": "PreSync",
            "argocd.argoproj.io/hook-delete-policy": "BeforeHookCreation",
        }}),
        "spec": {
            "backoffLimit": 2,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": labels(f"{name}-migrate", "migration")},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": "grab-mini",
                    "automountServiceAccountToken": False,
                    "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                    "containers": [{
                        "name": "migrate",
                        "image": f"{IMAGE_REPO}/{name}:latest",
                        "args": ["migrate"],
                        "env": [
                            {"name": "APP_ENV", "value": "production"},
                            {"name": "LOG_FORMAT", "value": "json"},
                            {"name": "DATABASE_URL_FILE", "value": "/etc/secrets/db/url"},
                        ],
                        "volumeMounts": [{"name": "db", "mountPath": "/etc/secrets/db", "readOnly": True}],
                        "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"memory": "128Mi"}},
                        "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                            "runAsNonRoot": True, "capabilities": {"drop": ["ALL"]}},
                    }],
                    "volumes": [{"name": "db", "secret": {"secretName": f"{CLUSTER}-db-{name}", "items": [{"key": "url", "path": "url"}]}}],
                },
            },
        },
    }


def retention_cronjob(name, service, table, column):
    # Deletes in small batches so one run never holds a long lock or bloats
    # the WAL; the loop ends when a batch deletes nothing.
    sql = (f"WITH d AS (DELETE FROM {table} WHERE ctid IN (SELECT ctid FROM {table} "
           f"WHERE {column} < now() - interval '{DEDUPE_RETENTION_DAYS} days' LIMIT 10000) RETURNING 1) "
           f"SELECT count(*) FROM d")
    script = (f'set -eu; url=$(cat /etc/secrets/db/url); '
              f'while :; do n=$(psql "$url" -v ON_ERROR_STOP=1 -At -c "{sql}"); '
              f'echo "deleted $n rows from {table}"; [ "$n" -eq 0 ] && break; done')
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": meta(f"retention-{name}", "retention"),
        "spec": {
            "schedule": "17 3 * * *",
            "concurrencyPolicy": "Forbid",
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 3,
            "jobTemplate": {"spec": {
                "backoffLimit": 1,
                "activeDeadlineSeconds": 1800,
                "template": {
                    "metadata": {"labels": labels(f"retention-{name}", "retention")},
                    "spec": {
                        "restartPolicy": "Never",
                        "serviceAccountName": "grab-mini",
                        "automountServiceAccountToken": False,
                        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                        "containers": [{
                            "name": "retention",
                            "image": POSTGRES_IMAGE,
                            "command": ["sh", "-c", script],
                            "volumeMounts": [{"name": "db", "mountPath": "/etc/secrets/db", "readOnly": True}],
                            "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"memory": "128Mi"}},
                            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                                "runAsNonRoot": True, "runAsUser": 70, "capabilities": {"drop": ["ALL"]}},
                        }],
                        "volumes": [{"name": "db", "secret": {"secretName": f"{CLUSTER}-db-{service}", "items": [{"key": "url", "path": "url"}]}}],
                    },
                },
            }},
        },
    }


# ------------------------------------------------------------- data ops ----
SECURE = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "runAsNonRoot": True, "capabilities": {"drop": ["ALL"]}}


def script_config_map(name, component, paths):
    """The shell scripts of this repository, as a ConfigMap: the Jobs run the very files the tests ran."""
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta(name, component),
            "data": {Path(p).name: Literal((ROOT / p).read_text()) for p in paths}}


def data_ops_config():
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta("grab-mini-data-ops", "config"),
            "data": {
                "RETENTION_DAYS": str(HISTORY_RETENTION_DAYS),
                "MATCHED_ORDER_DAYS": str(MATCHED_ORDER_DAYS),
                "BATCH_SIZE": "5000",
                # Per environment. s3://bucket/prefix; a lifecycle rule on the bucket expires old dumps.
                "BACKUP_TARGET": "s3://CHANGE-ME-grab-mini-backups/postgres",
                "BACKUP_S3_ENDPOINT": "",  # set for S3-compatible storage that is not AWS
                "AWS_DEFAULT_REGION": "us-east-1",
            }}


def history_retention_cronjob(suffix, service, arg):
    name = f"retention-{suffix}"
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": meta(name, "retention"),
        "spec": {
            "schedule": "43 3 * * *",
            "concurrencyPolicy": "Forbid",
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 3,
            "jobTemplate": {"spec": {
                "backoffLimit": 1,
                "activeDeadlineSeconds": 3600,
                "template": {
                    "metadata": {"labels": labels(name, "retention")},
                    "spec": {
                        "restartPolicy": "Never",
                        "serviceAccountName": "grab-mini",
                        "automountServiceAccountToken": False,
                        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                        "containers": [{
                            "name": "retention",
                            "image": POSTGRES_IMAGE,
                            "command": ["sh", "/scripts/pg-retention.sh", arg],
                            "envFrom": [{"configMapRef": {"name": "grab-mini-data-ops"}}],
                            "env": [{"name": "DATABASE_URL_FILE", "value": "/etc/secrets/db/url"}],
                            "volumeMounts": [{"name": "db", "mountPath": "/etc/secrets/db", "readOnly": True},
                                             {"name": "scripts", "mountPath": "/scripts", "readOnly": True}],
                            "resources": {"requests": {"cpu": "50m", "memory": "64Mi"}, "limits": {"memory": "128Mi"}},
                            "securityContext": dict(SECURE, runAsUser=70),
                        }],
                        "volumes": [
                            {"name": "db", "secret": {"secretName": f"{CLUSTER}-db-{service}", "items": [{"key": "url", "path": "url"}]}},
                            {"name": "scripts", "configMap": {"name": "grab-mini-data-ops-scripts"}},
                        ],
                    },
                },
            }},
        },
    }


def backup_cronjob(service):
    """pg_dump in one container, upload in another: no image has both pg_dump and the AWS CLI."""
    name = f"backup-{service}"
    common_env = [{"name": "BACKUP_NAME", "value": service}, {"name": "BACKUP_DIR", "value": "/backup"}]
    mounts = [{"name": "scripts", "mountPath": "/scripts", "readOnly": True}, {"name": "backup", "mountPath": "/backup"}]
    cfg = [{"configMapRef": {"name": "grab-mini-data-ops"}}]
    secret_env = lambda key: {"name": key, "valueFrom": {"secretKeyRef": {"name": f"{CLUSTER}-backup", "key": key}}}
    return {
        "apiVersion": "batch/v1",
        "kind": "CronJob",
        "metadata": meta(name, "backup"),
        "spec": {
            "schedule": BACKUP_SCHEDULE,
            "concurrencyPolicy": "Forbid",
            "successfulJobsHistoryLimit": 1,
            "failedJobsHistoryLimit": 3,
            "jobTemplate": {"spec": {
                "backoffLimit": 1,
                "activeDeadlineSeconds": 3600,
                "template": {
                    "metadata": {"labels": labels(name, "backup")},
                    "spec": {
                        "restartPolicy": "Never",
                        "serviceAccountName": "grab-mini",
                        "automountServiceAccountToken": False,
                        "securityContext": {"seccompProfile": {"type": "RuntimeDefault"}},
                        "initContainers": [{
                            "name": "dump",
                            "image": POSTGRES_IMAGE,
                            "command": ["sh", "/scripts/pg-backup.sh", "dump"],
                            "envFrom": cfg,
                            "env": common_env + [{"name": "DATABASE_URL_FILE", "value": "/etc/secrets/db/url"}],
                            "volumeMounts": mounts + [{"name": "db", "mountPath": "/etc/secrets/db", "readOnly": True}],
                            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                            "securityContext": dict(SECURE, runAsUser=70),
                        }],
                        "containers": [{
                            "name": "upload",
                            "image": AWS_CLI_IMAGE,
                            "command": ["sh", "/scripts/pg-backup.sh", "upload"],
                            "envFrom": cfg,
                            "env": common_env + [{"name": "HOME", "value": "/tmp"}, secret_env("AWS_ACCESS_KEY_ID"), secret_env("AWS_SECRET_ACCESS_KEY")],
                            "volumeMounts": mounts + [{"name": "tmp", "mountPath": "/tmp"}],
                            "resources": {"requests": {"cpu": "50m", "memory": "128Mi"}, "limits": {"memory": "256Mi"}},
                            "securityContext": dict(SECURE, runAsUser=1000),
                        }],
                        "volumes": [
                            {"name": "db", "secret": {"secretName": f"{CLUSTER}-db-{service}", "items": [{"key": "url", "path": "url"}]}},
                            {"name": "scripts", "configMap": {"name": "grab-mini-data-ops-scripts"}},
                            {"name": "backup", "emptyDir": {"sizeLimit": BACKUP_SCRATCH_SIZE}},
                            {"name": "tmp", "emptyDir": {}},
                        ],
                    },
                },
            }},
        },
    }


# ------------------------------------------------------------------ redis ----
def redis_objects():
    """Redis with Sentinel: three pods, each running redis, a sentinel and the exporter.

    Replication is asynchronous and there is ONE master (Sentinel adds
    availability, not capacity). Who is master is decided by the Sentinels at
    run time (deploy/redis/*.sh), so the StatefulSet itself is symmetric.
    """
    sel = {"app.kubernetes.io/name": "redis"}
    domain = "redis-headless.$(POD_NAMESPACE).svc"
    pw = {"name": "REDIS_PASSWORD", "valueFrom": {"secretKeyRef": {"name": f"{CLUSTER}-redis", "key": "password"}}}
    cli_pw = dict(pw, name="REDISCLI_AUTH")  # redis-cli, used by the probes, reads this
    base_env = [{"name": "POD_NAMESPACE", "valueFrom": {"fieldRef": {"fieldPath": "metadata.namespace"}}},
                {"name": "REDIS_DOMAIN", "value": domain}, {"name": "REDIS_MASTER_NAME", "value": REDIS_MASTER_NAME},
                {"name": "REDIS_REPLICAS", "value": str(REDIS_REPLICAS)}, pw, cli_pw]
    ping = lambda port: ["sh", "-c", f"redis-cli -p {port} ping | grep -q PONG"]
    scripts = {"name": "scripts", "mountPath": "/scripts", "readOnly": True}

    redis_c = {
        "name": "redis",
        "image": REDIS_IMAGE,
        "command": ["sh", "/scripts/redis-start.sh"],
        "ports": [{"name": "redis", "containerPort": REDIS_PORT}],
        "env": base_env + [{"name": "REDIS_MAXMEMORY", "value": "400mb"}],
        "volumeMounts": [scripts, {"name": "data", "mountPath": "/data"}, {"name": "conf", "mountPath": "/conf"}],
        # Loading a big AOF takes a while; PING answers LOADING until it is done, so it is not ready.
        "startupProbe": {"exec": {"command": ping(REDIS_PORT)}, "periodSeconds": 5, "failureThreshold": 60},
        "readinessProbe": {"exec": {"command": ping(REDIS_PORT)}, "periodSeconds": 5, "timeoutSeconds": 3, "failureThreshold": 3},
        "livenessProbe": {"exec": {"command": ping(REDIS_PORT)}, "periodSeconds": 10, "timeoutSeconds": 5, "failureThreshold": 6},
        # If this pod is the master, hand the role over before it stops (a drain or rollout
        # then costs a planned failover, not Sentinel noticing a dead master).
        "lifecycle": {"preStop": {"exec": {"command": ["sh", "/scripts/redis-prestop.sh"]}}},
        # maxmemory 400mb + the copy-on-write of an AOF rewrite + replica buffers.
        "resources": {"requests": {"cpu": "250m", "memory": "768Mi"}, "limits": {"memory": "768Mi"}},
        "securityContext": SECURE,
    }
    sentinel_c = {
        "name": "sentinel",
        "image": REDIS_IMAGE,
        "command": ["sh", "/scripts/sentinel-start.sh"],
        "ports": [{"name": "sentinel", "containerPort": SENTINEL_PORT}],
        "env": base_env,
        "volumeMounts": [scripts, {"name": "sentinel", "mountPath": "/sentinel"}],
        "readinessProbe": {"exec": {"command": ping(SENTINEL_PORT)}, "periodSeconds": 5, "timeoutSeconds": 3, "failureThreshold": 3},
        "livenessProbe": {"exec": {"command": ping(SENTINEL_PORT)}, "periodSeconds": 10, "timeoutSeconds": 5, "failureThreshold": 6},
        "resources": {"requests": {"cpu": "50m", "memory": "32Mi"}, "limits": {"memory": "64Mi"}},
        "securityContext": SECURE,
    }
    exporter_c = {
        "name": "exporter",
        "image": REDIS_EXPORTER_IMAGE,
        "ports": [{"name": "metrics", "containerPort": EXPORTER_PORT}],
        "env": [{"name": "REDIS_ADDR", "value": f"redis://localhost:{REDIS_PORT}"}, pw],
        "readinessProbe": {"httpGet": {"path": "/", "port": "metrics"}, "periodSeconds": 10},
        "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"memory": "64Mi"}},
        "securityContext": SECURE,
    }
    statefulset = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": meta("redis", "redis"),
        "spec": {
            "serviceName": "redis-headless",
            "replicas": REDIS_REPLICAS,
            # In order: redis-0 is up (and master) before the others look for a master.
            "podManagementPolicy": "OrderedReady",
            "updateStrategy": {"type": "RollingUpdate"},
            # Deleting the StatefulSet must never delete the data.
            "persistentVolumeClaimRetentionPolicy": {"whenDeleted": "Retain", "whenScaled": "Retain"},
            "selector": {"matchLabels": sel},
            "template": {
                "metadata": {"labels": labels("redis", "redis")},
                "spec": {
                    "serviceAccountName": "grab-mini",
                    "automountServiceAccountToken": False,
                    # Time for the preStop handover and a clean AOF flush.
                    "terminationGracePeriodSeconds": 60,
                    "securityContext": {"runAsNonRoot": True, "runAsUser": 999, "runAsGroup": 1000, "fsGroup": 1000,
                                        "seccompProfile": {"type": "RuntimeDefault"}},
                    # Never two on one node; one per zone (a zone lost = one of three, quorum holds).
                    "affinity": {"podAntiAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": [
                        {"topologyKey": "kubernetes.io/hostname", "labelSelector": {"matchLabels": sel}}]}},
                    "topologySpreadConstraints": [{"maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone",
                                                   "whenUnsatisfiable": "DoNotSchedule", "labelSelector": {"matchLabels": sel}}],
                    "containers": [redis_c, sentinel_c, exporter_c],
                    "volumes": [
                        {"name": "scripts", "configMap": {"name": "redis-scripts"}},
                        {"name": "conf", "emptyDir": {}},
                        {"name": "sentinel", "emptyDir": {}},
                    ],
                },
            },
            "volumeClaimTemplates": [{
                "metadata": {"name": "data", "labels": labels("redis", "redis")},
                # No storageClassName: the cluster's default. Pick a class that is zonal and expandable.
                "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": REDIS_STORAGE}}},
            }],
        },
    }
    headless = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": meta("redis-headless", "redis"),
        "spec": {
            "clusterIP": "None",
            # Peers must resolve each other before they are ready: that is how the cluster forms.
            "publishNotReadyAddresses": True,
            "selector": sel,
            "ports": [{"name": "redis", "port": REDIS_PORT}, {"name": "sentinel", "port": SENTINEL_PORT},
                      {"name": "metrics", "port": EXPORTER_PORT}],
        },
    }
    pdb_ = {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudget",
        "metadata": meta("redis", "redis"),
        # Two of three must stay: Sentinel's quorum, and a replica to fail over to.
        "spec": {"maxUnavailable": 1, "selector": {"matchLabels": sel}},
    }
    return [headless, statefulset, pdb_]


def sentinel_addrs():
    return ",".join(f"redis-{i}.redis-headless:{SENTINEL_PORT}" for i in range(REDIS_REPLICAS))


# ---------------------------------------------------------------- scaling ----
def scaled_object(name, spec, consumes):
    mode, lo, hi = spec["scale"]
    triggers = []
    if mode == "lag":
        for group, topics in consumes.get(name, []):
            for topic in topics:
                triggers.append({
                    "type": "kafka",
                    "authenticationRef": {"name": "keda-kafka-auth"},
                    "metadata": {
                        "bootstrapServers": KAFKA_BOOTSTRAP,
                        "consumerGroup": group,
                        "topic": topic,
                        "lagThreshold": LAG_THRESHOLD.get(topic, "100"),
                        "offsetResetPolicy": "earliest",
                        "sasl": "scram_sha512",
                        "tls": "enable",
                    },
                })
        if spec["kind"] == "api":  # an API also scales with load
            triggers.append({"type": "cpu", "metricType": "Utilization", "metadata": {"value": "70"}})
    else:
        triggers.append({"type": "cpu", "metricType": "Utilization", "metadata": {"value": "70"}})
    return {
        "apiVersion": "keda.sh/v1alpha1",
        "kind": "ScaledObject",
        "metadata": meta(name, "scaling"),
        "spec": {
            "scaleTargetRef": {"name": name},
            "minReplicaCount": lo,
            "maxReplicaCount": hi,
            "pollingInterval": 15,
            "cooldownPeriod": 120,
            "advanced": {"horizontalPodAutoscalerConfig": {"behavior": {
                # Scaling a consumer group in or out rebalances it: shrink slowly.
                "scaleDown": {"stabilizationWindowSeconds": 300, "policies": [{"type": "Pods", "value": 1, "periodSeconds": 60}]},
            }}},
            "triggers": triggers,
        },
    }


def trigger_authentication():
    return {
        "apiVersion": "keda.sh/v1alpha1",
        "kind": "TriggerAuthentication",
        "metadata": meta("keda-kafka-auth", "scaling"),
        "spec": {"secretTargetRef": [
            # A Secret made out of band from the "keda" Kafka user's credentials and the cluster CA.
            {"parameter": "username", "name": "keda-kafka-auth", "key": "username"},
            {"parameter": "password", "name": "keda-kafka-auth", "key": "password"},
            {"parameter": "ca", "name": "keda-kafka-auth", "key": "ca.crt"},
        ]},
    }


# ----------------------------------------------------------------- kafka ----
def kafka_cluster():
    def pool(name, roles, replicas, size):
        spec = {
            "replicas": replicas,
            "roles": roles,
            "storage": {"type": "jbod", "volumes": [{
                "id": 0, "type": "persistent-claim", "size": size, "deleteClaim": False, "kraftMetadata": "shared"}]},
            "resources": {"requests": {"cpu": "500m", "memory": "2Gi"}, "limits": {"memory": "2Gi"}},
        }
        if "broker" in roles:
            spec["template"] = {"pod": {"topologySpreadConstraints": [{
                "maxSkew": 1, "topologyKey": "topology.kubernetes.io/zone", "whenUnsatisfiable": "DoNotSchedule",
                "labelSelector": {"matchLabels": {"strimzi.io/cluster": CLUSTER, "strimzi.io/pool-name": name}}}]}}
        return {
            "apiVersion": "kafka.strimzi.io/v1beta2",
            "kind": "KafkaNodePool",
            "metadata": {"name": name, "labels": {"strimzi.io/cluster": CLUSTER}},
            "spec": spec,
        }

    kafka = {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "Kafka",
        "metadata": {"name": CLUSTER, "annotations": {"strimzi.io/node-pools": "enabled", "strimzi.io/kraft": "enabled"}},
        "spec": {
            "kafka": {
                "version": "3.8.0",
                "metadataVersion": "3.8-IV0",
                # One broker per availability zone: a replica of every partition
                # lives in each, so losing a zone loses one replica of three.
                "rack": {"topologyKey": "topology.kubernetes.io/zone"},
                "listeners": [{"name": "tls", "port": 9093, "type": "internal", "tls": True,
                               "authentication": {"type": "scram-sha-512"}}],
                "authorization": {"type": "simple", "superUsers": ["ops"]},
                "config": {
                    "default.replication.factor": 3,
                    "min.insync.replicas": 2,
                    "offsets.topic.replication.factor": 3,
                    "transaction.state.log.replication.factor": 3,
                    "transaction.state.log.min.isr": 2,
                    # A replica that fell behind must never become leader: that loses acknowledged writes.
                    "unclean.leader.election.enable": False,
                    # Topics are declared (KafkaTopic) or created by a service with the ACL to do so.
                    "auto.create.topics.enable": False,
                    "group.initial.rebalance.delay.ms": 3000,
                },
            },
            "entityOperator": {"topicOperator": {}, "userOperator": {}},
            # Per-group lag and per-partition ISR, which the alerts use.
            "kafkaExporter": {"topicRegex": ".*", "groupRegex": ".*"},
        },
    }
    return [pool("controller", ["controller"], 3, "10Gi"), pool("broker", ["broker"], 3, "100Gi"), kafka]


def kafka_topics():
    out = []
    for name, (partitions, retention, isr) in TOPICS.items():
        out.append({
            "apiVersion": "kafka.strimzi.io/v1beta2",
            "kind": "KafkaTopic",
            "metadata": {"name": name.replace(".", "-").replace("_", "-"), "labels": {"strimzi.io/cluster": CLUSTER}},
            "spec": {"topicName": name, "partitions": partitions, "replicas": 3,
                     "config": {"retention.ms": retention, "min.insync.replicas": isr, "compression.type": "producer"}},
        })
    return out


def kafka_users(produces, consumes):
    users = []
    for svc in sorted(set(produces) | set(consumes)):
        acls = []
        for topic in produces.get(svc, []):
            acls.append({"resource": {"type": "topic", "name": topic, "patternType": "literal"}, "operations": ["Write", "Describe"]})
        for group, topics in consumes.get(svc, []):
            acls.append({"resource": {"type": "group", "name": group, "patternType": "prefix"}, "operations": ["Read"]})
            for topic in topics:
                acls.append({"resource": {"type": "topic", "name": topic, "patternType": "literal"}, "operations": ["Read", "Describe"]})
                acls.append({"resource": {"type": "topic", "name": topic + ".", "patternType": "prefix"},
                             "operations": ["Read", "Write", "Describe", "Create"]})
        users.append(kafka_user(svc, acls))
    # KEDA reads group offsets to compute lag; nothing else.
    keda = [{"resource": {"type": "group", "name": "*", "patternType": "literal"}, "operations": ["Describe"]},
            {"resource": {"type": "topic", "name": "*", "patternType": "literal"}, "operations": ["Describe"]}]
    users.append(kafka_user("keda", keda))
    # People who run dlq-redrive and inspect topics: a super user in the Kafka resource above.
    users.append(kafka_user("ops", None))
    return users


def kafka_user(name, acls):
    spec = {"authentication": {"type": "scram-sha-512"}}
    if acls is not None:
        spec["authorization"] = {"type": "simple", "acls": acls}
    return {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaUser",
        "metadata": {"name": name, "labels": {"strimzi.io/cluster": CLUSTER}},
        "spec": spec,
    }


# ------------------------------------------------- network and monitoring ----
# Where each dependency is reached. None of Postgres, Redis, the Schema Registry or
# the identity provider is deployed by this base, so each is allowed as either an
# in-namespace pod labelled app.kubernetes.io/name=<name> or an address in the
# private ranges on that port only. Narrow PRIVATE_CIDRS (a kustomize patch) to
# the real endpoints of a managed service to tighten this further.
PRIVATE_CIDRS = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
POSTGRES_PORTS = [5432]
REDIS_PORTS = [6379, 26379]  # 26379: Sentinel
REDIS_EXPORTER_PORT = 9121
REGISTRY_PORTS = [8081]
OTLP_PORTS = [4317, 4318]
AUTH_SERVICE_PORT = 3005


def policy(name, pod_selector, egress=None, ingress=None):
    types = (["Ingress"] if ingress is not None else []) + (["Egress"] if egress is not None else [])
    spec = {"podSelector": pod_selector, "policyTypes": types}
    if ingress:
        spec["ingress"] = ingress
    if egress:
        spec["egress"] = egress
    return {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": meta(name, "network"), "spec": spec}


def tcp(numbers):
    return [{"port": p, "protocol": "TCP"} for p in numbers]


def in_namespace(name):
    return {"podSelector": {"matchLabels": {"app.kubernetes.io/name": name}}}


def private_ranges():
    return [{"ipBlock": {"cidr": c}} for c in PRIVATE_CIDRS]


def names_in(names):
    return {"matchExpressions": [{"key": "app.kubernetes.io/name", "operator": "In", "values": sorted(names)}]}


def network_policies():
    part_of = {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini"}}
    services = {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini", "app.kubernetes.io/component": "service"}}
    jobs = {"matchExpressions": [{"key": "app.kubernetes.io/part-of", "operator": "In", "values": ["grab-mini"]},
                                 {"key": "app.kubernetes.io/component", "operator": "In", "values": ["migration", "retention"]}]}
    with_db = [n for n, sp in SERVICES.items() if sp.get("db")]
    with_redis = [n for n, sp in SERVICES.items() if sp.get("redis")]
    verifiers = [n for n, sp in SERVICES.items() if sp["kind"] == "api"]  # these check tokens, so they fetch the JWKS
    redis_pods = {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini", "app.kubernetes.io/component": "redis"}}
    backups = {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini", "app.kubernetes.io/component": "backup"}}

    return [
        policy("default-deny-ingress", part_of, ingress=[]),
        policy("allow-ingress-to-public-apis", {"matchLabels": {"grab-mini/public": "true"}},
               ingress=[{"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}}}],
                         "ports": [{"port": "http", "protocol": "TCP"}]}]),
        # Only the admin port (probes and /metrics), only from monitoring. The
        # kubelet's probes come from the node and are not subject to this.
        policy("allow-prometheus-scrape", part_of,
               ingress=[{"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}],
                         "ports": [{"port": ADMIN_PORT, "protocol": "TCP"}]}]),

        # Egress: nothing leaves a pod unless a policy below says so (policies add up).
        policy("default-deny-egress", part_of, egress=[]),
        policy("allow-egress-dns", part_of, egress=[{
            "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                    "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
            "ports": [{"port": 53, "protocol": "UDP"}, {"port": 53, "protocol": "TCP"}]}]),
        policy("allow-egress-kafka", services, egress=[{
            "to": [{"podSelector": {"matchLabels": {"strimzi.io/cluster": CLUSTER, "strimzi.io/kind": "Kafka"}}}],
            "ports": tcp([9093])}]),
        policy("allow-egress-schema-registry", services, egress=[{
            "to": [in_namespace("schema-registry")] + private_ranges(), "ports": tcp(REGISTRY_PORTS)}]),
        policy("allow-egress-otlp", services, egress=[{
            "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}], "ports": tcp(OTLP_PORTS)}]),
        policy("allow-egress-redis", names_in(with_redis), egress=[{
            "to": [in_namespace("redis")] + private_ranges(), "ports": tcp(REDIS_PORTS)}]),
        policy("allow-egress-postgres", names_in(with_db), egress=[{
            "to": [in_namespace("postgres")] + private_ranges(), "ports": tcp(POSTGRES_PORTS)}]),
        # Migration Jobs and retention CronJobs reach only Postgres (and DNS).
        policy("allow-egress-postgres-jobs", jobs, egress=[{
            "to": [in_namespace("postgres")] + private_ranges(), "ports": tcp(POSTGRES_PORTS)}]),
        # The JWKS: an Auth Service in this namespace, or an identity provider outside the
        # cluster over https (anywhere but the private ranges and the metadata address).
        policy("allow-egress-jwks", names_in(verifiers), egress=[
            {"to": [in_namespace("auth-service")], "ports": tcp([AUTH_SERVICE_PORT])},
            {"to": [{"ipBlock": {"cidr": "0.0.0.0/0", "except": PRIVATE_CIDRS + ["169.254.0.0/16"]}}], "ports": tcp([443])}]),

        # Redis (redis/): its clients and its own replicas/Sentinels talk to it, monitoring
        # scrapes the exporter, and the pods reach each other for replication and Sentinel.
        policy("allow-ingress-to-redis", redis_pods,
               ingress=[{"from": [{"podSelector": names_in(with_redis)}, {"podSelector": redis_pods}], "ports": tcp(REDIS_PORTS)},
                        {"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}],
                         "ports": tcp([REDIS_EXPORTER_PORT])}]),
        policy("allow-egress-redis-peers", redis_pods, egress=[{"to": [{"podSelector": redis_pods}], "ports": tcp(REDIS_PORTS)}]),

        # Backup CronJobs (backup/): pg_dump from Postgres, upload to object storage over https
        # (or BACKUP_S3_ENDPOINT, which may be in a private range).
        policy("allow-egress-backup", backups, egress=[
            {"to": [in_namespace("postgres")] + private_ranges(), "ports": tcp(POSTGRES_PORTS)},
            {"to": [{"ipBlock": {"cidr": "0.0.0.0/0", "except": ["169.254.0.0/16"]}}], "ports": tcp([443])}]),
    ]


def pod_monitors():
    return [
        {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": meta("grab-mini-services", "monitoring"),
         "spec": {"selector": {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini", "app.kubernetes.io/component": "service"}},
                  "podMetricsEndpoints": [{"port": "admin", "path": "/metrics", "interval": "15s"}]}},
        {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": meta("grab-mini-kafka-exporter", "monitoring"),
         "spec": {"selector": {"matchLabels": {"strimzi.io/name": f"{CLUSTER}-kafka-exporter"}},
                  "podMetricsEndpoints": [{"port": "tcp-prometheus", "path": "/metrics", "interval": "30s"}]}},
        {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": meta("grab-mini-redis", "monitoring"),
         "spec": {"selector": {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini", "app.kubernetes.io/component": "redis"}},
                  "podMetricsEndpoints": [{"port": "metrics", "path": "/metrics", "interval": "15s"}]}},
    ]


def prometheus_rule():
    rules = yaml.safe_load((ROOT / "deploy" / "monitoring" / "alerts.rules.yaml").read_text())
    return {"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
            "metadata": meta("grab-mini-alerts", "monitoring"), "spec": rules}


def config_map():
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": "grab-mini-config", "labels": labels("grab-mini-config", "config")},
            "data": {
                "APP_ENV": "production",
                "LOG_FORMAT": "json",
                "KAFKA_BROKERS": KAFKA_BOOTSTRAP,
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "SCRAM-SHA-512",
                "KAFKA_REPLICATION_FACTOR": "3",
                "KAFKA_TOPIC_PARTITIONS": "6",
                # These point at things this repository doesn't deploy; set them per environment.
                "SCHEMA_REGISTRY_URL": "https://schema-registry:8081",
                # Where the services fetch the public keys that verify tokens: the identity provider's
                # JWKS (Auth Service serves it at /.well-known/jwks.json). No service holds a signing key.
                "JWT_JWKS_URL": "https://auth.example.com/.well-known/jwks.json",
                # Redis is deployed here (redis/): the services ask its Sentinels for the master and follow
                # failovers on their own; REDIS_ADDR is then ignored. To use a managed Redis instead, empty
                # REDIS_SENTINEL_ADDRS and set REDIS_ADDR (and drop redis/ from the kustomization).
                "REDIS_ADDR": "redis:6379",
                "REDIS_SENTINEL_ADDRS": sentinel_addrs(),
                "REDIS_MASTER_NAME": REDIS_MASTER_NAME,
                # Payment Service's money rules (read only by it): the platform's cut of a fare in basis
                # points; the fee a rider pays for cancelling after a driver was assigned, and how many
                # seconds after the assignment cancelling is still free. Fee 0 makes cancelling free.
                "PLATFORM_COMMISSION_BPS": "2000",
                "CANCELLATION_FEE_VND": "10000",
                "CANCELLATION_GRACE_SECONDS": "180",
                # Cell size (degrees) to split the driver GEO set into; 0 = one key. Matching and
                # Driver Location Service must agree: see docs/performance.md before changing it.
                "GEO_SHARD_DEGREES": "0",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.monitoring:4318",
                "ALLOWED_ORIGINS": "https://app.example.com",
            }}


# ------------------------------------------------------------------ main ----
class Dumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


class Literal(str):
    """Dumped as a block scalar: a shell script in a ConfigMap reads as a script, not one escaped line."""


Dumper.add_representer(Literal, lambda d, v: d.represent_scalar("tag:yaml.org,2002:str", str(v), style="|"))


def dump(docs):
    if not isinstance(docs, list):
        docs = [docs]
    return "---\n".join(yaml.dump(d, Dumper=Dumper, sort_keys=False, default_flow_style=False, width=120) for d in docs)


def build():
    produces, consumes = parse_acl_table()
    files = {}
    files["namespace.yaml"] = dump({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "grab-mini"}})
    files["config.yaml"] = dump([config_map(), {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": "grab-mini"}}])
    for name, spec in SERVICES.items():
        docs = [deployment(name, spec)]
        if spec["kind"] == "api":
            docs.append(service(name, spec))
        docs.append(pdb(name, spec))
        files[f"services/{name}.yaml"] = dump(docs)
    for name in MIGRATING:
        files[f"migrations/{name}.yaml"] = dump(migration_job(name))
    for name, (owner, table, column) in RETENTION.items():
        files[f"retention/{name}.yaml"] = dump(retention_cronjob(name, owner, table, column))
    for suffix, (owner, arg) in HISTORY_RETENTION.items():
        files[f"retention/{suffix}.yaml"] = dump(history_retention_cronjob(suffix, owner, arg))
    files["dataops/config.yaml"] = dump([data_ops_config(), script_config_map(
        "grab-mini-data-ops-scripts", "dataops", ["scripts/pg-retention.sh", "scripts/pg-backup.sh"])])
    for svc in MIGRATING:
        files[f"backup/{svc}.yaml"] = dump(backup_cronjob(svc))
    files["redis/redis.yaml"] = dump(redis_objects())
    files["redis/scripts.yaml"] = dump(script_config_map(
        "redis-scripts", "redis", [f"deploy/redis/{s}" for s in ("redis-start.sh", "sentinel-start.sh", "redis-prestop.sh")]))
    scaled = [trigger_authentication()] + [scaled_object(n, s, consumes) for n, s in SERVICES.items()]
    files["scaling/keda.yaml"] = dump(scaled)
    files["kafka/cluster.yaml"] = dump(kafka_cluster())
    files["kafka/topics.yaml"] = dump(kafka_topics())
    files["kafka/users.yaml"] = dump(kafka_users(produces, consumes))
    files["network/policies.yaml"] = dump(network_policies())
    files["monitoring/podmonitors.yaml"] = dump(pod_monitors())
    files["monitoring/alerts.yaml"] = dump(prometheus_rule())
    resources = ["namespace.yaml", "config.yaml"] + sorted(f for f in files if f.startswith(("services/", "migrations/", "retention/", "scaling/", "kafka/", "network/", "monitoring/", "dataops/", "backup/", "redis/")))
    files["kustomization.yaml"] = dump({
        "apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
        "namespace": "grab-mini", "resources": resources,
    })
    return files


def main():
    files = build()
    header = "# Generated by deploy/k8s/generate.py: edit that, not this.\n"
    check = "--check" in sys.argv
    stale = []
    for rel, body in sorted(files.items()):
        path = OUT / rel
        content = header + body
        if check:
            if not path.exists() or path.read_text() != content:
                stale.append(rel)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    if check:
        extra = sorted(str(p.relative_to(OUT)) for p in OUT.rglob("*.yaml") if str(p.relative_to(OUT)) not in files) if OUT.exists() else []
        if stale or extra:
            sys.exit("generated files are out of date: %s %s\nrun: python3 deploy/k8s/generate.py" % (stale, extra))
        print("deploy/k8s/base is up to date")
    else:
        print(f"wrote {len(files)} files under {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()

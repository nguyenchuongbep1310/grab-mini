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
    "trip.status.changed": (12, 7 * DAY, 2),
    "trip.rated": (6, 7 * DAY, 2),
    "driver.location.updated": (48, 1 * DAY, 1),
}

# ------------------------------------------------------------- services ----
# kind: "api" has a public port; "worker" has only the admin port.
# secrets: what it mounts besides its Kafka credentials.
SERVICES = {
    "order-service": dict(port=3001, kind="api", db=True, redis=False, scale=("lag", 2, 6)),
    "trip-service": dict(port=3002, kind="api", db=True, redis=True, scale=("lag", 2, 12)),
    "rating-service": dict(port=3004, kind="api", db=True, redis=False, scale=("lag", 2, 6)),
    "notification-service": dict(port=3003, kind="api", db=False, redis=True, scale=("lag", 2, 24), origins=True),
    "location-gateway": dict(port=3006, kind="api", db=False, redis=False, scale=("cpu", 3, 30)),
    "matching-service": dict(port=None, kind="worker", db=False, redis=True, scale=("lag", 2, 12)),
    "driver-location-service": dict(port=None, kind="worker", db=False, redis=True, scale=("lag", 3, 24)),
}
LAG_THRESHOLD = {"driver.location.updated": "5000"}  # messages behind per replica before scaling out; default 100

# Which services have migrations (and so a migration Job).
MIGRATING = ["order-service", "trip-service", "rating-service"]


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
    if api:  # these verify tokens
        volumes.append({"name": "jwt", "secret": {"secretName": f"{CLUSTER}-jwt", "items": [{"key": "secret", "path": "secret"}]}})
        mounts.append({"name": "jwt", "mountPath": "/etc/secrets/jwt", "readOnly": True})
        env.append({"name": "JWT_SECRET_FILE", "value": "/etc/secrets/jwt/secret"})
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
def network_policies():
    return [
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": meta("default-deny-ingress", "network"),
         "spec": {"podSelector": {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini"}}, "policyTypes": ["Ingress"]}},
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": meta("allow-ingress-to-public-apis", "network"),
         "spec": {"podSelector": {"matchLabels": {"grab-mini/public": "true"}}, "policyTypes": ["Ingress"],
                  "ingress": [{"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}}}],
                               "ports": [{"port": "http", "protocol": "TCP"}]}]}},
        # Only the admin port (probes and /metrics), only from monitoring. The
        # kubelet's probes come from the node and are not subject to this.
        {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy", "metadata": meta("allow-prometheus-scrape", "network"),
         "spec": {"podSelector": {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini"}}, "policyTypes": ["Ingress"],
                  "ingress": [{"from": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "monitoring"}}}],
                               "ports": [{"port": ADMIN_PORT, "protocol": "TCP"}]}]}},
    ]


def pod_monitors():
    return [
        {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": meta("grab-mini-services", "monitoring"),
         "spec": {"selector": {"matchLabels": {"app.kubernetes.io/part-of": "grab-mini", "app.kubernetes.io/component": "service"}},
                  "podMetricsEndpoints": [{"port": "admin", "path": "/metrics", "interval": "15s"}]}},
        {"apiVersion": "monitoring.coreos.com/v1", "kind": "PodMonitor", "metadata": meta("grab-mini-kafka-exporter", "monitoring"),
         "spec": {"selector": {"matchLabels": {"strimzi.io/name": f"{CLUSTER}-kafka-exporter"}},
                  "podMetricsEndpoints": [{"port": "tcp-prometheus", "path": "/metrics", "interval": "30s"}]}},
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
                "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
                "REDIS_ADDR": "redis:6379",
                "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector.monitoring:4318",
                "ALLOWED_ORIGINS": "https://app.example.com",
            }}


# ------------------------------------------------------------------ main ----
class Dumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True


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
    scaled = [trigger_authentication()] + [scaled_object(n, s, consumes) for n, s in SERVICES.items()]
    files["scaling/keda.yaml"] = dump(scaled)
    files["kafka/cluster.yaml"] = dump(kafka_cluster())
    files["kafka/topics.yaml"] = dump(kafka_topics())
    files["kafka/users.yaml"] = dump(kafka_users(produces, consumes))
    files["network/policies.yaml"] = dump(network_policies())
    files["monitoring/podmonitors.yaml"] = dump(pod_monitors())
    files["monitoring/alerts.yaml"] = dump(prometheus_rule())
    resources = ["namespace.yaml", "config.yaml"] + sorted(f for f in files if f.startswith(("services/", "migrations/", "scaling/", "kafka/", "network/", "monitoring/")))
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

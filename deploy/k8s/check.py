#!/usr/bin/env python3
"""Cross-reference checks a schema can't make, on the rendered manifests.

    kustomize build deploy/k8s/base | python3 deploy/k8s/check.py

It reads the Go source too, so the manifests can't drift from the code:
 - every environment variable a Deployment sets is one the services read;
 - every consumer group in the code has Kafka ACLs, and every KEDA trigger names a real one;
 - every topic constant has a KafkaTopic;
 - the names that must agree (users, secrets, selectors, volumes, ports) agree.
"""
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
docs = [d for d in yaml.safe_load_all(sys.stdin) if d]
errors = []


def err(msg):
    errors.append(msg)


def of(kind):
    return [d for d in docs if d["kind"] == kind]


def name(d):
    return d["metadata"]["name"]


deployments = {name(d): d for d in of("Deployment")}
users = {name(d) for d in of("KafkaUser")}
scaled = {d["spec"]["scaleTargetRef"]["name"]: d for d in of("ScaledObject")}
configmaps = {name(d): d["data"] for d in of("ConfigMap")}
config = configmaps["grab-mini-config"]
statefulsets = {name(d): d for d in of("StatefulSet")}
topics = {d["spec"]["topicName"] for d in of("KafkaTopic")}

# ---------------------------------------------------------------- the code ----
go_files = [p for p in (ROOT / "services").rglob("*.go") if not p.name.endswith("_test.go")]
source = "\n".join(p.read_text() for p in go_files)
env_names_in_code = set(re.findall(r'"([A-Z][A-Z0-9_]{2,})"', source))
groups_in_code = set(re.findall(r'GroupID:\s*"([^"]+)"', source))
topics_in_code = set(re.findall(r'Topic\w+\s*=\s*"([a-z_.]+)"', (ROOT / "services/pkg/events/topics.go").read_text()))

# --------------------------------------------------------------- deployments ----
for dep_name, dep in deployments.items():
    pod = dep["spec"]["template"]
    spec = pod["spec"]
    c = spec["containers"][0]
    labels = pod["metadata"]["labels"]

    if dep["spec"]["selector"]["matchLabels"].items() > labels.items():
        err(f"{dep_name}: the selector isn't a subset of the pod labels")

    volumes = {v["name"] for v in spec.get("volumes", [])}
    for m in c.get("volumeMounts", []):
        if m["name"] not in volumes:
            err(f"{dep_name}: mounts volume {m['name']!r} that the pod doesn't define")
    for v in spec.get("volumes", []):
        if "secret" in v and v["secret"]["secretName"] == dep_name and dep_name not in users:
            err(f"{dep_name}: mounts the Kafka user secret {dep_name!r} but there is no KafkaUser of that name")

    env = {e["name"]: e for e in c.get("env", [])}
    if env.get("KAFKA_USERNAME", {}).get("value") != dep_name:
        err(f"{dep_name}: KAFKA_USERNAME must be its own name (one principal per service)")

    for name_ in env:
        base = name_[:-5] if name_.endswith("_FILE") else name_
        if base not in env_names_in_code and name_ not in env_names_in_code:
            err(f"{dep_name}: sets {name_}, which no service reads (a typo?)")
    for key in config:
        base = key[:-5] if key.endswith("_FILE") else key
        if base not in env_names_in_code:
            err(f"the ConfigMap sets {key}, which no service reads")
    for e in env.values():
        ref = e.get("valueFrom", {}).get("configMapKeyRef")
        if ref and ref["key"] not in config:
            err(f"{dep_name}: {e['name']} reads ConfigMap key {ref['key']!r} that doesn't exist")

    port_names = {p["name"] for p in c["ports"]}
    for probe in ("startupProbe", "livenessProbe", "readinessProbe"):
        p = c[probe]["httpGet"]["port"]
        if p not in port_names:
            err(f"{dep_name}: {probe} uses port {p!r} which the container doesn't declare")
    if "admin" not in port_names:
        err(f"{dep_name}: no admin port: probes and /metrics must not share the public port")

    if dep_name not in scaled:
        err(f"{dep_name}: no ScaledObject")
    pdbs = [p for p in of("PodDisruptionBudget") if p["spec"]["selector"]["matchLabels"]["app.kubernetes.io/name"] == dep_name]
    if not pdbs:
        err(f"{dep_name}: no PodDisruptionBudget")
    if dep["spec"]["replicas"] < 2:
        err(f"{dep_name}: fewer than 2 replicas")

for svc in of("Service"):
    if svc["spec"]["selector"]["app.kubernetes.io/name"] in statefulsets:
        continue  # checked with the StatefulSet below
    dep = deployments.get(svc["spec"]["selector"]["app.kubernetes.io/name"])
    if not dep:
        err(f"Service {name(svc)}: selects no Deployment")
        continue
    ports = {p["name"] for p in dep["spec"]["template"]["spec"]["containers"][0]["ports"]}
    for p in svc["spec"]["ports"]:
        if p["targetPort"] not in ports:
            err(f"Service {name(svc)}: targetPort {p['targetPort']!r} isn't a port of the container")

# ------------------------------------------------------------------- scaling ----
for dep_name, so in scaled.items():
    if dep_name not in deployments:
        err(f"ScaledObject {dep_name}: no such Deployment")
    for t in so["spec"]["triggers"]:
        if t["type"] != "kafka":
            continue
        md = t["metadata"]
        if md["consumerGroup"] not in groups_in_code:
            err(f"ScaledObject {dep_name}: scales on group {md['consumerGroup']!r}, which no code uses")
        if md["topic"] not in topics:
            err(f"ScaledObject {dep_name}: scales on topic {md['topic']!r}, which has no KafkaTopic")
    lo, hi = so["spec"]["minReplicaCount"], so["spec"]["maxReplicaCount"]
    if lo > hi:
        err(f"ScaledObject {dep_name}: min {lo} > max {hi}")

# --------------------------------------------------------------------- kafka ----
for t in sorted(topics_in_code - topics):
    err(f"topic {t!r} is in services/pkg/events/topics.go but has no KafkaTopic")
for t in sorted(topics - topics_in_code):
    err(f"KafkaTopic {t!r} isn't a topic the code knows")

acl_groups = set()
for u in of("KafkaUser"):
    for acl in u["spec"].get("authorization", {}).get("acls", []):
        r = acl["resource"]
        if r["type"] == "group" and name(u) != "keda":
            acl_groups.add(r["name"])
for g in sorted(groups_in_code - acl_groups):
    err(f"consumer group {g!r} is used in the code but no KafkaUser may read it: the service would be refused")
for g in sorted(acl_groups - groups_in_code):
    err(f"a KafkaUser may read group {g!r}, which no code uses")
for dep_name in deployments:
    if dep_name not in users:
        err(f"{dep_name}: no KafkaUser")

for t in of("KafkaTopic"):
    c = t["spec"]["config"]
    if t["spec"]["replicas"] < 3 or c["min.insync.replicas"] > t["spec"]["replicas"]:
        err(f"KafkaTopic {name(t)}: replicas/min.insync.replicas can't survive a broker loss")

# ------------------------------------------------------------------ network ----
def selects(selector, labels):
    for k, v in selector.get("matchLabels", {}).items():
        if labels.get(k) != v:
            return False
    for e in selector.get("matchExpressions", []):
        have = labels.get(e["key"])
        if e["operator"] == "In" and have not in e["values"]:
            return False
        if e["operator"] == "NotIn" and have in e["values"]:
            return False
    return True


def pods():
    """Labels of every kind of pod the base runs, by a readable name."""
    out = {}
    for d in of("Deployment"):
        out[name(d)] = d["spec"]["template"]["metadata"]["labels"]
    for j in of("Job"):
        out[name(j)] = j["spec"]["template"]["metadata"]["labels"]
    for j in of("CronJob"):
        out[name(j)] = j["spec"]["jobTemplate"]["spec"]["template"]["metadata"]["labels"]
    for st in of("StatefulSet"):
        out[name(st)] = st["spec"]["template"]["metadata"]["labels"]
    return out


def egress_ports(pol):
    return {(p["port"], p.get("protocol", "TCP")) for r in pol["spec"].get("egress", []) for p in r.get("ports", [])}


policies = {name(p): p for p in of("NetworkPolicy")}
egress_policies = [p for p in policies.values() if "Egress" in p["spec"].get("policyTypes", [])]
deny = policies.get("default-deny-egress")
if not deny or deny["spec"].get("egress"):
    err("default-deny-egress must exist and allow nothing itself")
for pod_name, labels in pods().items():
    if deny and not selects(deny["spec"]["podSelector"], labels):
        err(f"{pod_name}: not covered by default-deny-egress")

    def may(port, proto="TCP", pod_name=pod_name, labels=labels):
        return any(selects(p["spec"]["podSelector"], labels) and (port, proto) in egress_ports(p) for p in egress_policies)

    if not may(53, "UDP") or not may(53, "TCP"):
        err(f"{pod_name}: no egress to DNS")
    if pod_name in deployments:
        if not may(9093):
            err(f"{pod_name}: no egress to Kafka")
        for port, why in ((4318, "the OTLP collector"), (8081, "the Schema Registry")):
            if not may(port):
                err(f"{pod_name}: no egress to {why}")
        volumes = {v["name"] for v in deployments[pod_name]["spec"]["template"]["spec"]["volumes"]}
        if "db" in volumes and not may(5432):
            err(f"{pod_name}: has a database but no egress to Postgres")
        if labels.get("grab-mini/public") == "true" and not may(443):
            err(f"{pod_name}: verifies tokens but has no egress to the JWKS")
    elif pod_name.endswith("-migrate") or pod_name.startswith("retention-"):
        if not may(5432):
            err(f"{pod_name}: no egress to Postgres")
        if may(9093):
            err(f"{pod_name}: a job should not reach Kafka")
    elif pod_name.startswith("backup-"):
        if not may(5432):
            err(f"{pod_name}: no egress to Postgres (pg_dump)")
        if not may(443):
            err(f"{pod_name}: no egress to object storage (443)")
        if may(9093):
            err(f"{pod_name}: a job should not reach Kafka")
    elif labels.get("app.kubernetes.io/component") == "redis":
        if not may(6379) or not may(26379):
            err(f"{pod_name}: Redis pods can't reach each other (replication, Sentinel)")

# Nothing may mount a token-signing key except the issuer, which this base doesn't deploy.
for dep_name, dep in deployments.items():
    for v in dep["spec"]["template"]["spec"].get("volumes", []):
        sec = v.get("secret", {}).get("secretName", "")
        if "jwt" in sec:
            err(f"{dep_name}: mounts {sec!r}: services verify with the JWKS and must hold no token key")
    for e in dep["spec"]["template"]["spec"]["containers"][0].get("env", []):
        if e["name"] in ("JWT_SECRET", "JWT_SECRET_FILE", "JWT_PRIVATE_KEY", "JWT_PRIVATE_KEY_FILE"):
            err(f"{dep_name}: sets {e['name']}: only the token issuer may hold signing key material")
if "JWT_JWKS_URL" not in config:
    err("the ConfigMap has no JWT_JWKS_URL: the API services couldn't verify tokens")

# --------------------------------------------------------------------- redis ----
def mounts_secret(pod_spec, secret):
    return any(v.get("secret", {}).get("secretName") == secret for v in pod_spec.get("volumes", []))


def unpinned(image):
    return ":" not in image.rsplit("/", 1)[-1] or image.endswith(":latest")


# Which services use Redis is read from the code (they build their client with pkg/redisconn).
uses_redis = {n for n in deployments
              if any("redisconn." in p.read_text() for p in (ROOT / "services" / n).glob("*.go") if not p.name.endswith("_test.go"))}
sentinels = [a for a in config.get("REDIS_SENTINEL_ADDRS", "").split(",") if a]
for dep_name in sorted(uses_redis):
    pod = deployments[dep_name]["spec"]["template"]["spec"]
    env = {e["name"] for e in pod["containers"][0].get("env", [])}
    if sentinels:
        for var in ("REDIS_PASSWORD_FILE", "REDIS_SENTINEL_PASSWORD_FILE"):
            if var not in env:
                err(f"{dep_name}: uses Redis with Sentinel but doesn't set {var}")
        if not mounts_secret(pod, "grab-mini-redis"):
            err(f"{dep_name}: uses Redis but doesn't mount the grab-mini-redis Secret")
for dep_name in sorted(set(deployments) - uses_redis):
    if mounts_secret(deployments[dep_name]["spec"]["template"]["spec"], "grab-mini-redis"):
        err(f"{dep_name}: mounts the Redis password but doesn't use Redis")

if sentinels:
    sts = statefulsets.get("redis")
    if not sts:
        err("REDIS_SENTINEL_ADDRS is set but there is no StatefulSet 'redis'")
    else:
        spec = sts["spec"]
        pod = spec["template"]["spec"]
        replicas = spec["replicas"]
        cs = {c["name"]: c for c in pod["containers"]}
        if replicas < 3 or replicas % 2 == 0:
            err(f"redis: {replicas} replicas can't keep a Sentinel quorum (need an odd number, at least 3)")
        headless = [s for s in of("Service") if name(s) == spec["serviceName"]]
        if not headless or headless[0]["spec"].get("clusterIP") != "None":
            err(f"redis: serviceName {spec['serviceName']!r} isn't a headless Service")
        sentinel_port = {p["name"]: p["containerPort"] for p in cs["sentinel"]["ports"]}["sentinel"]
        want = {f"redis-{i}.{spec['serviceName']}:{sentinel_port}" for i in range(replicas)}
        if set(sentinels) != want:
            err(f"REDIS_SENTINEL_ADDRS is {sorted(sentinels)}, the StatefulSet's pods are {sorted(want)}")
        if config.get("REDIS_MASTER_NAME") != next((e["value"] for e in cs["redis"]["env"] if e["name"] == "REDIS_MASTER_NAME"), None):
            err("redis: REDIS_MASTER_NAME in the ConfigMap and in the StatefulSet differ: the clients would ask for a master that doesn't exist")
        pdb = [p for p in of("PodDisruptionBudget") if p["spec"]["selector"]["matchLabels"].items() <= sts["spec"]["template"]["metadata"]["labels"].items()
               and name(p) == "redis"]
        if not pdb or pdb[0]["spec"].get("maxUnavailable", replicas) > replicas - replicas // 2 - 1:
            err("redis: no PodDisruptionBudget that keeps a Sentinel majority")
        aff = pod.get("affinity", {}).get("podAntiAffinity", {}).get("requiredDuringSchedulingIgnoredDuringExecution", [])
        if not any(a["topologyKey"] == "kubernetes.io/hostname" for a in aff):
            err("redis: two replicas could share a node (no required anti-affinity on hostname)")
        if not any(t["topologyKey"] == "topology.kubernetes.io/zone" and t["whenUnsatisfiable"] == "DoNotSchedule" for t in pod.get("topologySpreadConstraints", [])):
            err("redis: replicas aren't forced across zones")
        claims = {c["metadata"]["name"] for c in spec.get("volumeClaimTemplates", [])}
        if "data" not in claims or not any(m["name"] == "data" and m["mountPath"] == "/data" for m in cs["redis"]["volumeMounts"]):
            err("redis: /data isn't a PersistentVolumeClaim (AOF would not survive a restart)")
        secret_refs = {c["name"]: [e["valueFrom"]["secretKeyRef"]["name"] for e in c.get("env", []) if "secretKeyRef" in e.get("valueFrom", {})] for c in pod["containers"]}
        for cname, refs in secret_refs.items():
            if refs != ["grab-mini-redis"] * len(refs) or (cname != "exporter" and len(refs) != 2) or not refs:
                err(f"redis/{cname}: password must come from the grab-mini-redis Secret, key 'password'")
        for cname, c in cs.items():
            if unpinned(c["image"]):
                err(f"redis/{cname}: image {c['image']!r} isn't pinned to a version")
            r = c.get("resources", {})
            if "memory" not in r.get("limits", {}) or "cpu" not in r.get("requests", {}) or "memory" not in r.get("requests", {}):
                err(f"redis/{cname}: needs cpu/memory requests and a memory limit")
            if not c.get("securityContext", {}).get("readOnlyRootFilesystem"):
                err(f"redis/{cname}: root filesystem isn't read-only")
        for cname in ("redis", "sentinel"):
            for probe in ("readinessProbe", "livenessProbe"):
                if probe not in cs[cname]:
                    err(f"redis/{cname}: no {probe}")
        if "startupProbe" not in cs["redis"]:
            err("redis/redis: no startupProbe (a long AOF load would be killed by the liveness probe)")
        if "preStop" not in cs["redis"].get("lifecycle", {}):
            err("redis/redis: no preStop handover")
        # The scripts the containers run exist in the ConfigMap they mount.
        scripts = configmaps.get("redis-scripts", {})
        for c in cs.values():
            for part in c.get("command", []) + c.get("lifecycle", {}).get("preStop", {}).get("exec", {}).get("command", []):
                if part.startswith("/scripts/") and part.split("/")[-1] not in scripts:
                    err(f"redis/{c['name']}: runs {part}, which the redis-scripts ConfigMap doesn't have")
        # Metrics: the exporter's port is the one the PodMonitor scrapes.
        monitors = [m for m in of("PodMonitor") if m["spec"]["selector"]["matchLabels"].items() <= sts["spec"]["template"]["metadata"]["labels"].items()]
        eports = {p["name"] for p in cs["exporter"]["ports"]}
        if not any(e["port"] in eports for m in monitors for e in m["spec"]["podMetricsEndpoints"]):
            err("redis: no PodMonitor scrapes the exporter")

# ------------------------------------------------------------------ data ops ----
ops = configmaps["grab-mini-data-ops"]
ops_scripts = configmaps["grab-mini-data-ops-scripts"]
days, matched = int(ops["RETENTION_DAYS"]), int(ops["MATCHED_ORDER_DAYS"])
if days < 30 or matched < days:
    err(f"data-ops: RETENTION_DAYS={days}, MATCHED_ORDER_DAYS={matched}: need >= 30 (the dedupe floor) and matched >= retention")
migrating = {name(j)[: -len("-migrate")] for j in of("Job")}
tables_by_service = {}
for n in migrating:
    sql = "\n".join(p.read_text() for p in (ROOT / "services" / n / "migrations").glob("*.sql"))
    tables_by_service[n] = set(re.findall(r"CREATE TABLE IF NOT EXISTS (\w+)", sql))
cronjobs = {name(c): c for c in of("CronJob")}
for cj_name, cj in cronjobs.items():
    pod = cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]
    if cj["spec"].get("concurrencyPolicy") != "Forbid":
        err(f"CronJob {cj_name}: concurrencyPolicy must be Forbid (two runs would fight over the same rows)")
    for c in pod.get("initContainers", []) + pod["containers"]:
        if unpinned(c["image"]):
            err(f"CronJob {cj_name}/{c['name']}: image {c['image']!r} isn't pinned to a version")
        for ref in c.get("envFrom", []):
            cm = ref.get("configMapRef", {}).get("name")
            if cm and cm not in configmaps:
                err(f"CronJob {cj_name}: envFrom ConfigMap {cm!r} doesn't exist")
        for part in c.get("command", []):
            if part.startswith("/scripts/") and part.split("/")[-1] not in ops_scripts:
                err(f"CronJob {cj_name}: runs {part}, which grab-mini-data-ops-scripts doesn't have")
    for v in pod.get("volumes", []):
        if "configMap" in v and v["configMap"]["name"] not in configmaps:
            err(f"CronJob {cj_name}: mounts ConfigMap {v['configMap']['name']!r} which doesn't exist")
for n in sorted(migrating):
    if f"backup-{n}" not in cronjobs:
        err(f"{n} has a database but no backup-{n} CronJob")
for cj_name, cj in cronjobs.items():
    if cj_name.startswith("retention-"):
        c = cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]
        svc = next((v["secret"]["secretName"] for v in cj["spec"]["jobTemplate"]["spec"]["template"]["spec"]["volumes"] if "secret" in v), "")
        owner = svc.replace("grab-mini-db-", "")
        if owner not in migrating:
            err(f"CronJob {cj_name}: reads the database of {owner!r}, which has no migrations")
        if cj_name in ("retention-order-orders", "retention-trip-trips"):
            table = c["command"][-1]
            needed = {"orders": {"orders"}, "trips": {"trips", "cancelled_orders"}}[table]
            if not needed <= tables_by_service.get(owner, set()):
                err(f"CronJob {cj_name}: {sorted(needed)} aren't all tables of {owner} (see its migrations)")

# Redis is reachable: something admits its clients, the other replicas and the exporter scrape.
ingress_policies = [p for p in policies.values() if "Ingress" in p["spec"].get("policyTypes", [])]
for pod_name, labels in pods().items():
    if labels.get("app.kubernetes.io/component") != "redis":
        continue
    rules = [r for p in ingress_policies if selects(p["spec"]["podSelector"], labels) for r in p["spec"].get("ingress", [])]
    ports = {pt["port"] for r in rules for pt in r.get("ports", [])}
    for port, who in ((6379, "its clients"), (26379, "the Sentinels"), (9121, "the metrics scrape")):
        if port not in ports:
            err(f"{pod_name}: no ingress on {port} for {who}")
    for dep_name, dep in deployments.items():
        if not mounts_secret(dep["spec"]["template"]["spec"], "grab-mini-redis"):
            continue
        dep_labels = dep["spec"]["template"]["metadata"]["labels"]
        if not any(any("podSelector" in f and selects(f["podSelector"], dep_labels) for f in r.get("from", []))
                   and any(pt["port"] == 6379 for pt in r.get("ports", [])) for r in rules):
            err(f"{pod_name}: {dep_name} uses Redis but no ingress rule admits it on 6379")

if errors:
    print("\n".join(f"  - {e}" for e in errors))
    sys.exit(f"{len(errors)} problem(s)")
print(f"ok: {len(deployments)} deployments, {len(users)} Kafka users, {len(topics)} topics, {len(groups_in_code)} consumer groups")

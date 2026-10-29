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
config = of("ConfigMap")[0]["data"]
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

if errors:
    print("\n".join(f"  - {e}" for e in errors))
    sys.exit(f"{len(errors)} problem(s)")
print(f"ok: {len(deployments)} deployments, {len(users)} Kafka users, {len(topics)} topics, {len(groups_in_code)} consumer groups")

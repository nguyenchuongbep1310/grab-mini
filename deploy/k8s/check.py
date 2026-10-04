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

if errors:
    print("\n".join(f"  - {e}" for e in errors))
    sys.exit(f"{len(errors)} problem(s)")
print(f"ok: {len(deployments)} deployments, {len(users)} Kafka users, {len(topics)} topics, {len(groups_in_code)} consumer groups")

#!/usr/bin/env bash
# Checks everything under deploy/ that can be checked without a cluster:
#
#   deploy/verify.sh
#
# Needs: python3 with PyYAML, kustomize, kubeconform, promtool, go. Install the
# first three Go tools with `go install` (kustomize: sigs.k8s.io/kustomize/kustomize/v5,
# kubeconform: github.com/yannh/kubeconform/cmd/kubeconform); promtool comes with
# a Prometheus release. Set TOOLS to a directory to look for them in.
#
# What this proves: the manifests are valid Kubernetes and CRD objects, agree with
# one another and with the code, the alert rules parse and behave on synthetic
# data, and every service builds the way the Dockerfile builds it. What it does
# not: that a cluster accepts them, or that the Docker image builds.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -n "${TOOLS:-}" ] && export PATH="$TOOLS:$PATH"

for tool in python3 kustomize kubeconform promtool go; do
  command -v "$tool" >/dev/null || { echo "missing: $tool (see the header of this script)"; exit 1; }
done
step() { printf '\n== %s\n' "$*"; }

step "generated manifests are up to date"
python3 deploy/k8s/generate.py --check

step "kustomize build"
rendered=$(mktemp --suffix=.yaml)   # kubeconform skips files that don't end in .yaml
trap 'rm -f "$rendered"' EXIT
kustomize build deploy/k8s/base > "$rendered"
echo "$(grep -c '^kind:' "$rendered") objects"

step "kubeconform (Kubernetes 1.30 and the Strimzi, KEDA and Prometheus Operator CRDs)"
summary=$(kubeconform -strict -summary -kubernetes-version 1.30.0 \
  -schema-location default \
  -schema-location 'https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json' \
  "$rendered" deploy/k8s/dr/*.yaml)   # dr/ is applied by hand in the standby region, so it isn't in the build
echo "$summary"
# Checking nothing must not look like success.
case "$summary" in *"Valid: 0,"*|*"0 resource found"*) echo "kubeconform checked nothing"; exit 1;; esac

step "cross-reference checks (names, ports, env, groups, topics)"
python3 deploy/k8s/check.py < "$rendered"

step "alert rules"
promtool check rules deploy/monitoring/alerts.rules.yaml
promtool test rules deploy/monitoring/alerts.test.yaml

step "every service builds as the Dockerfile builds it (static, CGO off)"
out=$(mktemp -d)
for dir in services/*-service services/location-gateway services/dlq-redrive; do
  svc=$(basename "$dir")
  (cd "$dir" && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o "$out/$svc" .)
  printf '  %-26s %s\n' "$svc" "$(du -h "$out/$svc" | cut -f1)"
done
rm -rf "$out"

printf '\nall deploy checks passed\n'

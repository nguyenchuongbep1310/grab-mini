#!/usr/bin/env bash
# Runs govulncheck in the current module and fails on any vulnerability the code can
# reach, except the advisories listed in .govulncheck-allow (ones with no fix yet).
#
#   cd services/order-service && ../../scripts/govulncheck.sh
#
# Needs govulncheck on PATH: go install golang.org/x/vuln/cmd/govulncheck@v1.9.0
set -euo pipefail

ALLOW=$(cd "$(dirname "$0")/.." && pwd)/.govulncheck-allow
command -v govulncheck >/dev/null || { echo "error: govulncheck is not installed" >&2; exit 2; }

out=$(mktemp)
trap 'rm -f "$out"' EXIT
# -format json exits 0 whatever it finds; we decide from the findings.
govulncheck -format json ./... > "$out"

python3 - "$ALLOW" "$out" <<'PY'
import json, sys

allow = set()
for line in open(sys.argv[1]):
    line = line.split("#", 1)[0].strip()
    if line:
        allow.add(line.split()[0])

text = open(sys.argv[2]).read()
dec, i, msgs = json.JSONDecoder(), 0, []
while i < len(text):
    while i < len(text) and text[i].isspace():
        i += 1
    if i >= len(text):
        break
    obj, i = dec.raw_decode(text, i)
    msgs.append(obj)

osv = {m["osv"]["id"]: m["osv"] for m in msgs if "osv" in m}
# A finding whose trace starts at a function is one the code calls; the others only
# import or require the vulnerable package or module.
called = {}
for m in msgs:
    f = m.get("finding")
    if f and f["trace"] and f["trace"][0].get("function"):
        called.setdefault(f["osv"], f["trace"][0])

bad = {k: v for k, v in called.items() if k not in allow}
for k in sorted(called):
    t = called[k]
    status = "ALLOWED" if k in allow else "FAIL"
    print(f"{status}  {k}  {osv.get(k, {}).get('summary', '')}  [{t.get('module')}@{t.get('version')}, fixed in {t.get('fixed_version') or 'n/a'}]")
if bad:
    print(f"{len(bad)} reachable vulnerabilit{'y' if len(bad) == 1 else 'ies'} not in .govulncheck-allow", file=sys.stderr)
    sys.exit(1)
print("govulncheck: no reachable vulnerabilities" + (f" beyond {len(called)} allowed" if called else ""))
PY

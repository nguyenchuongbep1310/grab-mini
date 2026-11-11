---
name: migrating-go-service-clean-architecture
description: Use when asked to refactor or migrate a grab-mini Go service to Clean Architecture under ADR 0014, to continue the migration order, or to decide whether a service is worth migrating.
---

# Migrating a Go service to Clean Architecture (ADR 0014)

The authority is `docs/adr/0014-clean-architecture-for-go-services.md` (layout, dependency rule, the survey table, and one Result section per migrated service). Migrated so far: payment, trip, order, promotion, profile. pricing-service and chat-service were checked and deliberately not migrated; their ADR rows say why and when to revisit.

## Decide first

Read the service's ADR survey row, then measure: non-test lines (`ls services/<svc>/*.go | grep -v _test | xargs cat | wc -l`), where its rules really live (shared `services/pkg/*` code cannot move into one service's `internal/domain`), and `go list -f '{{join .Imports " "}}'` for each pkg it uses (a package that imports `os` or `net` is not import-pure and needs a port or stays out). If the gain is small, record "No, revisit" in the ADR row instead of migrating.

## The run

1. Plan: dispatch the `solution-architect` agent to write `docs/superpowers/plans/<date>-<svc>-clean-architecture.md` (use the profile-service plan as the template). Commit the plan on main.
2. Execute with `superpowers:subagent-driven-development` in a worktree `.claude/worktrees/sdd-<svc>` on `feature/<svc>-clean-architecture`: implementer per task, task review, fix loop, final whole-branch review on the most capable model.
3. Merge, push and the CI read happen only when the user asks (`pushing-and-reading-ci`), then `cleaning-merged-worktrees`.
4. Final message lists every `Ruling:` line from the ledger with what each costs if wrong; then delete the workspace.

## Invariants the plans and reviews must hold

- Behaviour is preserved. The flat code is the spec; where the plan disagrees, the flat code wins and the report says so.
- Order is behaviour: refusal order, claim and lock order, where the clock is read relative to the row lock, encode, outbox add, commit, then counters, log and wake.
- `main.go`, `env.go`, `migrations/` stay at the service root; `go build .` and `//go:embed` keep working; subcommands keep flags, output and exit codes.
- A consumer's `GroupID` stays a literal in a non-test `.go` file (`deploy/k8s/check.py` reads non-test files recursively). A service with no consumer must still show none.
- Each metric is declared in exactly one package; `go test` per package passes while `main` panics on a double registration, so run the binary.
- Each DB-backed test package gets a private database; Kafka is recreated before each e2e-style run.
- `internal/arch_test.go` is `archtest.Default(module).Test(t, ".")`; prove it can fail with temporary violating files.
- Split golden tests keep every assertion; keep a real-database twin when the fast copy uses fakes.

## Check at the final review

Makefile, CI workflow (service detection, Kafka job), Dockerfile, `deploy/k8s/check.py`, scripts and docs that name moved files; the ADR status line, the survey row's line count, and a Result section with measured before/after numbers and claims that were actually verified (the "second service" wording slip came from an unchecked claim). e2e is only claimed if it ran.

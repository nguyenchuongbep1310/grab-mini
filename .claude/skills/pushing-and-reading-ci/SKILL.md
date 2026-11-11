---
name: pushing-and-reading-ci
description: Use when the user asks to push to origin and read, check or report the CI result for grab-mini, or when a pushed commit's GitHub Actions run needs to be found, awaited and summarised.
---

# Pushing and reading CI

Push and merge only on the user's request. Approval for one push does not cover the next.

## Steps

1. `git status --short` and `git branch --show-current`. Stage nothing you did not write; the user's untracked files (`docs/SECURITY-REVIEW.md`, `.agents/`, `skills-lock.json`) are never committed.
2. Merge a feature branch with `git merge --no-ff <branch>` (message ends with the commit trailer from the system reminder), then `git push origin main`. Note the pushed range (`old..new`).
3. Find the run for the pushed commit, not the latest one:
   `gh run list --branch main --limit 3 --json databaseId,headSha,status,conclusion,name`
   Match `headSha` to the commit you pushed. A freshly pushed run may take a few seconds to appear (it shows `queued`).
4. Wait with one bounded call, not a hand-rolled poll:
   `timeout 590 gh run watch <id> --interval 20 --exit-status >/dev/null 2>&1; gh run view <id> --json status,conclusion --jq '.status+" "+.conclusion'`
5. Read jobs: `gh run view <id> --json jobs --jq '.jobs[] | select(.name|test("<service>")) | .conclusion+" "+.name'`, and count failures with `select(.conclusion!="success" and .conclusion!="skipped")`.

## Report

- Conclusion of the run, the pushed range, and the jobs for the service that changed (`go`, `go + kafka`, `govulncheck`).
- Say only what the job names show. The `services/e2e-test` jobs are that module's own go, kafka and govulncheck jobs; they do not by themselves prove a service-level e2e ran.
- On failure, read the failing job's log (`gh run view <id> --log-failed`) before proposing a fix. A `govulncheck` failure names a vulnerable dependency; the fix is a version bump in each module that requires it, re-checked with `scripts/govulncheck.sh`.
- A docs-only commit still gets a run; say when you did not wait for it.

## Common mistakes

- Reading the previous run because the new one was not listed yet.
- Reporting "CI green" from the latest run on the branch without matching the sha.
- Pushing again to "retry" without reading why it failed.

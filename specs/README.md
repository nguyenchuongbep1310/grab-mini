# Where specs, plans and decisions live

| What | Where | Rule |
|---|---|---|
| The product: requirements, design, task list | `specs/ride-booking-mini/` | `requirements.md`, `design.md`, `tasks.md` are kept current. Anything that changes behaviour, adds a service or adds infrastructure gets a line in `tasks.md` and, if it changes a requirement or the design, in those files too. |
| A feature or demo that needs its own design | `docs/superpowers/specs/<date>-<name>-design.md` | Written and approved before the plan. Goals, non-goals and anti-goals bind the plan. Linked from `tasks.md`. |
| The step-by-step plan for one change | `docs/superpowers/plans/<date>-<name>.md` | Names its spec (or its ADR when the change is a refactor). Linked from `tasks.md`. |
| A decision and what it gave up | `docs/adr/NNNN-*.md` | Also serves as the spec for refactors that change no behaviour (ADR 0014 for the Clean Architecture migrations). |

Order of work: spec (or ADR) approved, then plan committed, then implementation in a worktree, then review, then merge. `tasks.md` is ticked when the change is merged, not when it is planned.

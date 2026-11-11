---
name: running-the-dev-stack
description: Use when the user wants to start, stop, reset or wipe the grab-mini local environment: stop all services, shut down Docker, delete data, or start again from scratch.
---

# Running the dev stack

## What to run

| Goal | Command |
|---|---|
| Start everything | `make dev` (or `pnpm dev:all`) |
| Stop Go services and web apps | Ctrl-C in the terminal running `make dev`; `scripts/dev.sh` traps INT and TERM and cleans up its children |
| Stop Docker infra, keep data | `make infra-down` |
| Stop Docker infra and delete all volumes | `make infra-reset` |
| Wipe data and start again | `make dev-reset` (runs in the foreground until Ctrl-C) |
| Delete built binaries, logs, frontend `dist` | `make clean` |

`.dev/` (binaries, logs, run files) and the browser's localStorage are not touched by the `infra-*` targets.

## Wipe everything and shut it all down

1. Find the running dev script: `ps -eo pid,ppid,command | grep -E 'scripts/dev.sh|\.dev/bin/' | grep -v grep`.
2. If one is running, stop it first with `kill -TERM <pid of the topmost scripts/dev.sh>` and wait until `ps` shows no `dev.sh`, `.dev/bin/*` or `go build` left. Wiping volumes under running services leaves them broken.
3. `make infra-reset`.
4. Verify nothing is left: `docker ps -a --format '{{.Names}}' | grep grab-mini`, `docker volume ls --format '{{.Name}}' | grep grab-mini`, `docker network ls --format '{{.Name}}' | grep grab-mini`, and `lsof -nP -iTCP:3001-3012,5171-5173,8080,8081,8088,9090,9092 -sTCP:LISTEN`.

## Rules

- Deleting data is destructive: do it when the user asks for it, say what goes (six volumes: Postgres, Kafka, Redis, Grafana, Prometheus, Loki), and never as a side effect of something else.
- If the stack came back after a reset, someone started `make dev` again; look for the process before wiping a second time.
- Throwaway containers used by tests have their own names (for example `pf-*`); never touch containers or volumes named `grab-mini*` unless the user asked for the dev stack.
- After a reset, `make dev` recreates topics and schemas and seeds five drivers, three riders and three promo codes; no `dev-reset` is needed.

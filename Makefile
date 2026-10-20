SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

GO_MODULES := $(shell sed -n 's/^[[:space:]]*\(\.\/[^[:space:]]*\)[[:space:]]*$$/\1/p' go.work)
SERVICES := auth-service order-service matching-service trip-service driver-location-service notification-service rating-service location-gateway pricing-service driver-app-sim dlq-redrive loadtest dev-registry
MIGRATE_SERVICES := order-service trip-service rating-service

BIN_DIR := .dev/bin
LOG_DIR := .dev/logs

KAFKA_BROKERS ?= localhost:9092
REDIS_ADDR ?= localhost:6379
DATABASE_URL ?= postgres://grabmini:grabmini@localhost:5432/grabmini?sslmode=disable
TOPIC ?= order.created
SVC ?=
LIMIT ?= 50
RATE ?= 50
DURATION ?= 30s
DRIVERS ?= 100

.PHONY: help dev dev-reset build build-go build-web seed \
	infra-up infra-down infra-reset infra-ps obs-up \
	logs logs-errors logs-loki metrics-check prom-check \
	test test-web test-e2e test-integration fmt fmt-check vet typecheck lint verify \
	migrate kafka-topics kafka-lag dlq-dry-run dlq-redrive loadtest clean

##@ Development & Build

help: ## Show all available automation commands
	@awk 'BEGIN {FS = ":.*##"; printf "\n\033[1mgrab-mini Automation Commands\033[0m\n"} \
		/^[a-zA-Z0-9_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
		/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)
	@echo ""

dev: ## Start full stack: Docker infra, Prometheus/Grafana/Loki, Go services & web apps
	bash scripts/dev.sh

dev-reset: ## Wipe Docker volumes and start the full stack from scratch
	bash scripts/dev.sh --reset

build: build-go build-web ## Build all Go binaries (.dev/bin) and frontend apps

build-go: ## Build all Go services and tools into .dev/bin
	@mkdir -p $(BIN_DIR)
	@root=$$(pwd); \
	for svc in $(SERVICES); do \
		printf "  building %-26s\n" "$$svc"; \
		(cd "services/$$svc" && go build -o "$$root/$(BIN_DIR)/$$svc" .); \
	done

build-web: ## Build all frontend packages and apps (rider, driver, ops, ui)
	pnpm --filter @grab-mini/ui build
	pnpm --filter @grab-mini/rider build
	pnpm --filter @grab-mini/driver build
	pnpm --filter @grab-mini/ops build

seed: ## Seed the 5 default drivers (cars & bikes) into Redis
	cd services/matching-service && REDIS_ADDR=$(REDIS_ADDR) go run . seed-drivers

##@ Infrastructure & Observability (Docker Compose)

infra-up: ## Start all Docker containers (Kafka, Postgres, Redis, ksqlDB, Prometheus, Loki, Grafana)
	@mkdir -p $(LOG_DIR)
	docker compose up -d

infra-down: ## Stop all Docker containers (keeps data volumes)
	docker compose down

infra-reset: ## Stop all Docker containers and delete data volumes
	docker compose down -v

infra-ps: ## List status and ports of all Docker containers
	docker compose ps

obs-up: ## Start only the observability stack (Prometheus, Loki, Promtail, Grafana)
	@mkdir -p $(LOG_DIR)
	docker compose up -d prometheus loki promtail grafana
	@echo "Grafana:    http://localhost:3000"
	@echo "Prometheus: http://localhost:9090"
	@echo "Loki Ready: http://localhost:3100/ready"

##@ Logging & Metrics

logs: ## Tail local service logs (usage: make logs or make logs SVC=order-service)
	@if [ -n "$(SVC)" ]; then \
		tail -n 100 -f "$(LOG_DIR)/$(SVC).log"; \
	else \
		tail -n 50 -f $(LOG_DIR)/*.log; \
	fi

logs-errors: ## Filter WARN and ERROR lines across all .dev/logs/*.log files
	@grep -E '"level":"(WARN|ERROR)"|level=(WARN|ERROR)|error|Error|panic' $(LOG_DIR)/*.log 2>/dev/null || echo "No error/warn logs found in $(LOG_DIR)."

logs-loki: ## Query recent logs from Loki (usage: make logs-loki SVC=order-service LIMIT=50)
	@query='{job="grab-mini"}'; \
	if [ -n "$(SVC)" ]; then query='{job="grab-mini",service="$(SVC)"}'; fi; \
	curl -fsSG "http://localhost:3100/loki/api/v1/query_range" \
		--data-urlencode "query=$$query" \
		--data-urlencode "limit=$(LIMIT)" \
	| python3 -c 'import json,sys; d=json.load(sys.stdin); [print(f"[{s[\"stream\"].get(\"service\",\"?\")}] {v[1]}") for s in d.get("data",{}).get("result",[]) for v in s.get("values",[])]'

metrics-check: ## Check /readyz and /metrics on all Go services plus Prometheus scrape targets
	@printf "\033[1m%-26s %-14s %-12s %s\033[0m\n" "SERVICE" "PORT" "READYZ" "METRICS SERIES"
	@for item in \
		"order-service:3001" \
		"trip-service:3002" \
		"notification-service:3003" \
		"rating-service:3004" \
		"auth-service:3005" \
		"location-gateway:3006" \
		"pricing-service:3007" \
		"matching-service:9101" \
		"driver-location-service:9102"; do \
		svc=$${item%%:*}; port=$${item##*:}; \
		if curl -fs "http://localhost:$$port/readyz" >/dev/null 2>&1; then \
			ready="\033[32mREADY\033[0m"; \
			series=$$(curl -fs "http://localhost:$$port/metrics" 2>/dev/null | grep -c -v '^#' || echo 0); \
		else \
			ready="\033[31mDOWN\033[0m"; \
			series="-"; \
		fi; \
		printf "%-26s :%-13s %b       %s\n" "$$svc" "$$port" "$$ready" "$$series"; \
	done
	@if curl -fs "http://localhost:9090/-/healthy" >/dev/null 2>&1; then \
		echo ""; \
		echo "Prometheus Active Targets (http://localhost:9090/targets):"; \
		curl -fs "http://localhost:9090/api/v1/targets" | python3 -c 'import json,sys; [print(f"  - {t[\"labels\"].get(\"job\",\"?\"):26s} {t[\"health\"].upper()} ({t[\"scrapeUrl\"]})") for t in json.load(sys.stdin)["data"]["activeTargets"]]'; \
	fi

prom-check: ## Validate Prometheus config, alert rules, and run alert unit tests
	@if command -v promtool >/dev/null 2>&1; then \
		promtool check config deploy/monitoring/prometheus.yml; \
		promtool check rules deploy/monitoring/alerts.rules.yaml; \
		promtool test rules deploy/monitoring/alerts.test.yaml; \
	else \
		docker run --rm --entrypoint promtool \
			-v "$$(pwd)/deploy/monitoring:/etc/prometheus:ro" \
			-w /etc/prometheus \
			prom/prometheus:v3.5.0 \
			check config /etc/prometheus/prometheus.yml && \
		docker run --rm --entrypoint promtool \
			-v "$$(pwd)/deploy/monitoring:/etc/prometheus:ro" \
			-w /etc/prometheus \
			prom/prometheus:v3.5.0 \
			check rules /etc/prometheus/alerts.rules.yaml && \
		docker run --rm --entrypoint promtool \
			-v "$$(pwd)/deploy/monitoring:/etc/prometheus:ro" \
			-w /etc/prometheus \
			prom/prometheus:v3.5.0 \
			test rules /etc/prometheus/alerts.test.yaml; \
	fi

##@ Testing, Formatting & Verification

test: ## Run unit tests across all Go modules and frontend packages
	@for mod in $(GO_MODULES); do \
		echo "==> go test $$mod"; \
		(cd "$$mod" && go test ./...) || exit 1; \
	done
	pnpm test

test-web: ## Run frontend unit & component tests (Vitest)
	pnpm test

test-e2e: ## Run browser E2E tests across Rider, Driver & Ops apps (Playwright)
	pnpm test:e2e

test-integration: ## Run Go tests (including full-stack e2e-test) with Postgres, Redis and Kafka
	@for mod in $(GO_MODULES); do \
		echo "==> go test (integration) $$mod"; \
		(cd "$$mod" && \
			TEST_DATABASE_URL="$(DATABASE_URL)" \
			TEST_REDIS_ADDR="$(REDIS_ADDR)" \
			TEST_KAFKA_BROKERS="$(KAFKA_BROKERS)" \
			go test ./...) || exit 1; \
	done

fmt: ## Format all Go source files with gofmt
	gofmt -s -w services/

fmt-check: ## Verify all Go files are formatted with gofmt
	@out=$$(gofmt -l services/); \
	if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; \
	fi

vet: ## Run go vet across all Go modules
	@for mod in $(GO_MODULES); do \
		(cd "$$mod" && go vet ./...) || exit 1; \
	done

typecheck: ## Run TypeScript typecheck across all frontend packages
	pnpm -r typecheck

lint: fmt-check vet typecheck ## Run all code quality checks (gofmt, go vet, pnpm typecheck)
	@echo "All lint checks passed."

verify: ## Run deployment & manifest verification (deploy/verify.sh)
	bash deploy/verify.sh

##@ Database, Kafka, DLQ & Load Testing

migrate: ## Run SQL migrations for order-service, trip-service, and rating-service
	@for svc in $(MIGRATE_SERVICES); do \
		echo "==> migrating $$svc"; \
		(cd "services/$$svc" && DATABASE_URL="$(DATABASE_URL)" go run . migrate) || exit 1; \
	done

kafka-topics: ## List all Kafka topics in the local broker
	docker exec grab-mini-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-lag: ## Show consumer group offsets and lag across all service groups
	docker exec grab-mini-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe

dlq-dry-run: ## Inspect dead-letter queue messages for TOPIC without redriving (usage: make dlq-dry-run TOPIC=order.created)
	KAFKA_BROKERS=$(KAFKA_BROKERS) go run ./services/dlq-redrive -topic $(TOPIC) -dry-run

dlq-redrive: ## Redrive dead-letter queue messages back to TOPIC (usage: make dlq-redrive TOPIC=order.created)
	KAFKA_BROKERS=$(KAFKA_BROKERS) go run ./services/dlq-redrive -topic $(TOPIC)

loadtest: ## Run end-to-end ride loadtest (usage: make loadtest RATE=50 DURATION=30s DRIVERS=100)
	go run ./services/loadtest -rate $(RATE) -duration $(DURATION) -drivers $(DRIVERS)

clean: ## Remove built binaries (.dev/bin), logs (.dev/logs) and frontend dist folders
	rm -rf $(BIN_DIR) $(LOG_DIR) apps/*/dist packages/*/dist

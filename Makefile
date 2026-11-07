.PHONY: up down logs build test test-integration lint proto migrate run-api run-processor run-query run-worker loadgen dlq dlq-replay demo demo-load demo-down smoke

demo: ## Run everything in Docker and open the demo page. Needs only Docker
	docker compose --profile app up -d --build
	@echo "Waiting for the services to come up..."
	@for i in $$(seq 1 120); do curl -sf localhost:8081/healthz >/dev/null && curl -sf localhost:8082/healthz >/dev/null && break; sleep 1; done
	@curl -sf localhost:8082/healthz >/dev/null || (echo "The Query API didn't start; see docker compose logs" && exit 1)
	@echo
	@echo "Demo page:  http://localhost:8082"
	@echo "Grafana:    http://localhost:3000  (Fintech > Transaction Pipeline)"
	@echo "Jaeger:     http://localhost:16686"
	@echo "Temporal:   http://localhost:8233"
	@echo "Kafka UI:   http://localhost:8080"
	@echo
	@echo "Generate traffic with 'make demo-load'. Stop with 'make demo-down'."
	@(command -v open >/dev/null && open http://localhost:8082) || (command -v xdg-open >/dev/null && xdg-open http://localhost:8082) || true

demo-load: ## Send 60 s of traffic through the demo, including withdrawals
	docker compose --profile tools run --rm --build loadgen

demo-down: ## Stop the demo and delete its data
	docker compose --profile app --profile tools down -v

smoke: ## Check a running system end to end through its public APIs
	./scripts/smoke.sh

up: ## Start the local stack, create topics, and run migrations
	docker compose up -d --wait kafka postgres kafka-ui otel-collector jaeger prometheus grafana temporal
	docker compose run --rm kafka-init
	docker compose run --rm migrate

down: ## Stop the local stack (add -v to the command to also drop data)
	docker compose down

logs:
	docker compose logs -f

build:
	go build ./...

test: ## Unit tests (no Docker needed)
	go test ./...

test-integration: ## Integration tests against real Kafka and Postgres (needs Docker)
	go test -tags integration -count=1 ./...

lint:
	go vet ./...
	golangci-lint run --build-tags integration ./...
	buf lint

proto: ## Regenerate Go code from proto/
	buf generate

migrate: ## Apply database migrations
	docker compose run --rm migrate

run-api:
	go run ./src/cmd/api

run-processor:
	go run ./src/cmd/processor

run-query: ## Query API + live stream + demo page at http://localhost:8082
	go run ./src/cmd/query

run-worker: ## Settlement workflows (Temporal worker + outbox relay)
	go run ./src/cmd/worker

loadgen: ## e.g. make loadgen ARGS="-rate 200 -duration 60s"
	go run ./src/cmd/loadgen $(ARGS)

dlq: ## List dead-lettered events
	go run ./src/cmd/dlq

dlq-replay: ## Republish dead-lettered events to the main topic
	go run ./src/cmd/dlq -replay

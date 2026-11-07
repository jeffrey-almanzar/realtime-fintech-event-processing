.PHONY: up down logs build test test-integration lint proto migrate run-api run-processor run-query run-worker loadgen dlq dlq-replay

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

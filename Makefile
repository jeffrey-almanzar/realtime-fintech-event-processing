.PHONY: up down logs build test lint proto migrate run-api run-processor loadgen

up: ## Start the local stack, create topics, and run migrations
	docker compose up -d --wait kafka postgres kafka-ui otel-collector jaeger prometheus grafana
	docker compose run --rm kafka-init
	docker compose run --rm migrate

down: ## Stop the local stack (add -v to the command to also drop data)
	docker compose down

logs:
	docker compose logs -f

build:
	go build ./...

test:
	go test ./...

lint:
	go vet ./...
	golangci-lint run ./...
	buf lint

proto: ## Regenerate Go code from proto/
	buf generate

migrate: ## Apply database migrations
	docker compose run --rm migrate

run-api:
	go run ./src/cmd/api

run-processor:
	go run ./src/cmd/processor

loadgen: ## e.g. make loadgen ARGS="-rate 200 -duration 60s"
	go run ./src/cmd/loadgen $(ARGS)

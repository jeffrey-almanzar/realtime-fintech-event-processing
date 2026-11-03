.PHONY: up down logs build test lint proto run-producer run-consumer

up: ## Start the local stack and create topics
	docker compose up -d --wait kafka postgres kafka-ui otel-collector prometheus grafana
	docker compose run --rm kafka-init

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

run-producer:
	go run ./src/cmd/producer

run-consumer:
	go run ./src/cmd/consumer

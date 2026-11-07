// Package config loads service configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

// Kafka holds the settings shared by producers and consumers.
type Kafka struct {
	BootstrapServers string
	Topic            string
	DLQTopic         string
	GroupID          string
	// GroupInstanceID enables static group membership when set: a consumer
	// that restarts with the same ID gets its partitions back immediately
	// instead of waiting for a rebalance. Each running instance needs a
	// unique, stable ID (e.g. a StatefulSet pod name).
	GroupInstanceID string
}

// LoadKafka reads Kafka settings from the environment, falling back to
// defaults that match the local docker-compose stack.
func LoadKafka() Kafka {
	return Kafka{
		BootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		Topic:            getEnv("KAFKA_TOPIC", "transactions"),
		DLQTopic:         getEnv("KAFKA_DLQ_TOPIC", "transactions.dlq"),
		GroupID:          getEnv("KAFKA_GROUP_ID", "transaction-processor"),
		GroupInstanceID:  getEnv("KAFKA_GROUP_INSTANCE_ID", ""),
	}
}

// DatabaseURL returns the Postgres connection string.
func DatabaseURL() string {
	return getEnv("DATABASE_URL", "postgres://fintech:fintech@localhost:5432/fintech?sslmode=disable")
}

// ProcessorMaxBatch caps how many events the Processor applies in one
// database transaction.
func ProcessorMaxBatch() int {
	n, err := strconv.Atoi(getEnv("PROCESSOR_MAX_BATCH", "500"))
	if err != nil || n < 1 {
		return 500
	}
	return n
}

// TemporalAddress returns the Temporal server's host:port.
func TemporalAddress() string {
	return getEnv("TEMPORAL_ADDRESS", "localhost:7233")
}

// ProviderDelay is how long the mock payment provider takes per charge. Raise
// it to have time to kill the worker in the middle of a settlement.
func ProviderDelay() time.Duration {
	d, err := time.ParseDuration(getEnv("PROVIDER_DELAY", "500ms"))
	if err != nil {
		return 500 * time.Millisecond
	}
	return d
}

// HTTPAddr returns the address the Event API listens on.
func HTTPAddr() string {
	return getEnv("HTTP_ADDR", ":8081")
}

// QueryHTTPAddr returns the address the Query API listens on.
func QueryHTTPAddr() string {
	return getEnv("QUERY_HTTP_ADDR", ":8082")
}

// EventAPIURL is where browser clients (the demo page) send transactions.
func EventAPIURL() string {
	return getEnv("EVENT_API_URL", "http://localhost:8081")
}

// CORSAllowedOrigins lists the browser origins allowed to call the Event API,
// comma-separated.
func CORSAllowedOrigins() string {
	return getEnv("CORS_ALLOWED_ORIGINS", "http://localhost:8082")
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

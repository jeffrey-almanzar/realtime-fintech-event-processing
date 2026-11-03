// Package config loads service configuration from environment variables.
package config

import "os"

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

// HTTPAddr returns the address the Event API listens on.
func HTTPAddr() string {
	return getEnv("HTTP_ADDR", ":8081")
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

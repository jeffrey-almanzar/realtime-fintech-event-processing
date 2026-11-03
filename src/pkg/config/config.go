// Package config loads service configuration from environment variables.
package config

import "os"

// Kafka holds the settings shared by producers and consumers.
type Kafka struct {
	BootstrapServers string
	Topic            string
	DLQTopic         string
	GroupID          string
}

// LoadKafka reads Kafka settings from the environment, falling back to
// defaults that match the local docker-compose stack.
func LoadKafka() Kafka {
	return Kafka{
		BootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
		Topic:            getEnv("KAFKA_TOPIC", "transactions"),
		DLQTopic:         getEnv("KAFKA_DLQ_TOPIC", "transactions.dlq"),
		GroupID:          getEnv("KAFKA_GROUP_ID", "transaction-processor"),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

package config

import "testing"

func TestLoadKafkaDefaults(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "")
	t.Setenv("KAFKA_TOPIC", "")

	cfg := LoadKafka()
	if cfg.BootstrapServers != "localhost:9092" {
		t.Errorf("BootstrapServers = %q, want %q", cfg.BootstrapServers, "localhost:9092")
	}
	if cfg.Topic != "transactions" {
		t.Errorf("Topic = %q, want %q", cfg.Topic, "transactions")
	}
}

func TestLoadKafkaFromEnv(t *testing.T) {
	t.Setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
	t.Setenv("KAFKA_TOPIC", "custom")

	cfg := LoadKafka()
	if cfg.BootstrapServers != "broker:29092" {
		t.Errorf("BootstrapServers = %q, want %q", cfg.BootstrapServers, "broker:29092")
	}
	if cfg.Topic != "custom" {
		t.Errorf("Topic = %q, want %q", cfg.Topic, "custom")
	}
}

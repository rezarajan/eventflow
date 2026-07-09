// Package redpanda publishes CloudEvents to a Kafka-compatible Redpanda broker.
package redpanda

import (
	"os"
	"strconv"
	"strings"
)

// Name is the adapter name used by fan-out configuration.
const Name = "redpanda"

// Config defines the Redpanda publisher settings.
type Config struct {
	Brokers   []string
	Topic     string
	TopicMode string
	BatchSize int
}

// FromEnv builds a Redpanda publisher configuration from environment variables.
func FromEnv() Config {
	return Config{
		Brokers:   splitCSV(envString("DATASCAPE_REDPANDA_BROKERS", "localhost:19092")),
		Topic:     envString("DATASCAPE_REDPANDA_TOPIC", "datascape.events.v1"),
		TopicMode: envString("DATASCAPE_REDPANDA_TOPIC_MODE", "single"),
		BatchSize: envInt("DATASCAPE_REDPANDA_BATCH_SIZE", 100),
	}
}

// normalized returns configuration values with safe defaults applied.
func (c Config) normalized() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	return c
}

// envString returns an environment variable value or a fallback.
func envString(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// envInt returns an integer environment variable value or a fallback.
func envInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

// splitCSV splits a comma-separated string into trimmed non-empty values.
func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

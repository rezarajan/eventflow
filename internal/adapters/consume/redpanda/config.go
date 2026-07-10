// Package redpanda consumes CloudEvents from a Kafka-compatible Redpanda broker.
package redpanda

import (
	"os"
	"strings"
)

// Name is the event source name used by consumer configuration.
const Name = "redpanda"

// Config defines Redpanda consumer settings.
type Config struct {
	Brokers     []string
	Topic       string
	GroupID     string
	StartOffset string
}

// FromEnv builds Redpanda consumer configuration from environment variables.
func FromEnv() Config {
	return Config{
		Brokers:     splitCSV(envString("EVENTFLOW_REDPANDA_BROKERS", envString("DATASCAPE_REDPANDA_BROKERS", "localhost:19092"))),
		Topic:       envString("EVENTFLOW_REDPANDA_TOPIC", envString("DATASCAPE_REDPANDA_TOPIC", "")),
		GroupID:     envString("EVENTFLOW_REDPANDA_CONSUMER_GROUP", envString("DATASCAPE_REDPANDA_CONSUMER_GROUP", "eventflow-consume")),
		StartOffset: envString("EVENTFLOW_REDPANDA_CONSUMER_START_OFFSET", envString("DATASCAPE_REDPANDA_CONSUMER_START_OFFSET", "first")),
	}
}

// normalized returns configuration values with safe defaults applied.
func (c Config) normalized() Config {
	if len(c.Brokers) == 0 {
		c.Brokers = []string{"localhost:19092"}
	}
	if c.GroupID == "" {
		c.GroupID = "eventflow-consume"
	}
	if c.StartOffset == "" {
		c.StartOffset = "first"
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

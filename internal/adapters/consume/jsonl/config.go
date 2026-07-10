// Package jsonl materializes CloudEvents into local JSONL table files.
package jsonl

import "os"

// Name is the handler name used by consumer configuration.
const Name = "jsonl"

// Config defines JSONL materializer settings.
type Config struct {
	Dir string
}

// FromEnv builds JSONL materializer configuration from environment variables.
func FromEnv() Config {
	return Config{Dir: envString("EVENTFLOW_JSONL_DIR", envString("DATASCAPE_JSONL_DIR", "var/eventflow/materialized"))}
}

// normalized returns configuration values with safe defaults applied.
func (c Config) normalized() Config {
	if c.Dir == "" {
		c.Dir = "var/eventflow/materialized"
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

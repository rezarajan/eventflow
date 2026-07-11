// Package duckdb materializes CloudEvents into a local DuckDB database.
package duckdb

import "os"

// Name is the handler name used by consume configuration.
const Name = "duckdb"

// Config defines DuckDB materialization settings.
type Config struct {
	Path string
}

// FromEnv builds DuckDB configuration from environment variables.
func FromEnv() Config {
	return Config{
		Path: envString("EVENTFLOW_DUCKDB_PATH", "var/eventflow/eventflow.duckdb"),
	}
}

// normalized returns configuration values with safe defaults applied.
func (c Config) normalized() Config {
	if c.Path == "" {
		c.Path = "var/eventflow/eventflow.duckdb"
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

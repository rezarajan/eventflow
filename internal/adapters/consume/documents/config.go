// Package documents materializes text document artifacts from CloudEvents.
package documents

import "os"

// Name is the handler name used by consumer configuration.
const Name = "objects"

// Config defines local document artifact settings.
type Config struct {
	Dir string
}

// FromEnv builds document artifact configuration from environment variables.
func FromEnv() Config {
	return Config{Dir: envString("EVENTFLOW_OBJECT_DIR", envString("DATASCAPE_OBJECT_DIR", "var/eventflow/objects"))}
}

// normalized returns configuration values with safe defaults applied.
func (c Config) normalized() Config {
	if c.Dir == "" {
		c.Dir = "var/eventflow/objects"
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

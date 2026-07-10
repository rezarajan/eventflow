package redpanda

import (
	"fmt"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/lakehouse-poc/internal/contracts/event"
)

// IsSingleTopicMode reports whether a topic mode resolves all events to one configured topic.
func IsSingleTopicMode(topicMode string) bool {
	switch strings.ToLower(strings.TrimSpace(topicMode)) {
	case "", "single":
		return true
	default:
		return false
	}
}

// TopicFor resolves the Redpanda topic for a CloudEvent using the configured topic mode.
func TopicFor(config Config, evt cloudevents.Event) (string, error) {
	switch strings.ToLower(strings.TrimSpace(config.TopicMode)) {
	case "", "single":
		if config.Topic == "" {
			return "", fmt.Errorf("redpanda topic is required for single topic mode")
		}
		return config.Topic, nil
	case "type-prefix":
		prefix := strings.Split(evt.Type(), ".")[0]
		if prefix == "" {
			return "", fmt.Errorf("CloudEvent type is required for type-prefix topic mode")
		}
		return prefix + ".events.v1", nil
	case "catalog":
		spec, ok := event.DefaultCatalog().Lookup(evt.Type())
		if !ok {
			return "", fmt.Errorf("no catalog topic for CloudEvent type %q", evt.Type())
		}
		return spec.Topic, nil
	default:
		return "", fmt.Errorf("unsupported redpanda topic mode %q", config.TopicMode)
	}
}

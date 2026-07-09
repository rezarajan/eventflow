package redpanda

import "testing"

// TestKafkaMessageForTopicWriterOmitsTopic verifies kafka-go messages do not set
// a per-message topic when the writer is already configured with one.
func TestKafkaMessageForTopicWriterOmitsTopic(t *testing.T) {
	message := Message{
		Topic: "datascape.events.v1",
		Key:   []byte("event-1"),
		Value: []byte(`{"id":"event-1"}`),
		Headers: []Header{
			{Key: "ce_type", Value: []byte("thing.created.v1")},
		},
	}

	kafkaMessage := kafkaMessageForTopicWriter(message)

	if kafkaMessage.Topic != "" {
		t.Fatalf("kafka message topic = %q, want empty topic for topic-bound writer", kafkaMessage.Topic)
	}
	if string(kafkaMessage.Key) != "event-1" || string(kafkaMessage.Value) != `{"id":"event-1"}` {
		t.Fatalf("unexpected key/value conversion: key=%s value=%s", kafkaMessage.Key, kafkaMessage.Value)
	}
	if len(kafkaMessage.Headers) != 1 || kafkaMessage.Headers[0].Key != "ce_type" || string(kafkaMessage.Headers[0].Value) != "thing.created.v1" {
		t.Fatalf("unexpected header conversion: %+v", kafkaMessage.Headers)
	}
}

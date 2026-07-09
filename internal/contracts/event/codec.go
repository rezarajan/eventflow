package event

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// EncodeJSONL writes CloudEvents SDK events as newline-delimited JSON.
func EncodeJSONL(ctx context.Context, writer io.Writer, events <-chan cloudevents.Event) error {
	encoder := json.NewEncoder(writer)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-events:
			if !ok {
				return nil
			}
			if err := encoder.Encode(evt); err != nil {
				return fmt.Errorf("encode CloudEvent: %w", err)
			}
		}
	}
}

// DecodeJSONL reads newline-delimited CloudEvents JSON and streams SDK events to the output channel.
func DecodeJSONL(ctx context.Context, reader io.Reader, out chan<- cloudevents.Event) error {
	defer close(out)
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 1024*1024)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var evt cloudevents.Event
		if err := json.Unmarshal(scanner.Bytes(), &evt); err != nil {
			return fmt.Errorf("decode CloudEvent: %w", err)
		}
		if err := evt.Validate(); err != nil {
			return fmt.Errorf("validate CloudEvent: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- evt:
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan CloudEvents JSONL: %w", err)
	}
	return nil
}

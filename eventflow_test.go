package eventflow

import (
	"context"
	"io"
	"testing"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"
)

func TestRuntimeAcknowledgesAfterHandlerSuccess(t *testing.T) {
	receiver := &ackReceiver{event: testRuntimeEvent(t)}
	handler := &recordingHandler{}
	runtime := Runtime{Receiver: receiver, Handler: handler}
	if err := runtime.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !handler.called || receiver.acks != 1 {
		t.Fatalf("handler called=%v acks=%d", handler.called, receiver.acks)
	}
}

func TestRuntimeDoesNotAcknowledgeWhenHandlerFails(t *testing.T) {
	receiver := &ackReceiver{event: testRuntimeEvent(t)}
	runtime := Runtime{Receiver: receiver, Handler: EventHandlerFunc(func(context.Context, Event) error {
		return context.Canceled
	})}
	if err := runtime.Run(context.Background()); err == nil {
		t.Fatal("expected handler error")
	}
	if receiver.acks != 0 || receiver.nacks != 1 {
		t.Fatalf("acks=%d nacks=%d", receiver.acks, receiver.nacks)
	}
}

type EventHandlerFunc func(context.Context, Event) error

func (f EventHandlerFunc) Handle(ctx context.Context, event Event) error { return f(ctx, event) }

type ackReceiver struct {
	event Event
	read  bool
	acks  int
	nacks int
}

func (*ackReceiver) Open(context.Context) error  { return nil }
func (*ackReceiver) Close(context.Context) error { return nil }
func (r *ackReceiver) Receive(context.Context) (Event, error) {
	return Event{}, context.Canceled
}
func (r *ackReceiver) ReceiveAck(context.Context) (ReceivedEvent, error) {
	if r.read {
		return ReceivedEvent{}, io.EOF
	}
	r.read = true
	return ReceivedEvent{
		Event: r.event,
		Ack: func(context.Context) error {
			r.acks++
			return nil
		},
		Nack: func(context.Context) error {
			r.nacks++
			return nil
		},
	}, nil
}

type recordingHandler struct{ called bool }

func (h *recordingHandler) Handle(context.Context, Event) error {
	h.called = true
	return nil
}

func testRuntimeEvent(t *testing.T) Event {
	t.Helper()
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID("1")
	event.SetType("example.created.v1")
	event.SetSource("urn:test")
	event.SetTime(time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC))
	if err := event.SetData(sdk.ApplicationJSON, map[string]any{"id": "1"}); err != nil {
		t.Fatal(err)
	}
	return event
}

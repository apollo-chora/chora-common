package eventbus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

func testEnvelope() envelope.Envelope {
	now := time.Now().UTC().Truncate(time.Second)
	return envelope.Envelope{
		EventID:        "evt-1",
		IdempotencyKey: "evt-1",
		TenantID:       "00000000-0000-7000-8000-000000000001",
		GCID:           "00000000-0000-7000-8000-000000000002",
		OccurredAt:     now,
		PublishedAt:    now,
		Traceparent:    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		SourceService:  "test",
		SourceProject:  "local",
		SchemaVersion:  1,
	}
}

func TestValidateSubject(t *testing.T) {
	cases := map[string]bool{
		"chora.observability.token_usage.recorded.v1": true,
		"chora.tenancy.tenant.created.v1":             true,
		"chora.closure.saga.started.v2":               true,
		"chora.unknown.thing.happened.v1":             false,
		"notchora.observability.token_usage.recorded.v1": false,
		"chora.observability.token_usage.recorded.v0":    false,
		"chora.observability.Token_Usage.recorded.v1":    false,
		"": false,
	}
	for subject, wantOK := range cases {
		err := ValidateSubject(subject)
		if wantOK && err != nil {
			t.Errorf("ValidateSubject(%q) = %v, want nil", subject, err)
		}
		if !wantOK && err == nil {
			t.Errorf("ValidateSubject(%q) = nil, want error", subject)
		}
	}
}

func TestInMemoryBusPublishSubscribe(t *testing.T) {
	bus := NewInMemoryBus(WithSynchronousDelivery())
	defer bus.Close()

	env := testEnvelope()
	got := make(chan Message, 1)
	if _, err := bus.Subscribe(context.Background(), "chora.observability.thing.happened.v1", func(_ context.Context, m Message) error {
		got <- m
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), "chora.observability.thing.happened.v1", env, []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-got:
		if m.Subject != "chora.observability.thing.happened.v1" {
			t.Errorf("subject = %q", m.Subject)
		}
		if string(m.Payload) != "payload" {
			t.Errorf("payload = %q", m.Payload)
		}
		if m.Envelope.EventID != env.EventID {
			t.Errorf("envelope event id = %q", m.Envelope.EventID)
		}
	case <-time.After(time.Second):
		t.Fatal("no message received")
	}
}

func TestInMemoryBusHandlerErrorGoesToDLQ(t *testing.T) {
	bus := NewInMemoryBus(WithSynchronousDelivery(), WithMaxDeliveryAttempts(2))
	defer bus.Close()

	dlq := make(chan Message, 1)
	if _, err := bus.SubscribeDLQ(context.Background(), "chora.observability.thing.happened.v1", func(_ context.Context, m Message) error {
		dlq <- m
		return nil
	}); err != nil {
		t.Fatalf("subscribe dlq: %v", err)
	}

	if _, err := bus.Subscribe(context.Background(), "chora.observability.thing.happened.v1", func(_ context.Context, _ Message) error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	if err := bus.Publish(context.Background(), "chora.observability.thing.happened.v1", testEnvelope(), []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-dlq:
		if m.Subject != "chora.observability.thing.happened.v1.dlq" {
			t.Errorf("dlq subject = %q", m.Subject)
		}
	case <-time.After(time.Second):
		t.Fatal("no DLQ message received")
	}
}

func TestInMemoryBusRejectsInvalidSubject(t *testing.T) {
	bus := NewInMemoryBus()
	defer bus.Close()
	if err := bus.Publish(context.Background(), "not-a-subject", testEnvelope(), nil); err == nil {
		t.Error("expected error for invalid subject")
	}
}

func TestJetStreamRequiresURL(t *testing.T) {
	if _, err := NewJetStream(JetStreamConfig{}); err == nil {
		t.Error("expected error for empty URL")
	}
}

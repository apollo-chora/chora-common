package pubsub

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apollo-chora/chora-common/envelope"
)

// recordingOutbox captures Publish calls for assertion.
type recordingOutbox struct {
	calls []struct {
		Topic   string
		Env     envelope.Envelope
		Payload []byte
	}
	fail error
}

func (r *recordingOutbox) Publish(
	_ context.Context, topic string, env envelope.Envelope, payload []byte,
) error {
	if r.fail != nil {
		return r.fail
	}
	r.calls = append(r.calls, struct {
		Topic   string
		Env     envelope.Envelope
		Payload []byte
	}{topic, env, payload})
	return nil
}

func TestClosureAckPublisher_PublishesEnvelopeConformingAck(t *testing.T) {
	rec := &recordingOutbox{}
	p := NewClosureAckPublisher(rec, "chora-489812", "chora-creation")

	err := p.Publish(
		"chora.creation.account.pseudonymised.v1",
		"tenant-1", "gcid-1", "00-abc-def-01",
		map[string]interface{}{
			"saga_id": "saga-1",
			"domain":  "creation",
		},
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if len(rec.calls) != 1 {
		t.Fatalf("want 1 publish, got %d", len(rec.calls))
	}
	c := rec.calls[0]
	if c.Topic != "chora.creation.account.pseudonymised.v1" {
		t.Errorf("topic = %q", c.Topic)
	}
	if c.Env.TenantID != "tenant-1" || c.Env.GCID != "gcid-1" {
		t.Errorf("env tenant/gcid = %q/%q", c.Env.TenantID, c.Env.GCID)
	}
	if c.Env.Traceparent != "00-abc-def-01" {
		t.Errorf("traceparent not propagated: %q", c.Env.Traceparent)
	}
	if c.Env.SourceProject != "chora-489812" || c.Env.SourceService != "chora-creation" {
		t.Errorf("source fields = %q/%q", c.Env.SourceProject, c.Env.SourceService)
	}
	if c.Env.EventID == "" || c.Env.IdempotencyKey == "" {
		t.Errorf("event_id/idempotency_key missing")
	}
	// Deterministic idempotency key from saga_id + gcid (re-ack after
	// redelivery collapses orchestrator-side).
	if c.Env.IdempotencyKey != "pseudonymise_ack:saga-1:gcid-1" {
		t.Errorf("idempotency key = %q", c.Env.IdempotencyKey)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(c.Payload, &body); err != nil {
		t.Fatalf("payload not JSON: %v", err)
	}
	if body["domain"] != "creation" {
		t.Errorf("payload domain = %v", body["domain"])
	}
}

func TestClosureAckPublisher_MintsTraceparentWhenAbsent(t *testing.T) {
	rec := &recordingOutbox{}
	p := NewClosureAckPublisher(rec, "chora-489812", "chora-tenancy")
	if err := p.Publish(
		"chora.tenancy.account.pseudonymised.v1",
		"tenant-1", "gcid-1", "",
		map[string]interface{}{"saga_id": "s"},
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if rec.calls[0].Env.Traceparent == "" {
		t.Error("traceparent should be minted when absent (OTLP-everywhere)")
	}
}

func TestClosureAckPublisher_NilGuards(t *testing.T) {
	var p *ClosureAckPublisher
	if err := p.Publish("t", "a", "b", "", nil); err == nil {
		t.Error("nil receiver should error")
	}
	q := NewClosureAckPublisher(nil, "p", "s")
	if err := q.Publish("t", "a", "b", "", nil); err == nil {
		t.Error("nil inner should error")
	}
}

func TestClosureAckPublisher_PropagatesInnerError(t *testing.T) {
	rec := &recordingOutbox{fail: context.DeadlineExceeded}
	p := NewClosureAckPublisher(rec, "p", "s")
	if err := p.Publish("t", "a", "b", "", map[string]interface{}{}); err == nil {
		t.Error("inner error should propagate (Handle nacks + Pub/Sub redelivers)")
	}
}

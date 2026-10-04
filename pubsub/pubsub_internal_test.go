// Internal-package tests for the residual branch gaps in the envelope
// projection / reconstruction helpers and the closure-ack publisher. These
// call unexported helpers directly, so they need the internal package.
package pubsub

import (
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

// TestEnvelopeFromAttributes_InvalidOccurredAt covers the occurred_at parse
// error branch of envelopeFromAttributes.
func TestEnvelopeFromAttributes_InvalidOccurredAt(t *testing.T) {
	attrs := map[string]string{
		"occurred_at":  "not-a-time",
		"published_at": "2026-05-09T12:00:00Z",
	}
	if _, err := envelopeFromAttributes(attrs); err == nil {
		t.Fatal("expected occurred_at parse error")
	} else if !strings.Contains(err.Error(), "occurred_at") {
		t.Errorf("err=%v; want occurred_at mentioned", err)
	}
}

// TestEnvelopeFromAttributes_InvalidPublishedAt covers the published_at parse
// error branch of envelopeFromAttributes.
func TestEnvelopeFromAttributes_InvalidPublishedAt(t *testing.T) {
	attrs := map[string]string{
		"occurred_at":  "2026-05-09T12:00:00Z",
		"published_at": "nope",
	}
	if _, err := envelopeFromAttributes(attrs); err == nil {
		t.Fatal("expected published_at parse error")
	} else if !strings.Contains(err.Error(), "published_at") {
		t.Errorf("err=%v; want published_at mentioned", err)
	}
}

// TestEnvelopeAttributes_OptionalSetBranches covers the non-empty branches of
// the optional attribute projection: tracestate, correlation_id, causation_id
// are emitted only when non-empty, and gcid is stripped when empty.
func TestEnvelopeAttributes_OptionalSetBranches(t *testing.T) {
	env := envelope.Envelope{
		EventID:        "e",
		IdempotencyKey: "k",
		TenantID:       "t",
		OccurredAt:     time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		PublishedAt:    time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC),
		Traceparent:    "00-abc",
		Tracestate:     "vendor=foo",
		SourceProject:  "p",
		SourceService:  "s",
		SchemaVersion:  1,
		CorrelationID:  "corr",
		CausationID:    "caus",
		// GCID deliberately empty — the wire attribute must be stripped.
	}
	attrs := envelopeAttributes(env)
	for _, key := range []string{"tracestate", "correlation_id", "causation_id"} {
		if _, ok := attrs[key]; !ok {
			t.Errorf("attribute %q missing; want set when non-empty", key)
		}
	}
	if _, ok := attrs["gcid"]; ok {
		t.Error("gcid attribute present; want stripped when empty")
	}
	// Mandatory keys remain present.
	for _, key := range []string{"event_id", "idempotency_key", "tenant_id",
		"occurred_at", "published_at", "traceparent", "source_project",
		"source_service", "schema_version"} {
		if _, ok := attrs[key]; !ok {
			t.Errorf("mandatory attribute %q missing", key)
		}
	}
}

// TestValidateTopicName_MustStartWithChora covers the "must start with chora"
// branch: it takes 5+ segments so the len<5 early return is bypassed.
func TestValidateTopicName_MustStartWithChora(t *testing.T) {
	if err := ValidateTopicName("foo.bar.baz.qux.v1"); err == nil {
		t.Fatal("expected error for non-chora prefix")
	} else if !strings.Contains(err.Error(), "must start with chora") {
		t.Errorf("err=%v; want 'must start with chora'", err)
	}
}

// TestClosureAckPublisher_PayloadEncodeError covers the json.Marshal error
// branch: an unencodable payload value (a func) must surface a wrapped
// encode error instead of a panic.
func TestClosureAckPublisher_PayloadEncodeError(t *testing.T) {
	p := NewClosureAckPublisher(&recordingOutbox{}, "p", "s")
	err := p.Publish("chora.creation.account.pseudonymised.v1", "tenant-1", "gcid-1", "",
		map[string]interface{}{"bad": func() {}})
	if err == nil {
		t.Fatal("expected payload encode error")
	}
	if !strings.Contains(err.Error(), "closure ack payload encode") {
		t.Errorf("err=%v; want 'closure ack payload encode' wrapper", err)
	}
}

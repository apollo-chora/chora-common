package eventbus

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/apollo-chora/chora-common/envelope"
)

// fullEnvelope returns an envelope with every optional field populated so the
// header round-trip test covers correlation/causation/IMDA as well.
func fullEnvelope() envelope.Envelope {
	occurred := time.Date(2026, 10, 5, 6, 49, 0, 123456789, time.UTC)
	published := time.Date(2026, 10, 5, 6, 49, 1, 987654321, time.UTC)
	return envelope.Envelope{
		EventID:            "0199a0f3-1c2d-7a4b-9f10-abcdef012345",
		IdempotencyKey:     "atom-42-v7",
		TenantID:           "00000000-0000-7000-8000-000000000001",
		GCID:               "00000000-0000-7000-8000-000000000002",
		OccurredAt:         occurred,
		PublishedAt:        published,
		Traceparent:        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		Tracestate:         "congo=t61rcWkgMzE",
		SourceProject:      "chora-local",
		SourceService:      "chora-observability",
		SchemaVersion:      2,
		CorrelationID:      "corr-1",
		CausationID:        "cause-1",
		ChoraImdaDimension: "accountability",
		ImdaLifecycleStage: "runtime",
	}
}

// TestEnvelopeHeaderRoundTrip is the guard for the bug that made every
// consumed message fail validation: the publisher set occurred_at /
// published_at / correlation_id / causation_id / IMDA headers that the
// subscriber never read back, so the reconstructed envelope failed
// envelope.Validate and the message was dropped.
func TestEnvelopeHeaderRoundTrip(t *testing.T) {
	want := fullEnvelope()
	got, err := envelopeFromHeaders(envelopeHeaders(want))
	if err != nil {
		t.Fatalf("envelopeFromHeaders: %v", err)
	}
	if !got.OccurredAt.Equal(want.OccurredAt) {
		t.Errorf("OccurredAt = %s, want %s", got.OccurredAt, want.OccurredAt)
	}
	if !got.PublishedAt.Equal(want.PublishedAt) {
		t.Errorf("PublishedAt = %s, want %s", got.PublishedAt, want.PublishedAt)
	}
	got.OccurredAt, got.PublishedAt = time.Time{}, time.Time{}
	want.OccurredAt, want.PublishedAt = time.Time{}, time.Time{}
	if got != want {
		t.Errorf("round-trip envelope mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestEnvelopeFromHeadersRejectsMissingOccurredAt(t *testing.T) {
	h := envelopeHeaders(fullEnvelope())
	h.Del("Chora-Occurred-At")
	if _, err := envelopeFromHeaders(h); err == nil {
		t.Fatal("expected error when occurred_at header is absent")
	}
}

func TestEnvelopeFromHeadersRejectsGarbageTimestamp(t *testing.T) {
	h := envelopeHeaders(fullEnvelope())
	h.Set("Chora-Published-At", "not-a-time")
	if _, err := envelopeFromHeaders(h); err == nil {
		t.Fatal("expected error for unparseable published_at")
	}
}

// TestDLQHeadersDeriveMessageID guards the DLQ duplicate-suppression trap: the
// dead-letter copy must not carry the source message's Nats-Msg-Id, or the
// broker would suppress the DLQ write as a duplicate of the message being
// dead-lettered.
func TestDLQHeadersDeriveMessageID(t *testing.T) {
	src := envelopeHeaders(fullEnvelope())
	src.Set(jetstream.MsgIDHeader, fullEnvelope().EventID)
	cfg := ConsumerConfig{Name: "obs.token_usage", Subject: "chora.observability.token_usage.recorded.v1"}

	h := dlqHeaders(src, cfg, 5, errors.New("boom"))

	if got := h.Get(jetstream.MsgIDHeader); got != fullEnvelope().EventID+".dlq."+cfg.Name {
		t.Errorf("Nats-Msg-Id = %q, want %q", got, fullEnvelope().EventID+".dlq."+cfg.Name)
	}
	if got := h.Get("Chora-Dlq-Source-Subject"); got != cfg.Subject {
		t.Errorf("Chora-Dlq-Source-Subject = %q, want %q", got, cfg.Subject)
	}
	if got := h.Get("Chora-Dlq-Consumer"); got != cfg.Name {
		t.Errorf("Chora-Dlq-Consumer = %q, want %q", got, cfg.Name)
	}
	if got := h.Get("Chora-Dlq-Reason"); got != "boom" {
		t.Errorf("Chora-Dlq-Reason = %q, want %q", got, "boom")
	}
	if got := h.Get("Chora-Dlq-Delivery-Count"); got != "5" {
		t.Errorf("Chora-Dlq-Delivery-Count = %q, want %q", got, "5")
	}
	if _, err := time.Parse(time.RFC3339Nano, h.Get("Chora-Dlq-Failed-At")); err != nil {
		t.Errorf("Chora-Dlq-Failed-At = %q is not RFC3339Nano: %v", h.Get("Chora-Dlq-Failed-At"), err)
	}
	// The payload envelope headers must survive onto the DLQ copy.
	if got := h.Get("Chora-Event-Id"); got != fullEnvelope().EventID {
		t.Errorf("Chora-Event-Id = %q, want %q", got, fullEnvelope().EventID)
	}
}

func TestDLQHeadersWithoutEventIDDropMessageID(t *testing.T) {
	src := nats.Header{}
	src.Set(jetstream.MsgIDHeader, "stale-id")
	cfg := ConsumerConfig{Subject: "chora.closure.requested.v1"}

	h := dlqHeaders(src, cfg, 0, nil)

	if got := h.Get(jetstream.MsgIDHeader); got != "" {
		t.Errorf("Nats-Msg-Id = %q, want empty (no event id to derive from)", got)
	}
	if got := h.Get("Chora-Dlq-Consumer"); got != "" {
		t.Errorf("Chora-Dlq-Consumer = %q, want empty when cfg.Name is blank", got)
	}
	if got := h.Get("Chora-Dlq-Reason"); got != "" {
		t.Errorf("Chora-Dlq-Reason = %q, want empty when cause is nil", got)
	}
	if got := h.Get("Chora-Dlq-Delivery-Count"); got != "" {
		t.Errorf("Chora-Dlq-Delivery-Count = %q, want empty when delivered is 0", got)
	}
}

func TestDLQSubjectConvention(t *testing.T) {
	if got, want := DLQSubject("chora.observability.token_usage.recorded.v1"), "_dlq.chora.observability.token_usage.recorded.v1"; got != want {
		t.Errorf("DLQSubject = %q, want %q", got, want)
	}
}

// TestRetryDelay covers the 1-based delivery indexing: the first failure must
// use backoff[0], and an exhausted schedule must clamp to its last entry.
func TestRetryDelay(t *testing.T) {
	backoff := []time.Duration{time.Second, 5 * time.Second, 15 * time.Second}
	cases := []struct {
		delivered uint64
		want      time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{2, 5 * time.Second},
		{3, 15 * time.Second},
		{4, 15 * time.Second},
		{99, 15 * time.Second},
	}
	for _, tc := range cases {
		if got := retryDelay(backoff, tc.delivered); got != tc.want {
			t.Errorf("retryDelay(delivered=%d) = %s, want %s", tc.delivered, got, tc.want)
		}
	}
	if got := retryDelay(nil, 1); got != 0 {
		t.Errorf("retryDelay(nil) = %s, want 0 (immediate Nak)", got)
	}
}

// TestPublishMessageIDIsEventID documents the broker-side duplicate-suppression
// key: the outbox re-publishes the same row with the same envelope bytes, so a
// stable EventID is what lets JetStream suppress the crash-window duplicate.
func TestPublishMessageIDIsEventID(t *testing.T) {
	if jetstream.MsgIDHeader != "Nats-Msg-Id" {
		t.Fatalf("jetstream.MsgIDHeader = %q; duplicate suppression key changed", jetstream.MsgIDHeader)
	}
	env := fullEnvelope()
	h := envelopeHeaders(env)
	h.Set(jetstream.MsgIDHeader, env.EventID)
	if h.Get(jetstream.MsgIDHeader) != env.EventID {
		t.Fatalf("Nats-Msg-Id = %q, want EventID %q", h.Get(jetstream.MsgIDHeader), env.EventID)
	}
}

// Package pubsub_test exercises the schema-validating publisher contract.
//
// Real Pub/Sub publishing is exercised by adapter integration tests in each
// service; this package validates the in-process logic — envelope check,
// topic-name validation, IMDA tagging passthrough — without spinning Pub/Sub
// emulator or hitting the network.
package pubsub_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
	chpubsub "github.com/apollo-chora/chora-common/pubsub"
	"github.com/apollo-chora/chora-common/tracing"
)

// fakeRawPublisher captures publish calls so tests can assert wire content.
type fakeRawPublisher struct {
	calls []chpubsub.PublishCall
	err   error
}

func (f *fakeRawPublisher) Publish(ctx context.Context, call chpubsub.PublishCall) error {
	f.calls = append(f.calls, call)
	return f.err
}

func TestValidateTopicName_OK(t *testing.T) {
	t.Parallel()
	cases := []string{
		"chora.creation.atom.published.v1",
		"chora.consumption.session.completed.v1",
		"chora.identity.account.lifecycle_changed.v1",
		"chora.ai_kernel.closure_orchestrator.crypto_shred_orchestrated.v1",
		"chora.tenancy.addon.usage_recorded.v1",
		"chora.governance.imda.dimension_attested.v99",
	}
	for _, name := range cases {
		name := name
		t.Run(name, func(t *testing.T) {
			if err := chpubsub.ValidateTopicName(name); err != nil {
				t.Errorf("expected ok, got %v", err)
			}
		})
	}
}

func TestValidateTopicName_Reject(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":                                  "empty",
		"creation.atom.published.v1":        "missing chora prefix",
		"chora.creation.atom":               "missing version",
		"chora.creation.atom.v1":            "missing event_type",
		"chora.creation.atom.x.v":           "version not numeric",
		"chora.creation.atom.x.v0":          "version starts at 1",
		"chora.creation.atom.PUBLISHED.v1":  "uppercase",
		"chora.unknown.atom.published.v1":   "unknown domain",
		"chora.creation.atom.published.v01": "leading zero in version",
	}
	for input, why := range cases {
		input, why := input, why
		t.Run(why, func(t *testing.T) {
			if err := chpubsub.ValidateTopicName(input); err == nil {
				t.Errorf("expected reject %q because %s", input, why)
			}
		})
	}
}

func TestPublishEnvelope_RejectsInvalidEnvelope(t *testing.T) {
	t.Parallel()
	raw := &fakeRawPublisher{}
	pub := chpubsub.NewPublisher(raw)

	// Empty envelope — Validate will fail.
	bad := envelope.Envelope{}
	err := pub.PublishEnvelope(context.Background(), "chora.creation.atom.published.v1", bad, []byte("payload"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if len(raw.calls) != 0 {
		t.Errorf("no publish should occur on validation failure")
	}
}

func TestPublishEnvelope_RejectsInvalidTopic(t *testing.T) {
	t.Parallel()
	raw := &fakeRawPublisher{}
	pub := chpubsub.NewPublisher(raw)

	env := makeValidEnvelope(t)
	err := pub.PublishEnvelope(context.Background(), "not.a.valid.topic", env, []byte("payload"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if len(raw.calls) != 0 {
		t.Errorf("no publish should occur on topic validation failure")
	}
}

func TestPublishEnvelope_HappyPath(t *testing.T) {
	t.Parallel()
	raw := &fakeRawPublisher{}
	pub := chpubsub.NewPublisher(raw)

	env := makeValidEnvelope(t)
	payload := []byte("encoded protobuf bytes")

	err := pub.PublishEnvelope(context.Background(), "chora.creation.atom.published.v1", env, payload)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(raw.calls) != 1 {
		t.Fatalf("expected 1 publish, got %d", len(raw.calls))
	}
	got := raw.calls[0]
	if got.Topic != "chora.creation.atom.published.v1" {
		t.Errorf("topic = %q", got.Topic)
	}
	if string(got.Data) != string(payload) {
		t.Errorf("payload mismatch")
	}
	// Attributes from envelope
	if got.Attributes["event_id"] != env.EventID {
		t.Errorf("event_id attribute missing or wrong")
	}
	if got.Attributes["tenant_id"] != env.TenantID {
		t.Errorf("tenant_id attribute missing or wrong")
	}
	if got.Attributes["traceparent"] != env.Traceparent {
		t.Errorf("traceparent attribute missing or wrong")
	}
	// IMDA tag (empty in this fixture but key still allowed if set)
	if _, hasTag := got.Attributes["chora_imda_dimension"]; hasTag && env.SchemaVersion < 1 {
		t.Errorf("unexpected imda tag")
	}
}

func TestPublishEnvelope_PropagatesPublisherError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("simulated publish failure")
	raw := &fakeRawPublisher{err: wantErr}
	pub := chpubsub.NewPublisher(raw)

	env := makeValidEnvelope(t)
	err := pub.PublishEnvelope(context.Background(), "chora.creation.atom.published.v1", env, []byte("x"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want wrapped %v", err, wantErr)
	}
}

func TestPublishEnvelope_StripsEmptyOptionalAttributes(t *testing.T) {
	t.Parallel()
	raw := &fakeRawPublisher{}
	pub := chpubsub.NewPublisher(raw)

	env := makeValidEnvelope(t)
	env.CorrelationID = ""
	env.CausationID = ""
	env.Tracestate = ""

	if err := pub.PublishEnvelope(context.Background(), "chora.creation.atom.published.v1", env, []byte("x")); err != nil {
		t.Fatal(err)
	}
	got := raw.calls[0]
	for _, key := range []string{"correlation_id", "causation_id", "tracestate"} {
		if _, has := got.Attributes[key]; has {
			t.Errorf("empty optional %q should not be in attributes", key)
		}
	}
}

func TestPublishEnvelope_PropagatesIMDATag(t *testing.T) {
	t.Parallel()
	raw := &fakeRawPublisher{}
	pub := chpubsub.NewPublisher(raw)

	env := makeValidEnvelope(t)
	env.ChoraImdaDimension = "accountability"
	env.ImdaLifecycleStage = "runtime"

	if err := pub.PublishEnvelope(context.Background(), "chora.creation.atom.published.v1", env, []byte("x")); err != nil {
		t.Fatal(err)
	}
	got := raw.calls[0]
	if got.Attributes["chora_imda_dimension"] != "accountability" {
		t.Errorf("imda dimension attribute not propagated")
	}
	if got.Attributes["imda_lifecycle_stage"] != "runtime" {
		t.Errorf("imda lifecycle stage attribute not propagated")
	}
}

// makeValidEnvelope returns an envelope that passes envelope.Validate.
func makeValidEnvelope(t *testing.T) envelope.Envelope {
	t.Helper()
	now := time.Now().UTC()
	ctx := tracing.WithTenantID(context.Background(), "11111111-1111-7111-8111-111111111111")
	ctx = tracing.WithGCID(ctx, "22222222-2222-7222-8222-222222222222")
	ctx = tracing.WithTraceparent(ctx, "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	env := envelope.Build(ctx, envelope.BuildOpts{
		EventType:     "atom_published",
		SchemaVersion: 1,
		SourceProject: "chora-content",
		SourceService: "chora-creation",
		Now:           func() time.Time { return now },
	})
	if err := envelope.Validate(env); err != nil {
		t.Fatalf("envelope fixture invalid: %v", err)
	}
	return env
}

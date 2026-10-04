package pubsub_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
	chpubsub "github.com/5007-Capstone/chora/libs/chora-go-common/pubsub"
)

// stubCloudClient is a minimal CloudPubSubClient stub for unit tests.
type stubCloudClient struct {
	mu      sync.Mutex
	pubArgs []stubPubCall

	// receiveSeed is the slice of CloudMessage values fed to handlers when
	// SubscriptionReceive is called.
	receiveSeed []chpubsub.CloudMessage

	// publishErr causes PublishMessage to return this error.
	publishErr error
}

type stubPubCall struct {
	topic string
	data  []byte
	attrs map[string]string
}

func (s *stubCloudClient) PublishMessage(_ context.Context, topic string, data []byte, attrs map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.publishErr != nil {
		return "", s.publishErr
	}
	s.pubArgs = append(s.pubArgs, stubPubCall{topic: topic, data: data, attrs: attrs})
	return "msg-id", nil
}

func (s *stubCloudClient) SubscriptionReceive(ctx context.Context, _ string, handler func(ctx context.Context, msg chpubsub.CloudMessage) error) error {
	s.mu.Lock()
	seed := append([]chpubsub.CloudMessage(nil), s.receiveSeed...)
	s.mu.Unlock()
	for _, m := range seed {
		if err := handler(ctx, m); err != nil {
			// Handler returned error; ignore in stub (real client retries).
			continue
		}
	}
	return nil
}

func TestCloudPublisher_PublishHappyPath(t *testing.T) {
	stub := &stubCloudClient{}
	pub := chpubsub.NewCloudPublisher(stub)
	env := makeValidEnvelope(t)

	if err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if len(stub.pubArgs) != 1 {
		t.Fatalf("pub calls=%d want 1", len(stub.pubArgs))
	}
	call := stub.pubArgs[0]
	if call.topic != "chora.creation.atom.created.v1" {
		t.Errorf("topic=%q", call.topic)
	}
	if string(call.data) != "p" {
		t.Errorf("data=%q want p", call.data)
	}
	if call.attrs["event_id"] != env.EventID {
		t.Error("event_id attribute missing")
	}
	// The publisher MUST stamp the destination topic as a message attribute so
	// downstream push dispatchers can route/guard on it. The shared outbox does
	// not carry it otherwise; consumers that strict-match topic silently drop
	// every real event without this (see feedback_pubsub_push_topic_attr).
	if call.attrs["topic"] != "chora.creation.atom.created.v1" {
		t.Errorf("topic attribute = %q; want chora.creation.atom.created.v1", call.attrs["topic"])
	}
}

func TestCloudPublisher_RejectsInvalidEnvelope(t *testing.T) {
	stub := &stubCloudClient{}
	pub := chpubsub.NewCloudPublisher(stub)
	bad := envelope.Envelope{}
	err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", bad, []byte("p"))
	if err == nil {
		t.Fatal("expected error for invalid envelope")
	}
}

func TestCloudPublisher_RejectsInvalidTopic(t *testing.T) {
	stub := &stubCloudClient{}
	pub := chpubsub.NewCloudPublisher(stub)
	env := makeValidEnvelope(t)
	err := pub.Publish(context.Background(), "bogus.topic", env, []byte("p"))
	if err == nil {
		t.Fatal("expected topic validation error")
	}
}

func TestCloudPublisher_PropagatesClientError(t *testing.T) {
	stub := &stubCloudClient{publishErr: errors.New("rpc failed")}
	pub := chpubsub.NewCloudPublisher(stub)
	env := makeValidEnvelope(t)
	err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p"))
	if err == nil {
		t.Fatal("expected propagated client error")
	}
}

func TestCloudSubscriber_AckOnSuccess(t *testing.T) {
	var ackCalled int32
	var nackCalled int32
	env := makeValidEnvelope(t)
	attrs := envelopeAttributesFor(env)
	attrs["topic"] = "chora.creation.atom.created.v1"

	stub := &stubCloudClient{
		receiveSeed: []chpubsub.CloudMessage{{
			ID:         "m-1",
			Data:       []byte("p"),
			Attributes: attrs,
			Ack:        func() { atomic.AddInt32(&ackCalled, 1) },
			Nack:       func() { atomic.AddInt32(&nackCalled, 1) },
		}},
	}
	sub := chpubsub.NewCloudSubscriber(stub)
	var got chpubsub.Message
	err := sub.Subscribe(context.Background(), "test-sub", func(_ context.Context, msg *chpubsub.Message) error {
		got = *msg
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if atomic.LoadInt32(&ackCalled) != 1 || atomic.LoadInt32(&nackCalled) != 0 {
		t.Errorf("ack=%d nack=%d want 1 0", ackCalled, nackCalled)
	}
	if got.Envelope.EventID != env.EventID {
		t.Error("envelope not propagated to handler")
	}
}

// TestCloudSubscriber_NackOnHandlerError asserts the inner handler Nacks (and
// does NOT Ack) when the typed handler returns an error. The error is logged
// centrally at the dispatch layer (dispatchReceivedMessage — covered by
// gcp_client_dispatch_test.go), not at the CloudSubscriber layer, so this test
// only asserts the ack/nack behaviour.
func TestCloudSubscriber_NackOnHandlerError(t *testing.T) {
	var ackCalled int32
	var nackCalled int32
	env := makeValidEnvelope(t)
	attrs := envelopeAttributesFor(env)
	attrs["topic"] = "chora.creation.atom.created.v1"

	stub := &stubCloudClient{
		receiveSeed: []chpubsub.CloudMessage{{
			ID:         "m-1",
			Data:       []byte("p"),
			Attributes: attrs,
			Ack:        func() { atomic.AddInt32(&ackCalled, 1) },
			Nack:       func() { atomic.AddInt32(&nackCalled, 1) },
		}},
	}
	sub := chpubsub.NewCloudSubscriber(stub)
	_ = sub.Subscribe(context.Background(), "test-sub", func(_ context.Context, _ *chpubsub.Message) error {
		return errors.New("handler boom")
	})
	if atomic.LoadInt32(&ackCalled) != 0 || atomic.LoadInt32(&nackCalled) != 1 {
		t.Errorf("ack=%d nack=%d want 0 1", ackCalled, nackCalled)
	}
}

// TestCloudSubscriber_NackOnMalformedEnvelope asserts a malformed/undecodable
// envelope is Nacked and the typed handler is never invoked. The decode error
// is returned and logged centrally at the dispatch layer, not here.
func TestCloudSubscriber_NackOnMalformedEnvelope(t *testing.T) {
	var nackCalled int32
	stub := &stubCloudClient{
		receiveSeed: []chpubsub.CloudMessage{{
			ID:         "m-1",
			Data:       []byte("p"),
			Attributes: map[string]string{}, // empty -> malformed
			Ack:        func() {},
			Nack:       func() { atomic.AddInt32(&nackCalled, 1) },
		}},
	}
	sub := chpubsub.NewCloudSubscriber(stub)
	called := int32(0)
	_ = sub.Subscribe(context.Background(), "test-sub", func(_ context.Context, _ *chpubsub.Message) error {
		atomic.AddInt32(&called, 1)
		return nil
	})
	if atomic.LoadInt32(&called) != 0 {
		t.Errorf("handler called=%d want 0 (envelope malformed)", called)
	}
	if atomic.LoadInt32(&nackCalled) != 1 {
		t.Errorf("nack=%d want 1", nackCalled)
	}
}

// envelopeAttributesFor mirrors envelopeAttributes() in the package, but
// we don't have access to the unexported helper from a *_test package, so
// we publish via the in-memory bus to a stub raw publisher to capture
// the canonical attribute map. Cheaper alternative: just hand-craft the
// minimum set the cloud subscriber decoder needs.
func envelopeAttributesFor(env envelope.Envelope) map[string]string {
	return map[string]string{
		"event_id":             env.EventID,
		"idempotency_key":      env.IdempotencyKey,
		"tenant_id":            env.TenantID,
		"gcid":                 env.GCID,
		"occurred_at":          env.OccurredAt.UTC().Format(time.RFC3339Nano),
		"published_at":         env.PublishedAt.UTC().Format(time.RFC3339Nano),
		"traceparent":          env.Traceparent,
		"tracestate":           env.Tracestate,
		"source_project":       env.SourceProject,
		"source_service":       env.SourceService,
		"schema_version":       "1",
		"correlation_id":       env.CorrelationID,
		"causation_id":         env.CausationID,
		"chora_imda_dimension": env.ChoraImdaDimension,
		"imda_lifecycle_stage": env.ImdaLifecycleStage,
	}
}

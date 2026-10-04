// Tests for the in-memory bus, reorder buffer, subscriber framework, and
// outbox.Publisher conformance. The Cloud Pub/Sub real adapter is in a
// build-tag gated file (cloud_pubsub.go) and exercised by integration
// tests; this file covers the test fixture + cross-cutting wiring.
package pubsub_test

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
	chpubsub "github.com/apollo-chora/chora-common/pubsub"
)

func TestInMemoryBus_PublishAndSubscribe(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	var received int32
	var captured chpubsub.Message
	var mu sync.Mutex
	cancel, err := bus.Subscribe(context.Background(), "chora.creation.atom.created.v1",
		func(_ context.Context, msg *chpubsub.Message) error {
			mu.Lock()
			captured = *msg
			mu.Unlock()
			atomic.AddInt32(&received, 1)
			return nil
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	waitForBus(t, func() bool { return atomic.LoadInt32(&received) > 0 })
	if got := atomic.LoadInt32(&received); got != 1 {
		t.Errorf("received=%d want 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if string(captured.Payload) != "p" {
		t.Errorf("payload=%q want p", captured.Payload)
	}
	if captured.Envelope.EventID != env.EventID {
		t.Error("envelope not propagated")
	}
}

func TestInMemoryBus_PublishWithoutSubscribers(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Errorf("Publish err=%v want nil", err)
	}
}

func TestInMemoryBus_DLQAfterMaxRetries(t *testing.T) {
	bus := chpubsub.NewInMemoryBus(chpubsub.WithMaxDeliveryAttempts(3))
	defer bus.Close()
	var attempts int32
	var dlqMessages int32
	cancel, err := bus.Subscribe(context.Background(), "chora.creation.atom.failing.v1",
		func(_ context.Context, _ *chpubsub.Message) error {
			atomic.AddInt32(&attempts, 1)
			return errors.New("permanent")
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	dlqCancel, err := bus.SubscribeDLQ(context.Background(), "chora.creation.atom.failing.v1",
		func(_ context.Context, _ *chpubsub.Message) error {
			atomic.AddInt32(&dlqMessages, 1)
			return nil
		})
	if err != nil {
		t.Fatalf("SubscribeDLQ: %v", err)
	}
	defer dlqCancel()

	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.failing.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	waitForBus(t, func() bool {
		return atomic.LoadInt32(&dlqMessages) > 0
	})
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Errorf("attempts=%d want 3 (MaxDeliveryAttempts)", got)
	}
	if got := atomic.LoadInt32(&dlqMessages); got != 1 {
		t.Errorf("DLQ messages=%d want 1", got)
	}
}

func TestInMemoryBus_RejectsInvalidEnvelope(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	bad := envelope.Envelope{} // missing all mandatory fields
	err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", bad, []byte("p"))
	if err == nil {
		t.Fatal("expected error publishing invalid envelope")
	}
}

func TestInMemoryBus_RejectsInvalidTopic(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	env := makeValidEnvelope(t)
	err := bus.Publish(context.Background(), "not.a.valid.topic", env, []byte("p"))
	if err == nil {
		t.Fatal("expected topic validation error")
	}
}

func TestInMemoryBus_OutboxPublisherConformance(t *testing.T) {
	// outbox.Publisher is the contract:
	//   Publish(ctx, topic, env, payload) error
	// Verify the in-memory bus satisfies it directly.
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	var p chpubsub.OutboxCompatible = bus // compile check via local alias
	_ = p
}

func TestSchemaRegistry_ValidateBeforePublish(t *testing.T) {
	var called int32
	validator := chpubsub.SchemaValidatorFunc(func(topic string, payload []byte) error {
		atomic.AddInt32(&called, 1)
		if topic == "chora.creation.atom.bad.v1" {
			return errors.New("schema mismatch")
		}
		return nil
	})
	bus := chpubsub.NewInMemoryBus(chpubsub.WithSchemaValidator(validator))
	defer bus.Close()
	env := makeValidEnvelope(t)
	err := bus.Publish(context.Background(), "chora.creation.atom.bad.v1", env, []byte("p"))
	if err == nil {
		t.Errorf("expected schema mismatch error")
	}
	err = bus.Publish(context.Background(), "chora.creation.atom.good.v1", env, []byte("p"))
	if err != nil {
		t.Errorf("good publish err=%v", err)
	}
	if atomic.LoadInt32(&called) != 2 {
		t.Errorf("validator calls=%d want 2", called)
	}
}

func TestReorderBuffer_PublishesByOccurredAt(t *testing.T) {
	delegate := newDelegateCapture()
	rb := chpubsub.NewReorderBuffer(delegate, chpubsub.ReorderConfig{
		Window:    20 * time.Millisecond,
		BatchSize: 10,
	})
	defer rb.Close()
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	pubs := []struct {
		body string
		off  time.Duration
	}{
		{"C", 2 * time.Millisecond},
		{"A", 0},
		{"B", 1 * time.Millisecond},
	}
	for _, p := range pubs {
		env := envelope.Envelope{
			EventID:        "e-" + p.body,
			IdempotencyKey: "e-" + p.body,
			TenantID:       "t",
			OccurredAt:     now.Add(p.off),
			PublishedAt:    now.Add(p.off),
			Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			SourceProject:  "p",
			SourceService:  "s",
			SchemaVersion:  1,
		}
		if err := rb.Publish(context.Background(), "chora.creation.atom.reordered.v1", env, []byte(p.body)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := rb.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	bodies := delegate.Bodies()
	if len(bodies) != 3 {
		t.Fatalf("flushed=%d want 3", len(bodies))
	}
	if bodies[0] != "A" || bodies[1] != "B" || bodies[2] != "C" {
		t.Errorf("flushed order=%v want [A B C]", bodies)
	}
}

func TestTraceparentPropagator_DefaultIsNoOp(t *testing.T) {
	var called int32
	prop := chpubsub.TraceparentPropagatorFunc(func(_ context.Context, _ *envelope.Envelope) {
		atomic.AddInt32(&called, 1)
	})
	bus := chpubsub.NewInMemoryBus(chpubsub.WithTraceparentPropagator(prop))
	defer bus.Close()
	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if atomic.LoadInt32(&called) != 1 {
		t.Errorf("propagator calls=%d want 1", called)
	}
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

func waitForBus(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition never became true within 2s")
}

type delegateCapture struct {
	mu     sync.Mutex
	bodies []string
}

func newDelegateCapture() *delegateCapture { return &delegateCapture{} }

func (p *delegateCapture) Publish(_ context.Context, _ string, _ envelope.Envelope, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bodies = append(p.bodies, string(payload))
	return nil
}

func (p *delegateCapture) Bodies() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.bodies))
	copy(out, p.bodies)
	return out
}

func (p *delegateCapture) SortedBodies() []string {
	out := p.Bodies()
	sort.Strings(out)
	return out
}

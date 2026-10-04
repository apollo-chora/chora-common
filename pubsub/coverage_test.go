package pubsub_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
	chpubsub "github.com/apollo-chora/chora-common/pubsub"
)

func TestInMemoryBus_WithRetryBackoff(t *testing.T) {
	// Just verify the option compiles + applies — backoff timing is tested
	// indirectly via DLQ test.
	bus := chpubsub.NewInMemoryBus(chpubsub.WithRetryBackoff(2 * time.Millisecond))
	defer bus.Close()
	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Errorf("Publish err=%v", err)
	}
}

func TestInMemoryBus_CloseIsIdempotent(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	bus.Close()
	bus.Close() // second close should be no-op
}

func TestInMemoryBus_PublishAfterCloseRejects(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	bus.Close()
	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err == nil {
		t.Error("expected error publishing to closed bus")
	}
}

func TestInMemoryBus_SubscribeRejectsInvalidTopic(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	_, err := bus.Subscribe(context.Background(), "bogus", func(_ context.Context, _ *chpubsub.Message) error { return nil })
	if err == nil {
		t.Error("expected topic validation error")
	}
}

func TestInMemoryBus_SubscribeRejectsNilHandler(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	_, err := bus.Subscribe(context.Background(), "chora.creation.atom.created.v1", nil)
	if err == nil {
		t.Error("expected nil-handler error")
	}
}

func TestInMemoryBus_SubscribeDLQRejectsInvalidTopic(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	_, err := bus.SubscribeDLQ(context.Background(), "bogus", func(_ context.Context, _ *chpubsub.Message) error { return nil })
	if err == nil {
		t.Error("expected topic validation error")
	}
}

func TestInMemoryBus_SubscribeDLQRejectsNilHandler(t *testing.T) {
	bus := chpubsub.NewInMemoryBus()
	defer bus.Close()
	_, err := bus.SubscribeDLQ(context.Background(), "chora.creation.atom.created.v1", nil)
	if err == nil {
		t.Error("expected nil-handler error")
	}
}

func TestInMemoryBus_SubscriberCancelStopsDelivery(t *testing.T) {
	bus := chpubsub.NewInMemoryBus(chpubsub.WithMaxDeliveryAttempts(10), chpubsub.WithRetryBackoff(20*time.Millisecond))
	defer bus.Close()
	var calls int32
	cancel, _ := bus.Subscribe(context.Background(), "chora.creation.atom.created.v1",
		func(_ context.Context, _ *chpubsub.Message) error {
			atomic.AddInt32(&calls, 1)
			return errors.New("boom") // forces retry
		})
	env := makeValidEnvelope(t)
	if err := bus.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// Wait for first call to land, then cancel.
	waitForBus(t, func() bool { return atomic.LoadInt32(&calls) >= 1 })
	cancel()
	// Cancel should stop further deliveries.
	time.Sleep(40 * time.Millisecond)
	final := atomic.LoadInt32(&calls)
	time.Sleep(40 * time.Millisecond)
	after := atomic.LoadInt32(&calls)
	if after-final > 1 {
		t.Errorf("calls grew %d -> %d after cancel; expected at most 1 in-flight", final, after)
	}
}

func TestReorderBuffer_DefaultBatchSize(t *testing.T) {
	delegate := newDelegateCapture()
	rb := chpubsub.NewReorderBuffer(delegate, chpubsub.ReorderConfig{
		Window: 5 * time.Millisecond,
	})
	defer rb.Close()
	env := makeValidEnvelope(t)
	if err := rb.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Errorf("Publish: %v", err)
	}
}

func TestReorderBuffer_PublishAfterCloseRejects(t *testing.T) {
	delegate := newDelegateCapture()
	rb := chpubsub.NewReorderBuffer(delegate, chpubsub.ReorderConfig{Window: 5 * time.Millisecond})
	rb.Close()
	env := makeValidEnvelope(t)
	if err := rb.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err == nil {
		t.Error("expected error publishing to closed buffer")
	}
}

func TestReorderBuffer_AutoFlushOnBatchFull(t *testing.T) {
	delegate := newDelegateCapture()
	rb := chpubsub.NewReorderBuffer(delegate, chpubsub.ReorderConfig{
		Window:    time.Hour, // never window-trigger
		BatchSize: 2,
	})
	defer rb.Close()
	env := makeValidEnvelope(t)
	for i := 0; i < 2; i++ {
		_ = rb.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p"))
	}
	if len(delegate.Bodies()) != 2 {
		t.Errorf("flushed=%d want 2 (auto-flush)", len(delegate.Bodies()))
	}
}

func TestCloudPublisher_RejectsNilClient(t *testing.T) {
	pub := chpubsub.NewCloudPublisher(nil)
	env := makeValidEnvelope(t)
	if err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err == nil {
		t.Error("expected error for nil client")
	}
}

func TestCloudPublisher_WithSchemaValidator(t *testing.T) {
	stub := &stubCloudClient{}
	var validated int32
	pub := chpubsub.NewCloudPublisher(stub,
		chpubsub.WithCloudSchemaValidator(chpubsub.SchemaValidatorFunc(func(_ string, _ []byte) error {
			atomic.AddInt32(&validated, 1)
			return nil
		})),
	)
	env := makeValidEnvelope(t)
	if err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if atomic.LoadInt32(&validated) != 1 {
		t.Errorf("validator calls=%d want 1", validated)
	}
}

func TestCloudPublisher_WithTraceparentPropagator(t *testing.T) {
	stub := &stubCloudClient{}
	var injected int32
	pub := chpubsub.NewCloudPublisher(stub,
		chpubsub.WithCloudTraceparentPropagator(chpubsub.TraceparentPropagatorFunc(func(_ context.Context, _ *envelope.Envelope) {
			atomic.AddInt32(&injected, 1)
		})),
	)
	env := makeValidEnvelope(t)
	if err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if atomic.LoadInt32(&injected) != 1 {
		t.Errorf("propagator calls=%d want 1", injected)
	}
}

func TestCloudPublisher_RejectsSchemaMismatch(t *testing.T) {
	stub := &stubCloudClient{}
	pub := chpubsub.NewCloudPublisher(stub,
		chpubsub.WithCloudSchemaValidator(chpubsub.SchemaValidatorFunc(func(_ string, _ []byte) error {
			return errors.New("mismatch")
		})),
	)
	env := makeValidEnvelope(t)
	if err := pub.Publish(context.Background(), "chora.creation.atom.created.v1", env, []byte("p")); err == nil {
		t.Error("expected schema mismatch error")
	}
}

func TestCloudSubscriber_RejectsNilClient(t *testing.T) {
	sub := chpubsub.NewCloudSubscriber(nil)
	err := sub.Subscribe(context.Background(), "test-sub", func(_ context.Context, _ *chpubsub.Message) error { return nil })
	if err == nil {
		t.Error("expected error for nil client")
	}
}

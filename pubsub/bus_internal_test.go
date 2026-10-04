// Internal-package tests for the in-memory bus delivery loop and the
// reorder buffer. These reach unexported state (the subscription struct) and
// exercise the cancel / parent-ctx delivery branches deterministically —
// without relying on goroutine scheduling races — so the branch coverage of
// the production code is complete for everything unit-testable without a
// live broker.
package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
	"github.com/5007-Capstone/chora/libs/chora-go-common/tracing"
)

const inMemTestTopic = "chora.creation.atom.created.v1"

// mustEnvelope builds an envelope that passes envelope.Validate (mirrors
// makeValidEnvelope in the external pubsub_test package, but this file is in
// the internal package so it needs its own copy).
func mustEnvelope(t *testing.T) envelope.Envelope {
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

// TestDeliver_ReturnsOnCanceledSubscription covers the `case <-sub.cancelCh:
// return` branch at the top of the delivery loop: a subscription whose
// cancelCh is already closed must NOT have its handler invoked.
func TestDeliver_ReturnsOnCanceledSubscription(t *testing.T) {
	bus := NewInMemoryBus(WithSynchronousDelivery())
	defer bus.Close()

	// Inject a subscription whose cancelCh is closed while it remains
	// registered. With synchronous delivery, Publish runs deliver() inline, so
	// the first select deterministically observes the closed channel.
	sub := &subscription{
		handler: func(_ context.Context, _ *Message) error {
			t.Error("handler must not be invoked for a canceled subscription")
			return nil
		},
		cancelCh: make(chan struct{}),
	}
	close(sub.cancelCh)
	bus.mu.Lock()
	bus.subscribers[inMemTestTopic] = append(bus.subscribers[inMemTestTopic], sub)
	bus.mu.Unlock()

	if err := bus.Publish(context.Background(), inMemTestTopic, mustEnvelope(t), []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// TestDeliver_ReturnsOnCanceledParent covers the `case <-parent.Done():
// return` branch at the top of the delivery loop.
func TestDeliver_ReturnsOnCanceledParent(t *testing.T) {
	bus := NewInMemoryBus(WithSynchronousDelivery())
	defer bus.Close()

	var calls int32
	cancelSub, err := bus.Subscribe(context.Background(), inMemTestTopic, func(_ context.Context, _ *Message) error {
		atomic.AddInt32(&calls, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancelSub()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before delivery begins
	if err := bus.Publish(ctx, inMemTestTopic, mustEnvelope(t), []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 0 {
		t.Errorf("handler calls=%d want 0 (parent ctx already canceled)", got)
	}
}

// TestDeliver_BackoffAbortsOnParentCancel covers the `case <-parent.Done():
// return` branch in the retry-backoff select: canceling the parent context
// from within a failing handler must abort the retry instead of sleeping.
func TestDeliver_BackoffAbortsOnParentCancel(t *testing.T) {
	// A huge backoff proves the abort: if the parent.Done() case did not fire
	// the select would sleep for the full hour and the test would hang.
	bus := NewInMemoryBus(WithSynchronousDelivery(), WithRetryBackoff(time.Hour))
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	var calls int32
	if _, err := bus.Subscribe(context.Background(), inMemTestTopic, func(c context.Context, _ *Message) error {
		atomic.AddInt32(&calls, 1)
		cancel() // cancel the parent while the first attempt is failing
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := bus.Publish(ctx, inMemTestTopic, mustEnvelope(t), []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("handler calls=%d want 1 (retry aborted on parent cancel)", got)
	}
}

// TestInMemoryBus_SynchronousDeliveryBlocksUntilHandlerCompletes proves
// WithSynchronousDelivery flips the bus into in-line delivery: Publish must
// not return until the handler has finished running.
func TestInMemoryBus_SynchronousDeliveryBlocksUntilHandlerCompletes(t *testing.T) {
	bus := NewInMemoryBus(WithSynchronousDelivery())
	defer bus.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	if _, err := bus.Subscribe(context.Background(), inMemTestTopic, func(_ context.Context, _ *Message) error {
		startedOnce.Do(func() { close(started) })
		<-release
		return nil
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	pubErr := make(chan error, 1)
	go func() { pubErr <- bus.Publish(context.Background(), inMemTestTopic, mustEnvelope(t), []byte("p")) }()

	select {
	case <-started:
	case err := <-pubErr:
		t.Fatalf("Publish returned before handler started: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("handler never started")
	}
	// Synchronous delivery: Publish is still blocked in the handler.
	select {
	case err := <-pubErr:
		t.Fatalf("Publish returned before handler completed (synchronous delivery): %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-pubErr; err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// errorDelegate is a bare-bones OutboxCompatible that always returns err —
// used to exercise the reorder buffer's delegate-error propagation.
type errorDelegate struct{ err error }

func (d errorDelegate) Publish(_ context.Context, _ string, _ envelope.Envelope, _ []byte) error {
	return d.err
}

// TestReorderBuffer_FlushPropagatesDelegateError covers the delegate-error
// return inside Flush's publish loop.
func TestReorderBuffer_FlushPropagatesDelegateError(t *testing.T) {
	want := errors.New("delegate blew up")
	rb := NewReorderBuffer(errorDelegate{err: want}, ReorderConfig{Window: time.Hour})
	defer rb.Close()

	if err := rb.Publish(context.Background(), inMemTestTopic, mustEnvelope(t), []byte("p")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := rb.Flush(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Flush err=%v want %v", err, want)
	}
}

// TestReorderBuffer_CloseIsIdempotent covers the already-closed early return
// in ReorderBuffer.Close.
func TestReorderBuffer_CloseIsIdempotent(t *testing.T) {
	rb := NewReorderBuffer(errorDelegate{}, ReorderConfig{Window: time.Hour})
	rb.Close()
	rb.Close() // second Close hits the `if r.closed.Swap(true) { return }`
}

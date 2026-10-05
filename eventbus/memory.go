package eventbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

// SchemaValidator is an optional payload-schema conformance check. Real
// implementations validate the Protobuf payload against a registered schema;
// tests inject a stub. This is transport-neutral (it does not assume a broker).
type SchemaValidator interface {
	ValidatePayload(subject string, payload []byte) error
}

// SchemaValidatorFunc adapts a function to the SchemaValidator interface.
type SchemaValidatorFunc func(subject string, payload []byte) error

// ValidatePayload satisfies SchemaValidator.
func (f SchemaValidatorFunc) ValidatePayload(subject string, payload []byte) error {
	return f(subject, payload)
}

// TraceparentPropagator injects/refreshs the W3C trace context onto an
// envelope before publication. Default is no-op (envelope already carries it).
type TraceparentPropagator interface {
	Inject(ctx context.Context, env *envelope.Envelope)
}

// TraceparentPropagatorFunc adapts a function to the propagator interface.
type TraceparentPropagatorFunc func(ctx context.Context, env *envelope.Envelope)

// Inject satisfies TraceparentPropagator.
func (f TraceparentPropagatorFunc) Inject(ctx context.Context, env *envelope.Envelope) {
	f(ctx, env)
}

// BusOption tunes a NewInMemoryBus call.
type BusOption func(*InMemoryBus)

// WithMaxDeliveryAttempts sets the per-message retry ceiling. Defaults to 5.
func WithMaxDeliveryAttempts(n int) BusOption {
	return func(b *InMemoryBus) {
		if n > 0 {
			b.maxAttempts = n
		}
	}
}

// WithSynchronousDelivery flips the bus into in-line delivery mode: Publish
// blocks until every subscriber's handler has completed. Useful in tests that
// assert receipt ordering. Default: async.
func WithSynchronousDelivery() BusOption {
	return func(b *InMemoryBus) { b.synchronous = true }
}

// WithSchemaValidator registers a payload-schema stub.
func WithSchemaValidator(v SchemaValidator) BusOption {
	return func(b *InMemoryBus) { b.schema = v }
}

// WithTraceparentPropagator registers a trace-context propagator stub.
func WithTraceparentPropagator(p TraceparentPropagator) BusOption {
	return func(b *InMemoryBus) { b.propagator = p }
}

// WithRetryBackoff sets the initial retry sleep between handler attempts.
func WithRetryBackoff(d time.Duration) BusOption {
	return func(b *InMemoryBus) {
		if d > 0 {
			b.retryBackoff = d
		}
	}
}

// InMemoryBus is a goroutine-safe in-process broker for tests and local dev. It
// satisfies Publisher so it can be passed to outbox relays in place of a real
// broker.
type InMemoryBus struct {
	mu             sync.Mutex
	subscribers    map[string][]*subscription
	dlqSubscribers map[string][]*subscription
	maxAttempts    int
	retryBackoff   time.Duration
	schema         SchemaValidator
	propagator     TraceparentPropagator
	closed         atomic.Bool
	deliveryWG     sync.WaitGroup
	synchronous    bool
}

type subscription struct {
	handler  Handler
	cancelCh chan struct{}
}

// NewInMemoryBus constructs a fresh in-memory bus. Defaults: 5 max delivery
// attempts, 1ms retry backoff, no schema validator, no-op propagator.
func NewInMemoryBus(opts ...BusOption) *InMemoryBus {
	b := &InMemoryBus{
		subscribers:    make(map[string][]*subscription),
		dlqSubscribers: make(map[string][]*subscription),
		maxAttempts:    5,
		retryBackoff:   time.Millisecond,
	}
	for _, o := range opts {
		o(b)
	}
	return b
}

// Close gracefully shuts the bus and waits for in-flight deliveries.
func (b *InMemoryBus) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	b.deliveryWG.Wait()
	return nil
}

// Subscribe registers handler against subject. Returns a cancel func.
func (b *InMemoryBus) Subscribe(_ context.Context, subject string, handler Handler) (func(), error) {
	if err := ValidateSubject(subject); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("eventbus: handler must not be nil")
	}
	sub := &subscription{handler: handler, cancelCh: make(chan struct{})}
	b.mu.Lock()
	b.subscribers[subject] = append(b.subscribers[subject], sub)
	b.mu.Unlock()
	return func() {
		close(sub.cancelCh)
		b.removeSub(subject, sub, false)
	}, nil
}

// SubscribeDLQ registers a handler for the DLQ companion subject. Messages
// that exceed MaxDeliveryAttempts are routed to DLQ subscribers registered for
// the SAME logical subject (the bus tracks DLQs internally).
func (b *InMemoryBus) SubscribeDLQ(_ context.Context, subject string, handler Handler) (func(), error) {
	if err := ValidateSubject(subject); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("eventbus: handler must not be nil")
	}
	sub := &subscription{handler: handler, cancelCh: make(chan struct{})}
	b.mu.Lock()
	b.dlqSubscribers[subject] = append(b.dlqSubscribers[subject], sub)
	b.mu.Unlock()
	return func() {
		close(sub.cancelCh)
		b.removeSub(subject, sub, true)
	}, nil
}

func (b *InMemoryBus) removeSub(subject string, sub *subscription, dlq bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	target := b.subscribers
	if dlq {
		target = b.dlqSubscribers
	}
	subs := target[subject]
	for i, s := range subs {
		if s == sub {
			target[subject] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Publish validates envelope + subject + (optional) schema, then delivers
// asynchronously to all current subscribers. It satisfies Publisher.
func (b *InMemoryBus) Publish(ctx context.Context, subject string, env envelope.Envelope, payload []byte) error {
	if b.closed.Load() {
		return errors.New("eventbus: bus closed")
	}
	if err := ValidateSubject(subject); err != nil {
		return fmt.Errorf("eventbus: %w", err)
	}
	if err := envelope.Validate(env); err != nil {
		return fmt.Errorf("eventbus: %w", err)
	}
	if b.schema != nil {
		if err := b.schema.ValidatePayload(subject, payload); err != nil {
			return fmt.Errorf("eventbus: schema: %w", err)
		}
	}
	if b.propagator != nil {
		envCopy := env
		b.propagator.Inject(ctx, &envCopy)
		env = envCopy
	}
	b.mu.Lock()
	subs := append([]*subscription(nil), b.subscribers[subject]...)
	b.mu.Unlock()
	for _, sub := range subs {
		s := sub
		if b.synchronous {
			b.deliver(ctx, subject, env, payload, s)
		} else {
			b.deliveryWG.Add(1)
			go b.deliver(ctx, subject, env, payload, s)
		}
	}
	return nil
}

func (b *InMemoryBus) deliver(parent context.Context, subject string, env envelope.Envelope, payload []byte, sub *subscription) {
	if !b.synchronous {
		defer b.deliveryWG.Done()
	}
	for attempt := uint64(1); attempt <= uint64(b.maxAttempts); attempt++ {
		select {
		case <-sub.cancelCh:
			return
		case <-parent.Done():
			return
		default:
		}
		msg := Message{
			Subject:         subject,
			Envelope:        env,
			Payload:         payload,
			DeliveryAttempt: attempt,
		}
		if err := sub.handler(parent, msg); err == nil {
			return
		}
		if attempt < uint64(b.maxAttempts) {
			select {
			case <-sub.cancelCh:
				return
			case <-parent.Done():
				return
			case <-time.After(b.retryBackoff * time.Duration(attempt)):
			}
		}
	}
	// Exhausted attempts -> DLQ.
	b.deliverDLQ(parent, subject, env, payload)
}

func (b *InMemoryBus) deliverDLQ(parent context.Context, subject string, env envelope.Envelope, payload []byte) {
	b.mu.Lock()
	dlqs := append([]*subscription(nil), b.dlqSubscribers[subject]...)
	b.mu.Unlock()
	for _, sub := range dlqs {
		b.deliveryWG.Add(1)
		s := sub
		go func() {
			defer b.deliveryWG.Done()
			msg := Message{
				Subject:         subject + ".dlq",
				Envelope:        env,
				Payload:         payload,
				DeliveryAttempt: 0,
			}
			_ = s.handler(parent, msg)
		}()
	}
}

// Compile-time check. InMemoryBus is a Publisher (usable as an outbox relay
// in tests) and exposes its own Subscribe/SubscribeDLQ for handler tests; it
// does not implement the full Bus interface because its Subscribe returns a
// cancel func rather than matching ConsumerConfig.
var _ Publisher = (*InMemoryBus)(nil)

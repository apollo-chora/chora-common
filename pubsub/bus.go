package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

// Message is the canonical wire envelope for a delivered subscriber message.
// Subscribers receive Message (envelope + raw bytes) so they can re-validate
// + decode independently.
type Message struct {
	Topic    string
	Envelope envelope.Envelope
	Payload  []byte

	// DeliveryAttempt is 1-indexed; >1 indicates a retry after a transient
	// handler error.
	DeliveryAttempt int
}

// Handler is a subscriber callback. Returning an error triggers retry +
// eventual DLQ routing per the bus's MaxDeliveryAttempts setting.
type Handler func(ctx context.Context, msg *Message) error

// SchemaValidator is the chora-platform Schema Registry contract. Real
// implementation: Cloud Pub/Sub Schema Registry client (lives in
// A-Platform-Infra). The in-memory bus accepts any SchemaValidator so
// tests can inject a stub that asserts envelope/payload conformance
// without spinning up the broker.
type SchemaValidator interface {
	// ValidatePayload checks the Protobuf payload against the schema
	// registered for the topic. Returns nil on success; an error if the
	// payload doesn't match.
	ValidatePayload(topic string, payload []byte) error
}

// SchemaValidatorFunc adapts a function to the SchemaValidator interface.
type SchemaValidatorFunc func(topic string, payload []byte) error

// ValidatePayload satisfies SchemaValidator.
func (f SchemaValidatorFunc) ValidatePayload(topic string, payload []byte) error {
	return f(topic, payload)
}

// TraceparentPropagator is the OTLP cross-Pub/Sub trace-context contract.
// Real implementation: A-Platform-Obs's OTLP propagator. Default is no-op
// (envelope.Traceparent already carries the W3C value).
type TraceparentPropagator interface {
	// Inject mutates env in place, stamping/refreshing the W3C trace
	// context from the supplied context. Implementations MUST not block.
	Inject(ctx context.Context, env *envelope.Envelope)
}

// TraceparentPropagatorFunc adapts a function to the propagator interface.
type TraceparentPropagatorFunc func(ctx context.Context, env *envelope.Envelope)

// Inject satisfies TraceparentPropagator.
func (f TraceparentPropagatorFunc) Inject(ctx context.Context, env *envelope.Envelope) {
	f(ctx, env)
}

// OutboxCompatible is a typed alias to compile-check that an in-memory bus
// satisfies the outbox.Publisher contract from chora-go-common/outbox
// (Publish(ctx, topic string, env envelope.Envelope, payload []byte) error).
//
// The aliased outbox.Publisher signature is reproduced locally to avoid an
// import cycle (outbox depends on pubsub for tests; pubsub does NOT depend
// on outbox).
type OutboxCompatible interface {
	Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error
}

// ----------------------------------------------------------------------------
// In-memory bus (test/dev fixture)
// ----------------------------------------------------------------------------

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

// WithSynchronousDelivery flips the bus into in-line delivery mode:
// Publish blocks until every subscriber's handler has completed.
// Useful in tests that assert receipt ordering — production Cloud Pub/Sub
// is asynchronous so callers MUST not rely on this. Default: async.
func WithSynchronousDelivery() BusOption {
	return func(b *InMemoryBus) { b.synchronous = true }
}

// WithSchemaValidator registers a Schema Registry stub.
func WithSchemaValidator(v SchemaValidator) BusOption {
	return func(b *InMemoryBus) { b.schema = v }
}

// WithTraceparentPropagator registers an OTLP propagator stub.
func WithTraceparentPropagator(p TraceparentPropagator) BusOption {
	return func(b *InMemoryBus) { b.propagator = p }
}

// WithRetryBackoff sets the initial retry sleep between handler attempts.
// Tests typically use a tiny value to keep the suite fast.
func WithRetryBackoff(d time.Duration) BusOption {
	return func(b *InMemoryBus) {
		if d > 0 {
			b.retryBackoff = d
		}
	}
}

// InMemoryBus is a goroutine-safe in-process broker. It satisfies
// OutboxCompatible so it can be passed to outbox.NewRelay in unit tests
// in place of a real Cloud Pub/Sub publisher.
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

// NewInMemoryBus constructs a fresh in-memory bus. Defaults: 5 max
// delivery attempts, 1ms retry backoff, no schema validator, no-op
// traceparent propagator.
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
func (b *InMemoryBus) Close() {
	if b.closed.Swap(true) {
		return
	}
	b.deliveryWG.Wait()
}

// Subscribe registers handler against topic. Returns a cancel func.
func (b *InMemoryBus) Subscribe(_ context.Context, topic string, handler Handler) (func(), error) {
	if err := ValidateTopicName(topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("pubsub: handler must not be nil")
	}
	sub := &subscription{handler: handler, cancelCh: make(chan struct{})}
	b.mu.Lock()
	b.subscribers[topic] = append(b.subscribers[topic], sub)
	b.mu.Unlock()
	return func() {
		close(sub.cancelCh)
		b.removeSub(topic, sub, false)
	}, nil
}

// SubscribeDLQ registers a handler for the DLQ companion topic.
//
// In a real Cloud Pub/Sub deployment, the DLQ is a separate topic
// `chora.dlq.{primary_topic}` (per pub-sub-topology skill). The in-memory
// fixture mirrors that semantics: messages that exceed MaxDeliveryAttempts
// are routed to DLQ subscribers registered for the SAME logical topic
// name (the bus tracks DLQs internally).
func (b *InMemoryBus) SubscribeDLQ(_ context.Context, topic string, handler Handler) (func(), error) {
	if err := ValidateTopicName(topic); err != nil {
		return nil, err
	}
	if handler == nil {
		return nil, errors.New("pubsub: handler must not be nil")
	}
	sub := &subscription{handler: handler, cancelCh: make(chan struct{})}
	b.mu.Lock()
	b.dlqSubscribers[topic] = append(b.dlqSubscribers[topic], sub)
	b.mu.Unlock()
	return func() {
		close(sub.cancelCh)
		b.removeSub(topic, sub, true)
	}, nil
}

func (b *InMemoryBus) removeSub(topic string, sub *subscription, dlq bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	target := b.subscribers
	if dlq {
		target = b.dlqSubscribers
	}
	subs := target[topic]
	for i, s := range subs {
		if s == sub {
			target[topic] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Publish satisfies OutboxCompatible + the wider Pub/Sub publisher
// contract. Validates envelope + topic + (optional) Schema Registry, then
// delivers asynchronously to all current subscribers.
func (b *InMemoryBus) Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error {
	if b.closed.Load() {
		return errors.New("pubsub: bus closed")
	}
	if err := ValidateTopicName(topic); err != nil {
		return fmt.Errorf("pubsub: %w", err)
	}
	if err := envelope.Validate(env); err != nil {
		return fmt.Errorf("pubsub: %w", err)
	}
	if b.schema != nil {
		if err := b.schema.ValidatePayload(topic, payload); err != nil {
			return fmt.Errorf("pubsub: schema: %w", err)
		}
	}
	if b.propagator != nil {
		envCopy := env
		b.propagator.Inject(ctx, &envCopy)
		env = envCopy
	}
	b.mu.Lock()
	subs := append([]*subscription(nil), b.subscribers[topic]...)
	b.mu.Unlock()
	for _, sub := range subs {
		s := sub
		if b.synchronous {
			b.deliver(ctx, topic, env, payload, s)
		} else {
			b.deliveryWG.Add(1)
			go b.deliver(ctx, topic, env, payload, s)
		}
	}
	return nil
}

func (b *InMemoryBus) deliver(parent context.Context, topic string, env envelope.Envelope, payload []byte, sub *subscription) {
	if !b.synchronous {
		defer b.deliveryWG.Done()
	}
	for attempt := 1; attempt <= b.maxAttempts; attempt++ {
		select {
		case <-sub.cancelCh:
			return
		case <-parent.Done():
			return
		default:
		}
		msg := &Message{
			Topic:           topic,
			Envelope:        env,
			Payload:         payload,
			DeliveryAttempt: attempt,
		}
		if err := sub.handler(parent, msg); err == nil {
			return
		}
		if attempt < b.maxAttempts {
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
	b.deliverDLQ(parent, topic, env, payload)
}

func (b *InMemoryBus) deliverDLQ(parent context.Context, topic string, env envelope.Envelope, payload []byte) {
	b.mu.Lock()
	dlqs := append([]*subscription(nil), b.dlqSubscribers[topic]...)
	b.mu.Unlock()
	for _, sub := range dlqs {
		b.deliveryWG.Add(1)
		s := sub
		go func() {
			defer b.deliveryWG.Done()
			msg := &Message{
				Topic:           topic + ".dlq",
				Envelope:        env,
				Payload:         payload,
				DeliveryAttempt: -1,
			}
			_ = s.handler(parent, msg)
		}()
	}
}

// Compile-time check.
var _ OutboxCompatible = (*InMemoryBus)(nil)

// ----------------------------------------------------------------------------
// Reorder buffer
// ----------------------------------------------------------------------------

// ReorderConfig tunes a NewReorderBuffer call.
type ReorderConfig struct {
	// Window is the longest the buffer holds a message before flushing it.
	Window time.Duration
	// BatchSize caps the in-buffer message count; the buffer auto-flushes
	// once the threshold is hit.
	BatchSize int
}

// reorderEntry is a single buffered message awaiting flush.
type reorderEntry struct {
	topic   string
	env     envelope.Envelope
	payload []byte
}

// ReorderBuffer wraps a delegate publisher and emits messages sorted by
// occurred_at within Window. Used by the outbox.Relay AND by services
// that publish events from concurrent producers and want best-effort
// domain-event ordering.
type ReorderBuffer struct {
	mu       sync.Mutex
	delegate OutboxCompatible
	cfg      ReorderConfig
	queue    []reorderEntry
	closed   atomic.Bool
}

// NewReorderBuffer constructs a ReorderBuffer wrapping the supplied
// publisher.
func NewReorderBuffer(delegate OutboxCompatible, cfg ReorderConfig) *ReorderBuffer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	return &ReorderBuffer{delegate: delegate, cfg: cfg}
}

// Publish enqueues the message for reorder. May trigger a flush if BatchSize
// is hit.
func (r *ReorderBuffer) Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error {
	if r.closed.Load() {
		return errors.New("pubsub: reorder buffer closed")
	}
	r.mu.Lock()
	r.queue = append(r.queue, reorderEntry{topic: topic, env: env, payload: payload})
	full := len(r.queue) >= r.cfg.BatchSize
	r.mu.Unlock()
	if full {
		return r.Flush(ctx)
	}
	return nil
}

// Flush sorts the buffered entries by occurred_at and delegates to the
// downstream publisher.
func (r *ReorderBuffer) Flush(ctx context.Context) error {
	r.mu.Lock()
	if len(r.queue) == 0 {
		r.mu.Unlock()
		return nil
	}
	q := r.queue
	r.queue = nil
	r.mu.Unlock()

	sort.SliceStable(q, func(i, j int) bool {
		return q[i].env.OccurredAt.Before(q[j].env.OccurredAt)
	})

	for _, e := range q {
		if err := r.delegate.Publish(ctx, e.topic, e.env, e.payload); err != nil {
			return err
		}
	}
	return nil
}

// Close flushes the buffer and prevents further publishes.
func (r *ReorderBuffer) Close() {
	if r.closed.Swap(true) {
		return
	}
	_ = r.Flush(context.Background())
}

// Compile-time check.
var _ OutboxCompatible = (*ReorderBuffer)(nil)

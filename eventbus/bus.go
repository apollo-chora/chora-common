// Package eventbus is the broker-neutral event transport for the chora
// platform. It replaces the Google Pub/Sub-specific pubsub package with a
// small, transport-agnostic contract: publishers emit an envelope + payload to
// a subject; subscribers receive Message values and signal success/failure by
// returning nil/error.
//
// The canonical event taxonomy (chora.{domain}.{aggregate}.{event_type}.v{N})
// is unchanged — these strings are valid NATS subjects, so the same event
// names carry over without renaming.
//
// Responsibilities are deliberately separated:
//   - eventbus owns transport delivery (one logical publish, ack/nack, DLQ).
//   - outbox owns durable producer retry (the outbox dispatcher retries).
//   - idempotent owns consumer-side duplicate suppression.
//   - domain handlers own business success/failure.
package eventbus

import (
	"context"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

// Message is the canonical wire envelope for a delivered message. Subscribers
// receive Message (envelope + raw bytes) so they can re-validate + decode
// independently.
type Message struct {
	Subject  string
	Envelope envelope.Envelope
	Payload  []byte

	// DeliveryAttempt is 1-indexed; >1 indicates a retry after a transient
	// handler error.
	DeliveryAttempt uint64
}

// Handler is a subscriber callback. Returning nil acknowledges the message;
// returning an error triggers redelivery and, after MaxDeliver attempts, DLQ
// routing. This mirrors the previous CloudSubscriber ack/nack behaviour without
// exposing broker settlement to domain handlers.
type Handler func(ctx context.Context, msg Message) error

// ConsumerConfig configures a durable subscription.
type ConsumerConfig struct {
	// Name is the durable consumer name (NATS durable / Pub/Sub subscription).
	Name string
	// Subject is the canonical event subject to subscribe to.
	Subject string
	// MaxDeliver is the redelivery ceiling before DLQ routing. Defaults to 5.
	MaxDeliver int
	// AckWait is how long a delivered message may be unacked before redelivery.
	AckWait time.Duration
	// Backoff is the per-attempt backoff schedule (optional).
	Backoff []time.Duration
	// DLQSubject is the dead-letter subject for exhausted messages. If empty, a
	// conventional _dlq.<subject> is used.
	DLQSubject string
}

// Publisher publishes an event to a subject.
type Publisher interface {
	Publish(ctx context.Context, subject string, env envelope.Envelope, payload []byte) error
}

// Subscriber subscribes to a subject with a handler.
type Subscriber interface {
	Subscribe(ctx context.Context, cfg ConsumerConfig, handler Handler) error
}

// Bus is the combined publisher + subscriber + lifecycle contract.
type Bus interface {
	Publisher
	Subscriber
	Close() error
}

// DLQSubject returns the dead-letter subject for an event subject. The single
// convention is the transport prefix `_dlq.` on the original subject
// (`_dlq.chora.observability.token_usage.recorded.v1`), which the root Compose
// CHORA_DLQ stream captures via `_dlq.>`. Callers that leave
// ConsumerConfig.DLQSubject empty get this subject; callers that set it
// explicitly should use this helper so every service dead-letters identically.
//
// The result is an infrastructure address, not a domain event name, so it is
// deliberately NOT run through ValidateSubject.
func DLQSubject(subject string) string {
	return "_dlq." + subject
}

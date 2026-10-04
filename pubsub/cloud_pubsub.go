// Package pubsub also provides a Cloud Pub/Sub adapter contract. The real
// `cloud.google.com/go/pubsub` client is wired in service-level adapter
// packages (e.g. services/chora-creation/internal/adapter/pubsub) — this
// file declares a thin interface those adapters implement so the
// chora-common library stays free of GCP-client dependencies.
//
// Design rationale:
//
//   - The library is a leaf package; adding cloud.google.com/go/pubsub as a
//     dependency would force every consumer (including unit-test-only ones)
//     to pull in the gRPC + GCP auth chain.
//   - Service adapters import this package + cloud.google.com/go/pubsub,
//     wrap a *pubsub.Client + Topic, and satisfy the RawPublisher interface
//     declared in pubsub.go.
//   - Authentication uses Workload Identity Federation (WIF) — service
//     account keys NEVER travel cross-project per CLAUDE.md §6 + the
//     secrets-and-env skill. The WIF wiring is owned by A-Platform-Sec.
//
// See chora-infra/topics/topics.yaml for the canonical topic catalogue and
// chora-infra/terraform/modules/m10-data-plane for the broker provisioning.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

// CloudPubSubClient is the minimal subset of cloud.google.com/go/pubsub we
// consume. Service adapters typically wrap *pubsub.Client + *pubsub.Topic
// and satisfy this interface so this package can exercise its envelope-
// projection + retry logic without importing the full GCP client.
type CloudPubSubClient interface {
	// PublishMessage sends a message to the topic with the supplied
	// attributes. The implementation handles batching + flow control via
	// the GCP client's defaults; retries are managed by Topic.PublishSettings.
	PublishMessage(ctx context.Context, topic string, data []byte, attrs map[string]string) (serverMessageID string, err error)

	// SubscriptionReceive starts a Pull-style receive loop on the named
	// subscription, dispatching messages to handler. Returns when ctx is
	// canceled or the underlying client returns a permanent error.
	//
	// The handler MUST call Ack() on the supplied AckFunc on success or
	// Nack() on transient failure. If the handler returns an error
	// without explicitly Ack/Nack-ing, the implementation MUST Nack().
	SubscriptionReceive(ctx context.Context, subscription string, handler func(ctx context.Context, msg CloudMessage) error) error
}

// CloudMessage is the wire-shape a Cloud Pub/Sub subscriber sees. The real
// adapter wraps *pubsub.Message; tests use a stub.
type CloudMessage struct {
	ID          string
	Data        []byte
	Attributes  map[string]string
	PublishTime time.Time

	// Ack acknowledges the message. Ack/Nack is mutually exclusive; only
	// one wins per message.
	Ack func()
	// Nack negative-acknowledges the message — it will be redelivered
	// per the subscription's retry_policy and routed to its DLQ topic
	// after max_delivery_attempts.
	Nack func()
}

// CloudPublisher wraps a CloudPubSubClient with envelope validation +
// attribute projection (the same projection used by the InMemoryBus —
// kept consistent so subscribers see the same attribute keys regardless
// of broker).
type CloudPublisher struct {
	client     CloudPubSubClient
	schema     SchemaValidator
	propagator TraceparentPropagator
}

// NewCloudPublisher constructs a CloudPublisher.
func NewCloudPublisher(client CloudPubSubClient, opts ...CloudPublisherOption) *CloudPublisher {
	p := &CloudPublisher{client: client}
	for _, o := range opts {
		o(p)
	}
	return p
}

// CloudPublisherOption tunes a CloudPublisher.
type CloudPublisherOption func(*CloudPublisher)

// WithCloudSchemaValidator registers a Schema Registry stub.
func WithCloudSchemaValidator(v SchemaValidator) CloudPublisherOption {
	return func(p *CloudPublisher) { p.schema = v }
}

// WithCloudTraceparentPropagator registers an OTLP propagator stub.
func WithCloudTraceparentPropagator(prop TraceparentPropagator) CloudPublisherOption {
	return func(p *CloudPublisher) { p.propagator = prop }
}

// Publish satisfies OutboxCompatible. Validates envelope + topic +
// (optional) Schema Registry, then delegates to the wrapped client.
func (p *CloudPublisher) Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error {
	if p.client == nil {
		return errors.New("pubsub: cloud publisher missing client")
	}
	if err := ValidateTopicName(topic); err != nil {
		return fmt.Errorf("pubsub: %w", err)
	}
	if err := envelope.Validate(env); err != nil {
		return fmt.Errorf("pubsub: %w", err)
	}
	if p.schema != nil {
		if err := p.schema.ValidatePayload(topic, payload); err != nil {
			return fmt.Errorf("pubsub: schema: %w", err)
		}
	}
	if p.propagator != nil {
		envCopy := env
		p.propagator.Inject(ctx, &envCopy)
		env = envCopy
	}
	attrs := envelopeAttributes(env)
	// Stamp the destination topic as a message attribute. Push dispatchers
	// downstream route/guard on msg.Attributes["topic"]; without this the
	// attribute is absent and consumers that strict-match the topic silently
	// drop every real event (see feedback_pubsub_push_topic_attr). Additive +
	// backward-compatible: consumers either ignore, tolerate, or now correctly
	// match it. The Schema Registry validates the payload, not attributes.
	attrs["topic"] = topic
	if _, err := p.client.PublishMessage(ctx, topic, payload, attrs); err != nil {
		return fmt.Errorf("pubsub: publish %s: %w", topic, err)
	}
	return nil
}

// Compile-time check.
var _ OutboxCompatible = (*CloudPublisher)(nil)

// ----------------------------------------------------------------------------
// Subscriber framework
// ----------------------------------------------------------------------------

// CloudSubscriber wraps a CloudPubSubClient with a typed Handler API. It
// extracts the envelope from message attributes, calls the handler, and
// ack/nacks on the caller's behalf.
//
// On handler error (or a malformed envelope): the inner handler Nacks and
// returns the error — the broker handles retry + DLQ per the subscription's
// retry_policy + dead_letter_policy (configured in
// chora-infra/terraform/modules/m10-data-plane). The returned error is logged
// centrally one layer down, at the GCPClient.SubscriptionReceive dispatch
// (dispatchReceivedMessage in gcp_client.go) — the SOLE subscriber-side
// failure log, so the D5 "never silent" contract still holds without
// double-logging the same failure at two layers of the same call path.
type CloudSubscriber struct {
	client CloudPubSubClient
}

// NewCloudSubscriber constructs a subscriber wrapper. Subscriber-side delivery
// failures are surfaced centrally at the dispatch layer
// (dispatchReceivedMessage in gcp_client.go), which logs the returned error.
func NewCloudSubscriber(client CloudPubSubClient) *CloudSubscriber {
	return &CloudSubscriber{client: client}
}

// Subscribe runs the receive loop on the named subscription. Handler
// returns nil on success (Ack); error on failure (Nack).
//
// Subscribe blocks until ctx is canceled or the client returns a
// permanent error.
func (s *CloudSubscriber) Subscribe(ctx context.Context, subscription string, handler Handler) error {
	if s.client == nil {
		return errors.New("pubsub: subscriber missing client")
	}
	return s.client.SubscriptionReceive(ctx, subscription, func(ctx context.Context, m CloudMessage) error {
		env, err := envelopeFromAttributes(m.Attributes)
		if err != nil {
			// Malformed envelope — Nack so the broker retries; persistent
			// malformed messages will eventually deadletter. Return the err so
			// it is logged centrally at the dispatch layer
			// (dispatchReceivedMessage in gcp_client.go) — never silent.
			m.Nack()
			return err
		}
		msg := &Message{
			Topic:           m.Attributes["topic"],
			Envelope:        env,
			Payload:         m.Data,
			DeliveryAttempt: 1, // Cloud Pub/Sub doesn't always expose attempt count
		}
		if err := handler(ctx, msg); err != nil {
			// Handler reported a transient failure — Nack for broker retry +
			// DLQ. Return the err so it is logged centrally at the dispatch
			// layer (dispatchReceivedMessage in gcp_client.go) — never silent.
			m.Nack()
			return err
		}
		m.Ack()
		return nil
	})
}

// envelopeFromAttributes reconstructs an envelope from the canonical
// attribute set produced by envelopeAttributes(). Used by subscribers.
func envelopeFromAttributes(attrs map[string]string) (envelope.Envelope, error) {
	if len(attrs) == 0 {
		return envelope.Envelope{}, errors.New("pubsub: no attributes")
	}
	occ, err := time.Parse(time.RFC3339Nano, attrs["occurred_at"])
	if err != nil {
		return envelope.Envelope{}, fmt.Errorf("envelope occurred_at: %w", err)
	}
	pub, err := time.Parse(time.RFC3339Nano, attrs["published_at"])
	if err != nil {
		return envelope.Envelope{}, fmt.Errorf("envelope published_at: %w", err)
	}
	var schemaVersion int32
	if v, ok := attrs["schema_version"]; ok {
		var sv int64
		_, err := fmt.Sscanf(v, "%d", &sv)
		if err == nil {
			schemaVersion = int32(sv)
		}
	}
	return envelope.Envelope{
		EventID:            attrs["event_id"],
		IdempotencyKey:     attrs["idempotency_key"],
		TenantID:           attrs["tenant_id"],
		GCID:               attrs["gcid"],
		OccurredAt:         occ,
		PublishedAt:        pub,
		Traceparent:        attrs["traceparent"],
		Tracestate:         attrs["tracestate"],
		SourceProject:      attrs["source_project"],
		SourceService:      attrs["source_service"],
		SchemaVersion:      schemaVersion,
		CorrelationID:      attrs["correlation_id"],
		CausationID:        attrs["causation_id"],
		ChoraImdaDimension: attrs["chora_imda_dimension"],
		ImdaLifecycleStage: attrs["imda_lifecycle_stage"],
	}, nil
}

package eventbus

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DefaultStream is the JetStream stream provisioned by the root Compose
// nats-init service (subjects chora.>). Consumers are created on this stream.
const DefaultStream = "CHORA_EVENTS"

// JetStreamConfig configures a JetStreamBus.
type JetStreamConfig struct {
	// URL is the NATS server URL (e.g. nats://nats:4222).
	URL string
	// StreamName is the JetStream stream to consume from. Defaults to
	// DefaultStream (provisioned by nats-init).
	StreamName string
	// ConnOptions are optional nats.Option for the connection.
	ConnOptions []nats.Option
}

// JetStreamBus is a Bus backed by NATS JetStream. It assumes the target stream
// already exists (the root Compose nats-init service provisions CHORA_EVENTS
// with subjects chora.>).
type JetStreamBus struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	stream string
}

// NewJetStream connects to NATS and returns a JetStreamBus.
func NewJetStream(cfg JetStreamConfig) (*JetStreamBus, error) {
	if cfg.URL == "" {
		return nil, errors.New("eventbus: NATS URL required")
	}
	stream := cfg.StreamName
	if stream == "" {
		stream = DefaultStream
	}
	nc, err := nats.Connect(cfg.URL, cfg.ConnOptions...)
	if err != nil {
		return nil, fmt.Errorf("eventbus: nats connect: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("eventbus: jetstream: %w", err)
	}
	return &JetStreamBus{nc: nc, js: js, stream: stream}, nil
}

// Close closes the underlying NATS connection.
func (b *JetStreamBus) Close() error {
	b.nc.Close()
	return nil
}

// Publish publishes an event to a subject with envelope metadata as NATS
// headers. It makes one logical broker publish and reports success/failure;
// durable producer retry belongs to the outbox dispatcher, not here.
func (b *JetStreamBus) Publish(ctx context.Context, subject string, env envelope.Envelope, payload []byte) error {
	if err := ValidateSubject(subject); err != nil {
		return fmt.Errorf("eventbus: %w", err)
	}
	if err := envelope.Validate(env); err != nil {
		return fmt.Errorf("eventbus: %w", err)
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  envelopeHeaders(env),
	}
	if _, err := b.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("eventbus: publish: %w", err)
	}
	return nil
}

// Subscribe creates a durable consumer on the configured stream and starts a
// consume loop. The loop acks on handler success and naks on error; once
// MaxDeliver is reached the message is republished to the DLQ subject and acked.
func (b *JetStreamBus) Subscribe(ctx context.Context, cfg ConsumerConfig, handler Handler) error {
	if err := ValidateSubject(cfg.Subject); err != nil {
		return err
	}
	if handler == nil {
		return errors.New("eventbus: handler must not be nil")
	}
	maxDeliver := cfg.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 5
	}
	ackWait := cfg.AckWait
	if ackWait <= 0 {
		ackWait = 30 * time.Second
	}
	cons, err := b.js.CreateOrUpdateConsumer(ctx, b.stream, jetstream.ConsumerConfig{
		Durable:       cfg.Name,
		FilterSubject: cfg.Subject,
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxDeliver:    maxDeliver,
		AckWait:       ackWait,
	})
	if err != nil {
		return fmt.Errorf("eventbus: consumer: %w", err)
	}
	go b.consumeLoop(ctx, cons, cfg, handler)
	return nil
}

func (b *JetStreamBus) consumeLoop(ctx context.Context, cons jetstream.Consumer, cfg ConsumerConfig, handler Handler) {
	iter, err := cons.Messages()
	if err != nil {
		return
	}
	defer iter.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		msg, err := iter.Next()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		b.handleMsg(ctx, msg, cfg, handler)
	}
}

func (b *JetStreamBus) handleMsg(ctx context.Context, msg jetstream.Msg, cfg ConsumerConfig, handler Handler) {
	env, err := envelopeFromHeaders(msg.Headers())
	if err != nil {
		// Cannot even parse the envelope — ack to avoid a redelivery loop.
		_ = msg.Ack()
		return
	}
	var delivered uint64
	if md, err := msg.Metadata(); err == nil {
		delivered = md.NumDelivered
	}
	m := Message{
		Subject:         msg.Subject(),
		Envelope:        env,
		Payload:         msg.Data(),
		DeliveryAttempt: delivered,
	}
	if err := handler(ctx, m); err == nil {
		_ = msg.Ack()
		return
	}
	// Handler failed. If the redelivery ceiling is reached, route to DLQ and
	// ack so JetStream does not redeliver; otherwise nak for redelivery.
	maxDeliver := cfg.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 5
	}
	if delivered >= uint64(maxDeliver) {
		dlq := cfg.DLQSubject
		if dlq == "" {
			dlq = "_dlq." + cfg.Subject
		}
		republish := &nats.Msg{
			Subject: dlq,
			Data:    msg.Data(),
			Header:  msg.Headers(),
		}
		_, _ = b.js.PublishMsg(ctx, republish)
		_ = msg.Ack()
		return
	}
	_ = msg.Nak()
}

// envelopeHeaders projects an envelope onto NATS headers so subscribers and
// observers can read tenant_id / traceparent / etc. without decoding the payload.
func envelopeHeaders(env envelope.Envelope) nats.Header {
	h := nats.Header{}
	set := func(k, v string) {
		if v != "" {
			h.Set(k, v)
		}
	}
	set("Chora-Event-Id", env.EventID)
	set("Chora-Idempotency-Key", env.IdempotencyKey)
	set("Chora-Tenant-Id", env.TenantID)
	set("Chora-Gcid", env.GCID)
	set("Chora-Source-Service", env.SourceService)
	set("Chora-Source-Project", env.SourceProject)
	set("Chora-Schema-Version", strconv.FormatInt(int64(env.SchemaVersion), 10))
	set("Traceparent", env.Traceparent)
	set("Tracestate", env.Tracestate)
	if !env.OccurredAt.IsZero() {
		h.Set("Chora-Occurred-At", env.OccurredAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
	}
	if !env.PublishedAt.IsZero() {
		h.Set("Chora-Published-At", env.PublishedAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"))
	}
	return h
}

// envelopeFromHeaders reconstructs an envelope from NATS headers. Fields not
// present as headers fall back to the zero value.
func envelopeFromHeaders(h nats.Header) (envelope.Envelope, error) {
	get := func(k string) string { return h.Get(k) }
	env := envelope.Envelope{
		EventID:        get("Chora-Event-Id"),
		IdempotencyKey: get("Chora-Idempotency-Key"),
		TenantID:       get("Chora-Tenant-Id"),
		GCID:           get("Chora-Gcid"),
		SourceService:  get("Chora-Source-Service"),
		SourceProject:  get("Chora-Source-Project"),
		Traceparent:    get("Traceparent"),
		Tracestate:     get("Tracestate"),
	}
	if v := get("Chora-Schema-Version"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			env.SchemaVersion = int32(n)
		}
	}
	return env, envelope.Validate(env)
}

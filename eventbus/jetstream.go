package eventbus

import (
	"context"
	"errors"
	"fmt"
	"log"
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
//
// The envelope's EventID is projected onto the Nats-Msg-Id header so that a
// redelivery of the same outbox row (the publish succeeded but the process
// died before MarkPublished) is suppressed by the stream's duplicate window.
// That is what makes producer retries idempotent at the broker without
// changing the Publisher interface.
func (b *JetStreamBus) Publish(ctx context.Context, subject string, env envelope.Envelope, payload []byte) error {
	if err := ValidateSubject(subject); err != nil {
		return fmt.Errorf("eventbus: %w", err)
	}
	if err := envelope.Validate(env); err != nil {
		return fmt.Errorf("eventbus: %w", err)
	}
	hdr := envelopeHeaders(env)
	if env.EventID != "" {
		hdr.Set(jetstream.MsgIDHeader, env.EventID)
	}
	msg := &nats.Msg{
		Subject: subject,
		Data:    payload,
		Header:  hdr,
	}
	if _, err := b.js.PublishMsg(ctx, msg); err != nil {
		return fmt.Errorf("eventbus: publish: %w", err)
	}
	return nil
}

// Subscribe creates a durable consumer on the configured stream and starts a
// consume loop. The loop acks on handler success and naks on error; once
// MaxDeliver is reached the message is moved to the DLQ subject. A message is
// only acknowledged after it has been safely preserved (handled, or written to
// the DLQ) — a failed DLQ write never acknowledges the original.
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
		BackOff:       cfg.Backoff,
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
		// The envelope cannot be interpreted, so the payload cannot be handled
		// safely. Preserve it on the DLQ rather than dropping it silently.
		b.deadLetter(ctx, msg, cfg, err)
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
	// Handler failed. If the redelivery ceiling is reached, route to the DLQ;
	// otherwise nak for redelivery.
	maxDeliver := cfg.MaxDeliver
	if maxDeliver <= 0 {
		maxDeliver = 5
	}
	if delivered >= uint64(maxDeliver) {
		b.deadLetter(ctx, msg, cfg, nil)
		return
	}
	_ = msg.Nak()
}

// deadLetter republishes a message to the consumer's DLQ subject. The original
// is acknowledged only once the DLQ write has succeeded. If the write cannot
// be completed the original is left unsettled — never acknowledged — so the
// event is not lost; the failure is logged for an operator.
func (b *JetStreamBus) deadLetter(ctx context.Context, msg jetstream.Msg, cfg ConsumerConfig, cause error) {
	dlq := cfg.DLQSubject
	if dlq == "" {
		dlq = "_dlq." + cfg.Subject
	}
	republish := &nats.Msg{Subject: dlq, Data: msg.Data(), Header: dlqHeaders(msg.Headers(), cfg, cause)}

	schedule := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= len(schedule); attempt++ {
		if _, err := b.js.PublishMsg(ctx, republish); err == nil {
			_ = msg.Ack()
			return
		} else {
			lastErr = err
		}
		if attempt == len(schedule) || ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(schedule[attempt]):
		}
	}
	log.Printf("eventbus: DLQ publish failed after retries (consumer=%s subject=%s dlq=%s): %v — original left unacked",
		cfg.Name, cfg.Subject, dlq, lastErr)
}

// dlqHeaders builds the header set for a dead-letter copy of src. The copy must
// not reuse the source message's broker id, or JetStream would suppress it as a
// duplicate of the very message it is dead-lettering; it gets a derived id
// instead so repeated DLQ attempts for one event still deduplicate.
func dlqHeaders(src nats.Header, cfg ConsumerConfig, cause error) nats.Header {
	hdr := nats.Header{}
	for k, vs := range src {
		for _, v := range vs {
			hdr.Add(k, v)
		}
	}
	hdr.Del(jetstream.MsgIDHeader)
	if id := src.Get("Chora-Event-Id"); id != "" {
		hdr.Set(jetstream.MsgIDHeader, id+".dlq")
	}
	hdr.Set("Chora-Dlq-Source-Subject", cfg.Subject)
	if cfg.Name != "" {
		hdr.Set("Chora-Dlq-Consumer", cfg.Name)
	}
	if cause != nil {
		hdr.Set("Chora-Dlq-Reason", cause.Error())
	}
	return hdr
}

// envelopeHeaders projects an envelope onto NATS headers so subscribers and
// observers can read tenant_id / traceparent / etc. without decoding the
// payload. The projection must round-trip: envelopeFromHeaders reconstructs
// every field set here, because the consumer validates the reconstructed
// envelope before handing it to a domain handler.
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
	set("Chora-Correlation-Id", env.CorrelationID)
	set("Chora-Causation-Id", env.CausationID)
	set("Chora-Imda-Dimension", env.ChoraImdaDimension)
	set("Chora-Imda-Lifecycle-Stage", env.ImdaLifecycleStage)
	if !env.OccurredAt.IsZero() {
		h.Set("Chora-Occurred-At", env.OccurredAt.UTC().Format(time.RFC3339Nano))
	}
	if !env.PublishedAt.IsZero() {
		h.Set("Chora-Published-At", env.PublishedAt.UTC().Format(time.RFC3339Nano))
	}
	return h
}

// envelopeFromHeaders reconstructs an envelope from NATS headers. Fields not
// present as headers fall back to the zero value; the reconstructed envelope
// is then validated, so a message whose mandatory envelope fields are missing
// or malformed is rejected (and dead-lettered) rather than handled blindly.
func envelopeFromHeaders(h nats.Header) (envelope.Envelope, error) {
	get := func(k string) string { return h.Get(k) }
	env := envelope.Envelope{
		EventID:            get("Chora-Event-Id"),
		IdempotencyKey:     get("Chora-Idempotency-Key"),
		TenantID:           get("Chora-Tenant-Id"),
		GCID:               get("Chora-Gcid"),
		SourceService:      get("Chora-Source-Service"),
		SourceProject:      get("Chora-Source-Project"),
		Traceparent:        get("Traceparent"),
		Tracestate:         get("Tracestate"),
		CorrelationID:      get("Chora-Correlation-Id"),
		CausationID:        get("Chora-Causation-Id"),
		ChoraImdaDimension: get("Chora-Imda-Dimension"),
		ImdaLifecycleStage: get("Chora-Imda-Lifecycle-Stage"),
	}
	if v := get("Chora-Schema-Version"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			env.SchemaVersion = int32(n)
		}
	}
	if v := get("Chora-Occurred-At"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return envelope.Envelope{}, fmt.Errorf("eventbus: parse occurred_at %q: %w", v, err)
		}
		env.OccurredAt = t
	}
	if v := get("Chora-Published-At"); v != "" {
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return envelope.Envelope{}, fmt.Errorf("eventbus: parse published_at %q: %w", v, err)
		}
		env.PublishedAt = t
	}
	return env, envelope.Validate(env)
}

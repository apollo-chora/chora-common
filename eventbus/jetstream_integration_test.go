package eventbus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/apollo-chora/chora-common/envelope"
)

// These tests exercise the real JetStream transport, not the InMemoryBus. The
// three P0/P1 bugs found in review (every message ACKed before the handler,
// DLQ loss on publish failure, backoff ignored) all lived in the JetStream
// implementation and were invisible to the in-memory-only tests. They run
// against an embedded nats-server so they need no Docker or external broker.
//
// The fixture reproduces the production topology: a CHORA_EVENTS stream
// (subjects chora.>) and a separate CHORA_DLQ stream (subjects _dlq.>). Testing
// against a single combined stream would hide exactly the DLQ-routing defects
// this suite exists to catch.

func runEmbeddedNATS(t *testing.T) (string, func()) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natsserver.RunServer(&opts)
	return s.ClientURL(), s.Shutdown
}

// newTestBus creates the two production-shaped streams and returns a bus bound
// to the events stream, plus the JetStream handle and both stream names.
func newTestBus(t *testing.T, url string) (*JetStreamBus, jetstream.JetStream, string, string) {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	events := "CHORA_EVENTS_TEST_" + suffix
	dlq := "CHORA_DLQ_TEST_" + suffix
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       events,
		Subjects:   []string{"chora.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create events stream: %v", err)
	}
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       dlq,
		Subjects:   []string{"_dlq.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create dlq stream: %v", err)
	}
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), events)
		_ = js.DeleteStream(context.Background(), dlq)
	})
	bus, err := NewJetStream(JetStreamConfig{URL: url, StreamName: events})
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus, js, events, dlq
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", d, what)
}

func streamMsgs(t *testing.T, js jetstream.JetStream, stream string) uint64 {
	t.Helper()
	s, err := js.Stream(context.Background(), stream)
	if err != nil {
		return 0
	}
	info, err := s.Info(context.Background())
	if err != nil {
		return 0
	}
	return info.State.Msgs
}

func dlqProbe(t *testing.T, js jetstream.JetStream, dlqStream, durable string) jetstream.Consumer {
	t.Helper()
	cons, err := js.CreateOrUpdateConsumer(context.Background(), dlqStream, jetstream.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "_dlq.>",
	})
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	return cons
}

// nextWithTimeout reads one message from a JetStream consumer iterator or fails
// the test after d.
func nextWithTimeout(t *testing.T, cons jetstream.Consumer, d time.Duration) jetstream.Msg {
	t.Helper()
	type result struct {
		msg jetstream.Msg
		err error
	}
	ch := make(chan result, 1)
	iter, err := cons.Messages()
	if err != nil {
		t.Fatalf("consumer messages: %v", err)
	}
	defer iter.Stop()
	go func() {
		m, err := iter.Next()
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("next: %v", r.err)
		}
		return r.msg
	case <-time.After(d):
		t.Fatalf("timed out after %s waiting for a message", d)
		return nil
	}
}

func TestJetStreamIntegration_EnvelopeRoundTrip(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, _, _, _ := newTestBus(t, url)

	const subject = "chora.observability.token_usage.recorded.v1"
	want := fullEnvelope()
	got := make(chan Message, 1)
	if err := bus.Subscribe(context.Background(), ConsumerConfig{
		Name:    "roundtrip",
		Subject: subject,
		Backoff: []time.Duration{50 * time.Millisecond},
	}, func(_ context.Context, m Message) error {
		got <- m
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), subject, want, []byte("payload")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case m := <-got:
		if string(m.Payload) != "payload" {
			t.Errorf("payload = %q", m.Payload)
		}
		if m.Subject != subject {
			t.Errorf("subject = %q, want %q", m.Subject, subject)
		}
		if !m.Envelope.OccurredAt.Equal(want.OccurredAt) || !m.Envelope.PublishedAt.Equal(want.PublishedAt) {
			t.Errorf("timestamps not preserved: occurred=%s published=%s", m.Envelope.OccurredAt, m.Envelope.PublishedAt)
		}
		gotEnv := m.Envelope
		gotEnv.OccurredAt, gotEnv.PublishedAt = time.Time{}, time.Time{}
		wantEnv := want
		wantEnv.OccurredAt, wantEnv.PublishedAt = time.Time{}, time.Time{}
		if gotEnv != wantEnv {
			t.Errorf("envelope mismatch:\n got %+v\nwant %+v", gotEnv, wantEnv)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never received the message (the every-message-dropped bug)")
	}
}

func TestJetStreamIntegration_MaxDeliverRoutesToDLQ(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, events, dlq := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	if err := bus.Subscribe(context.Background(), ConsumerConfig{
		Name:       "always-fails",
		Subject:    subject,
		MaxDeliver: 2,
		Backoff:    []time.Duration{50 * time.Millisecond, 50 * time.Millisecond},
	}, func(_ context.Context, _ Message) error {
		return fmt.Errorf("boom")
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), subject, fullEnvelope(), []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msg := nextWithTimeout(t, dlqProbe(t, js, dlq, "dlq-probe"), 15*time.Second)
	if got := msg.Headers().Get("Chora-Dlq-Source-Subject"); got != subject {
		t.Errorf("Chora-Dlq-Source-Subject = %q, want %q", got, subject)
	}
	if got := msg.Headers().Get("Chora-Dlq-Consumer"); got != "always-fails" {
		t.Errorf("Chora-Dlq-Consumer = %q, want always-fails", got)
	}
	if got := msg.Headers().Get("Chora-Dlq-Delivery-Count"); got != "2" {
		t.Errorf("Chora-Dlq-Delivery-Count = %q, want 2", got)
	}
	// The handler's own error must be preserved for the operator.
	if got := msg.Headers().Get("Chora-Dlq-Reason"); got != "boom" {
		t.Errorf("Chora-Dlq-Reason = %q, want %q (handler error not preserved)", got, "boom")
	}
	// Exactly one DLQ record, and the original was settled in the events stream.
	waitFor(t, 5*time.Second, "exactly one DLQ record", func() bool {
		return streamMsgs(t, js, dlq) == 1
	})
	if n := streamMsgs(t, js, events); n != 1 {
		t.Errorf("events stream has %d messages, want 1 (original)", n)
	}
}

func TestJetStreamIntegration_UnparseableEnvelopeGoesToDLQ(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, _, dlq := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	handled := make(chan struct{}, 1)
	if err := bus.Subscribe(context.Background(), ConsumerConfig{
		Name:    "poison",
		Subject: subject,
		Backoff: []time.Duration{50 * time.Millisecond},
	}, func(_ context.Context, _ Message) error {
		handled <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Publish a raw message whose envelope headers omit occurred_at, so
	// envelopeFromHeaders cannot reconstruct a valid envelope.
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	raw := &nats.Msg{Subject: subject, Data: []byte("poison"), Header: nats.Header{}}
	raw.Header.Set("Chora-Event-Id", "evt-poison")
	if err := nc.PublishMsg(raw); err != nil {
		t.Fatalf("publish raw: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	msg := nextWithTimeout(t, dlqProbe(t, js, dlq, "dlq-probe-poison"), 15*time.Second)
	if got := msg.Headers().Get("Chora-Dlq-Source-Subject"); got != subject {
		t.Errorf("Chora-Dlq-Source-Subject = %q, want %q", got, subject)
	}
	if got := msg.Headers().Get("Chora-Dlq-Reason"); got == "" {
		t.Error("Chora-Dlq-Reason should record why the message was dead-lettered")
	}
	select {
	case <-handled:
		t.Error("an unparseable envelope must never reach the domain handler")
	case <-time.After(500 * time.Millisecond):
	}
}

// TestJetStreamIntegration_DLQIdentityIsPerConsumer proves the dead-letter
// record identity is (source event, failed consumer): a settlement retry of one
// pair deduplicates, while the same event failing on a different consumer is
// preserved as a second record.
func TestJetStreamIntegration_DLQIdentityIsPerConsumer(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	_, js, _, dlq := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	env := fullEnvelope()
	src := envelopeHeaders(env)
	src.Set(jetstream.MsgIDHeader, env.EventID)

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()
	publish := func(consumer string) {
		cfg := ConsumerConfig{Name: consumer, Subject: subject}
		m := &nats.Msg{Subject: DLQSubject(subject), Data: []byte("poison"), Header: dlqHeaders(src, cfg, 3, errors.New("boom"))}
		if err := nc.PublishMsg(m); err != nil {
			t.Fatalf("publish dlq: %v", err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
	}

	publish("consumer-a")
	publish("consumer-a") // settlement retry of the same (event, consumer)
	publish("consumer-b") // same event, different consumer

	waitFor(t, 5*time.Second, "two DLQ records (one per consumer)", func() bool {
		return streamMsgs(t, js, dlq) == 2
	})
}

func TestJetStreamIntegration_DeduplicatesSameEventID(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, events, _ := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	env := fullEnvelope()
	for i := 0; i < 2; i++ {
		if err := bus.Publish(context.Background(), subject, env, []byte("same")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, 5*time.Second, "duplicate suppression to leave one message", func() bool {
		return streamMsgs(t, js, events) == 1
	})
}

// TestJetStreamIntegration_SameIdempotencyKeyDifferentEventIDs locks in the
// decision that the broker dedup key is EventID, never IdempotencyKey: two
// distinct events that intentionally share a business idempotency key must
// both be stored.
func TestJetStreamIntegration_SameIdempotencyKeyDifferentEventIDs(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, events, _ := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	first := fullEnvelope()
	second := fullEnvelope()
	second.EventID = "0199a0f3-1c2d-7a4b-9f10-000000000099"
	second.IdempotencyKey = first.IdempotencyKey // deliberately shared

	for _, env := range []envelope.Envelope{first, second} {
		if err := bus.Publish(context.Background(), subject, env, []byte("x")); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	waitFor(t, 5*time.Second, "both distinct events stored", func() bool {
		return streamMsgs(t, js, events) == 2
	})
}

package eventbus

import (
	"context"
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

func runEmbeddedNATS(t *testing.T) (string, func()) {
	t.Helper()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.StoreDir = t.TempDir()
	s := natsserver.RunServer(&opts)
	return s.ClientURL(), s.Shutdown
}

// newTestBus creates a unique stream that captures both domain events and DLQ
// subjects, and returns a bus bound to it.
func newTestBus(t *testing.T, url string) (*JetStreamBus, jetstream.JetStream, string) {
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
	name := fmt.Sprintf("CHORA_EVENTS_TEST_%d", time.Now().UnixNano())
	if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:       name,
		Subjects:   []string{"chora.>", "_dlq.>"},
		Storage:    jetstream.FileStorage,
		Duplicates: 10 * time.Minute,
	}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	t.Cleanup(func() {
		_ = js.DeleteStream(context.Background(), name)
	})
	bus, err := NewJetStream(JetStreamConfig{URL: url, StreamName: name})
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	return bus, js, name
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
	bus, _, _ := newTestBus(t, url)

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
	bus, js, stream := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	attempts := make(chan struct{}, 32)
	if err := bus.Subscribe(context.Background(), ConsumerConfig{
		Name:       "always-fails",
		Subject:    subject,
		MaxDeliver: 2,
		Backoff:    []time.Duration{50 * time.Millisecond, 50 * time.Millisecond},
	}, func(_ context.Context, _ Message) error {
		attempts <- struct{}{}
		return fmt.Errorf("boom")
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := bus.Publish(context.Background(), subject, fullEnvelope(), []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	dlqCons, err := js.CreateOrUpdateConsumer(context.Background(), stream, jetstream.ConsumerConfig{
		Durable:       "dlq-probe",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "_dlq.>",
	})
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	msg := nextWithTimeout(t, dlqCons, 15*time.Second)
	if got := msg.Headers().Get("Chora-Dlq-Source-Subject"); got != subject {
		t.Errorf("Chora-Dlq-Source-Subject = %q, want %q", got, subject)
	}
	if got := msg.Headers().Get("Chora-Dlq-Consumer"); got != "always-fails" {
		t.Errorf("Chora-Dlq-Consumer = %q, want always-fails", got)
	}
	if got := msg.Headers().Get("Chora-Dlq-Delivery-Count"); got != "2" {
		t.Errorf("Chora-Dlq-Delivery-Count = %q, want 2", got)
	}
	// Exactly one DLQ record, not one per failed delivery.
	waitFor(t, 2*time.Second, "exactly one DLQ record", func() bool {
		info, err := js.Stream(context.Background(), stream)
		if err != nil {
			return false
		}
		st, err := info.Info(context.Background())
		if err != nil {
			return false
		}
		return st.State.Msgs == 2 // 1 original + 1 DLQ
	})
}

func TestJetStreamIntegration_UnparseableEnvelopeGoesToDLQ(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, stream := newTestBus(t, url)

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

	dlqCons, err := js.CreateOrUpdateConsumer(context.Background(), stream, jetstream.ConsumerConfig{
		Durable:       "dlq-probe-poison",
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: "_dlq.>",
	})
	if err != nil {
		t.Fatalf("dlq consumer: %v", err)
	}
	msg := nextWithTimeout(t, dlqCons, 15*time.Second)
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

func TestJetStreamIntegration_DeduplicatesSameEventID(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, stream := newTestBus(t, url)

	const subject = "chora.observability.thing.happened.v1"
	env := fullEnvelope()
	for i := 0; i < 2; i++ {
		if err := bus.Publish(context.Background(), subject, env, []byte("same")); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	waitFor(t, 5*time.Second, "duplicate suppression to leave one message", func() bool {
		info, err := js.Stream(context.Background(), stream)
		if err != nil {
			return false
		}
		st, err := info.Info(context.Background())
		if err != nil {
			return false
		}
		return st.State.Msgs == 1
	})
}

// TestJetStreamIntegration_SameIdempotencyKeyDifferentEventIDs locks in the
// decision that the broker dedup key is EventID, never IdempotencyKey: two
// distinct events that intentionally share a business idempotency key must
// both be stored.
func TestJetStreamIntegration_SameIdempotencyKeyDifferentEventIDs(t *testing.T) {
	url, shutdown := runEmbeddedNATS(t)
	defer shutdown()
	bus, js, stream := newTestBus(t, url)

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
		info, err := js.Stream(context.Background(), stream)
		if err != nil {
			return false
		}
		st, err := info.Info(context.Background())
		if err != nil {
			return false
		}
		return st.State.Msgs == 2
	})
}

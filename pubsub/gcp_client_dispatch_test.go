package pubsub

// D5 (AUTH Phase-A debt pass): the GCPClient receive loop nacked handler
// errors WITHOUT logging them, so subscriber-side failures were invisible.
// dispatchReceivedMessage now logs at ERROR before the defensive Nack. These
// internal tests exercise that helper directly (the production
// SubscriptionReceive wraps the real *pubsub.Client, which has no unit seam).

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
)

func TestDispatchReceivedMessage_LogsAndNacksOnHandlerError(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	defer slog.SetDefault(prev)

	var ackCalled, nackCalled int32
	cm := CloudMessage{
		ID: "m-42",
		Attributes: map[string]string{
			"tenant_id": "tenant-xyz",
			"topic":     "chora.identity.user.created.v1",
		},
		Ack:  func() { atomic.AddInt32(&ackCalled, 1) },
		Nack: func() { atomic.AddInt32(&nackCalled, 1) },
	}
	handler := func(context.Context, CloudMessage) error {
		return errors.New("boom-handler")
	}

	dispatchReceivedMessage(context.Background(), "test-sub", cm, handler)

	if got := atomic.LoadInt32(&nackCalled); got != 1 {
		t.Fatalf("nack called=%d want 1", got)
	}
	if got := atomic.LoadInt32(&ackCalled); got != 0 {
		t.Fatalf("ack called=%d want 0 (handler owns Ack)", got)
	}
	logged := buf.String()
	// The dispatch log is the SOLE subscriber-side failure log; it MUST carry
	// topic + subscription + message_id + tenant_id + error so no signal is
	// lost now that CloudSubscriber no longer logs at its own layer.
	for _, want := range []string{"test-sub", "m-42", "tenant-xyz", "boom-handler", "chora.identity.user.created.v1"} {
		if !strings.Contains(logged, want) {
			t.Errorf("log missing %q\n--- log ---\n%s", want, logged)
		}
	}
	// Silent nacks were the bug — the line MUST be ERROR level.
	if !strings.Contains(logged, `"level":"ERROR"`) {
		t.Errorf("expected ERROR-level log, got: %s", logged)
	}
}

func TestDispatchReceivedMessage_SuccessNoLogNoNack(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(prev)

	var nackCalled int32
	cm := CloudMessage{
		ID:   "m-ok",
		Ack:  func() {}, // handler owns Ack; helper must not touch it
		Nack: func() { atomic.AddInt32(&nackCalled, 1) },
	}
	handler := func(context.Context, CloudMessage) error { return nil }

	dispatchReceivedMessage(context.Background(), "test-sub", cm, handler)

	if got := atomic.LoadInt32(&nackCalled); got != 0 {
		t.Fatalf("nack called=%d want 0 on success", got)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no log on success, got: %s", buf.String())
	}
}

func TestDispatchReceivedMessage_NilAttributesNoPanic(t *testing.T) {
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(io.Discard, nil)))
	defer slog.SetDefault(prev)

	var nackCalled int32
	cm := CloudMessage{ID: "m-nilattrs", Nack: func() { atomic.AddInt32(&nackCalled, 1) }}
	dispatchReceivedMessage(context.Background(), "s", cm, func(context.Context, CloudMessage) error {
		return errors.New("e")
	})
	if got := atomic.LoadInt32(&nackCalled); got != 1 {
		t.Fatalf("nack=%d want 1 (nil Attributes must not panic)", got)
	}
}

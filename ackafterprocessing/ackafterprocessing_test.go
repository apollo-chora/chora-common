// Tests for ack-after-processing subscriber semantics.
//
// W3 foundation Phase 4.5 (2026-05-12). Mirrors the Python
// AckAfterProcessingSubscriber + TransientError pattern in
// services/chora-closure-orchestrator/src/chora_closure_orchestrator/
// adapter/pubsub/subscriber.py so every Chora Go subscriber gets
// identical Ack/Nack + DLQ semantics regardless of runtime.
//
// Contract:
//
//	handler returns nil           → Ack
//	handler returns TransientError → Nack (transient — Pub/Sub redelivers
//	                                with exponential backoff)
//	handler returns any other err  → Nack (terminal — Pub/Sub redelivers
//	                                until subscription.max_delivery_attempts
//	                                then routes to DLQ)
//
// The wrapper distinguishes the two failure modes only in the log
// record; both result in the same broker behaviour (Nack). On-call uses
// the log distinction to triage whether a runbook should treat the
// failure as "wait for next redelivery" vs "investigate persistent error".

package ackafterprocessing_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	aap "github.com/apollo-chora/chora-common/ackafterprocessing"
)

// fakeMessage is a test double for ackafterprocessing.Message.
type fakeMessage struct {
	attrs map[string]string
	data  []byte
	ackN  int32
	nackN int32
}

func (f *fakeMessage) Attributes() map[string]string { return f.attrs }
func (f *fakeMessage) Data() []byte                  { return f.data }
func (f *fakeMessage) Ack()                          { atomic.AddInt32(&f.ackN, 1) }
func (f *fakeMessage) Nack()                         { atomic.AddInt32(&f.nackN, 1) }

func (f *fakeMessage) AckCount() int32  { return atomic.LoadInt32(&f.ackN) }
func (f *fakeMessage) NackCount() int32 { return atomic.LoadInt32(&f.nackN) }

func newMsg(eventID, tenantID, sagaID string, payload string) *fakeMessage {
	return &fakeMessage{
		attrs: map[string]string{
			"event_id":  eventID,
			"tenant_id": tenantID,
			"saga_id":   sagaID,
		},
		data: []byte(payload),
	}
}

// ----------------------------------------------------------------------------
// Success → Ack
// ----------------------------------------------------------------------------

func TestProcessOne_AcksOnHandlerSuccess(t *testing.T) {
	t.Parallel()
	called := 0
	sub := aap.NewSubscriber(func(ctx context.Context, attrs map[string]string, data []byte) error {
		called++
		return nil
	})
	msg := newMsg("evt-1", "tenant-A", "saga-1", "hello")
	if err := sub.ProcessOne(context.Background(), msg); err != nil {
		t.Fatalf("ProcessOne returned err: %v", err)
	}
	if called != 1 {
		t.Errorf("handler called=%d, want 1", called)
	}
	if got := msg.AckCount(); got != 1 {
		t.Errorf("Ack count=%d, want 1", got)
	}
	if got := msg.NackCount(); got != 0 {
		t.Errorf("Nack count=%d, want 0", got)
	}
}

// ----------------------------------------------------------------------------
// TransientError → Nack (transient classification)
// ----------------------------------------------------------------------------

func TestProcessOne_NacksOnTransientError(t *testing.T) {
	t.Parallel()
	sub := aap.NewSubscriber(func(ctx context.Context, attrs map[string]string, data []byte) error {
		return &aap.TransientError{Err: errors.New("upstream slow")}
	})
	msg := newMsg("evt-2", "tenant-A", "saga-2", "x")
	if err := sub.ProcessOne(context.Background(), msg); err == nil {
		t.Fatal("expected ProcessOne to return non-nil error on handler failure")
	}
	if got := msg.AckCount(); got != 0 {
		t.Errorf("Ack count=%d, want 0", got)
	}
	if got := msg.NackCount(); got != 1 {
		t.Errorf("Nack count=%d, want 1", got)
	}
}

// IsTransient correctly identifies the sentinel + a wrapped TransientError.
func TestIsTransient_TrueOnSentinelAndWrapped(t *testing.T) {
	t.Parallel()
	t1 := &aap.TransientError{Err: errors.New("upstream slow")}
	if !aap.IsTransient(t1) {
		t.Error("IsTransient(TransientError) should be true")
	}
	wrapped := fmt.Errorf("retry: %w", t1)
	if !aap.IsTransient(wrapped) {
		t.Error("IsTransient(wrapped TransientError) should be true")
	}
}

func TestIsTransient_FalseOnTerminalAndNil(t *testing.T) {
	t.Parallel()
	if aap.IsTransient(errors.New("plain")) {
		t.Error("IsTransient(plain error) should be false")
	}
	if aap.IsTransient(nil) {
		t.Error("IsTransient(nil) should be false")
	}
}

// ----------------------------------------------------------------------------
// Terminal error → Nack (terminal classification — leads to DLQ after max
// delivery attempts)
// ----------------------------------------------------------------------------

func TestProcessOne_NacksOnTerminalError(t *testing.T) {
	t.Parallel()
	sub := aap.NewSubscriber(func(ctx context.Context, attrs map[string]string, data []byte) error {
		return errors.New("permanent bug")
	})
	msg := newMsg("evt-3", "tenant-B", "saga-3", "x")
	if err := sub.ProcessOne(context.Background(), msg); err == nil {
		t.Fatal("expected ProcessOne to surface handler error")
	}
	if got := msg.AckCount(); got != 0 {
		t.Errorf("Ack count=%d, want 0", got)
	}
	if got := msg.NackCount(); got != 1 {
		t.Errorf("Nack count=%d, want 1", got)
	}
}

// ----------------------------------------------------------------------------
// Handler panic → Nack (recovers panic, classifies as terminal)
// ----------------------------------------------------------------------------

func TestProcessOne_NacksOnHandlerPanic(t *testing.T) {
	t.Parallel()
	sub := aap.NewSubscriber(func(ctx context.Context, attrs map[string]string, data []byte) error {
		panic("boom")
	})
	msg := newMsg("evt-4", "tenant-A", "saga-4", "x")
	if err := sub.ProcessOne(context.Background(), msg); err == nil {
		t.Fatal("expected ProcessOne to surface panic-recovery error")
	}
	if got := msg.NackCount(); got != 1 {
		t.Errorf("Nack count=%d, want 1", got)
	}
	if got := msg.AckCount(); got != 0 {
		t.Errorf("Ack count=%d, want 0", got)
	}
}

// ----------------------------------------------------------------------------
// Attributes + payload pass through to the handler
// ----------------------------------------------------------------------------

func TestProcessOne_PassesAttrsAndDataToHandler(t *testing.T) {
	t.Parallel()
	var seenAttrs map[string]string
	var seenData []byte
	sub := aap.NewSubscriber(func(ctx context.Context, attrs map[string]string, data []byte) error {
		seenAttrs = attrs
		seenData = data
		return nil
	})
	msg := newMsg("evt-5", "tenant-X", "saga-5", "payload-bytes")
	if err := sub.ProcessOne(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if seenAttrs["event_id"] != "evt-5" || seenAttrs["tenant_id"] != "tenant-X" {
		t.Errorf("attrs not propagated: %v", seenAttrs)
	}
	if string(seenData) != "payload-bytes" {
		t.Errorf("data not propagated: %s", string(seenData))
	}
}

// ----------------------------------------------------------------------------
// Nil handler → constructor returns nil; ProcessOne returns error without
// touching the message. Defensive — wrong usage shouldn't masquerade as a
// silent ACK.
// ----------------------------------------------------------------------------

func TestNewSubscriber_PanicsOnNilHandler(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on nil handler")
		}
	}()
	_ = aap.NewSubscriber(nil)
}

// ----------------------------------------------------------------------------
// Context cancellation BEFORE handler invocation → Nack (the message will
// be redelivered to a still-running pod).
// ----------------------------------------------------------------------------

func TestProcessOne_NacksWhenContextCancelledPreHandler(t *testing.T) {
	t.Parallel()
	sub := aap.NewSubscriber(func(ctx context.Context, attrs map[string]string, data []byte) error {
		t.Error("handler should not be invoked when context already cancelled")
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	msg := newMsg("evt-7", "tenant-A", "saga-7", "x")
	if err := sub.ProcessOne(ctx, msg); err == nil {
		t.Fatal("expected ProcessOne to surface context error")
	}
	if got := msg.NackCount(); got != 1 {
		t.Errorf("Nack count=%d, want 1", got)
	}
}

// ----------------------------------------------------------------------------
// TransientError method coverage — Error() shape + Unwrap()
// ----------------------------------------------------------------------------

func TestTransientError_ErrorOnNilAndEmptyInner(t *testing.T) {
	t.Parallel()
	var t1 *aap.TransientError
	if got := t1.Error(); got != "transient error" {
		t.Errorf("nil TransientError Error() = %q, want %q", got, "transient error")
	}
	t2 := &aap.TransientError{}
	if got := t2.Error(); got != "transient error" {
		t.Errorf("empty TransientError Error() = %q, want %q", got, "transient error")
	}
	t3 := &aap.TransientError{Err: errors.New("inner")}
	if got := t3.Error(); got != "transient: inner" {
		t.Errorf("wrapped TransientError Error() = %q, want %q", got, "transient: inner")
	}
}

func TestTransientError_Unwrap(t *testing.T) {
	t.Parallel()
	inner := errors.New("upstream")
	te := &aap.TransientError{Err: inner}
	if got := te.Unwrap(); got != inner {
		t.Errorf("Unwrap() = %v, want %v", got, inner)
	}
	var nilTE *aap.TransientError
	if got := nilTE.Unwrap(); got != nil {
		t.Errorf("nil Unwrap() = %v, want nil", got)
	}
	if !errors.Is(te, inner) {
		t.Error("errors.Is should reach the inner via Unwrap()")
	}
}

// ----------------------------------------------------------------------------
// WithLogger option — uses the supplied slog handler instead of the default
// ----------------------------------------------------------------------------

func TestWithLogger_RedirectsLogOutput(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)

	sub := aap.NewSubscriber(
		func(ctx context.Context, attrs map[string]string, data []byte) error {
			return errors.New("terminal-bug")
		},
		aap.WithLogger(logger),
	)
	msg := newMsg("evt-log-1", "tenant-Log", "saga-log-1", "x")
	_ = sub.ProcessOne(context.Background(), msg)

	out := buf.String()
	if !strings.Contains(out, "subscriber_handler_failed_nack") {
		t.Errorf("custom logger did not receive the expected record: %s", out)
	}
	if !strings.Contains(out, "tenant-Log") {
		t.Errorf("custom logger did not see tenant_id attribute: %s", out)
	}
}

// Nil logger handed to WithLogger is a no-op (defensive — operator slipped
// a nil but the wrapper still works against slog.Default()).
func TestWithLogger_NilNoOps(t *testing.T) {
	t.Parallel()
	sub := aap.NewSubscriber(
		func(ctx context.Context, attrs map[string]string, data []byte) error {
			return nil
		},
		aap.WithLogger(nil),
	)
	msg := newMsg("evt-log-2", "tenant-A", "saga-log-2", "x")
	if err := sub.ProcessOne(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got := msg.AckCount(); got != 1 {
		t.Errorf("Ack count=%d, want 1", got)
	}
}

// ----------------------------------------------------------------------------
// Truncation — error messages longer than 300 chars are clipped to keep
// the log line bounded. Verify the log line doesn't contain the trailing
// portion of an over-long error.
// ----------------------------------------------------------------------------

func TestProcessOne_TruncatesLongErrorMessageInLog(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("X", 500) + "_TAIL"
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(handler)
	sub := aap.NewSubscriber(
		func(ctx context.Context, attrs map[string]string, data []byte) error {
			return errors.New(long)
		},
		aap.WithLogger(logger),
	)
	msg := newMsg("evt-trunc", "tenant-A", "saga-trunc", "x")
	_ = sub.ProcessOne(context.Background(), msg)
	out := buf.String()
	if strings.Contains(out, "_TAIL") {
		t.Errorf("log line contained the over-long tail (not truncated): %s", out[:200])
	}
}

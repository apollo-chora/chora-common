// Package ackafterprocessing wraps a user handler with ack-after-processing
// semantics for broker subscribers. It is the canonical Go
// equivalent of the Python AckAfterProcessingSubscriber in
// services/chora-closure-orchestrator/src/chora_closure_orchestrator/
// adapter/pubsub/subscriber.py — added by W3 foundation Phase 4.5
// (2026-05-12) so every Chora Go subscriber gets identical Ack/Nack +
// DLQ semantics regardless of which agent runtime the subscriber lives
// inside (legacy Go executor / ADK Go agent / future runtimes).
//
// Contract:
//
//	handler returns nil            → Ack
//	handler returns *TransientError → Nack (transient — the broker redelivers
//	                                 with exponential backoff)
//	handler returns any other err   → Nack (terminal — the broker redelivers
//	                                 until subscription.max_delivery_attempts
//	                                 then routes to DLQ)
//	handler panics                 → Nack (terminal — recovered + classified
//	                                 as a terminal handler bug)
//
// The wrapper distinguishes transient vs terminal in the log record so
// on-call can grep the difference. Both classifications result in
// Nack (the broker decides redeliver-or-DLQ via subscription policy).
//
// Per feedback_d6_resilience_first_class: durable consumer-side
// processing is non-negotiable. The wrapper guarantees a message is
// never Ack'd before downstream processing completes.
package ackafterprocessing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// HandlerFunc processes a single broker message. Receives the
// envelope attributes (which carry tenant_id, event_id, saga_id, etc.)
// and the binary payload. Return nil for success (Ack), a
// *TransientError for transient failure (Nack + transient log), or any
// other error for terminal failure (Nack + handler-failed log).
type HandlerFunc func(ctx context.Context, attrs map[string]string, data []byte) error

// Message is the broker-agnostic shape a subscriber sees. The real
// adapter wraps the broker's message type (e.g. eventbus.Message); tests
// wire a stub. Implementations MUST be safe to call Ack/Nack exactly
// once; subsequent calls are no-ops (the broker client enforces this).
type Message interface {
	Attributes() map[string]string
	Data() []byte
	Ack()
	Nack()
}

// TransientError signals a transient failure that should result in Nack
// + redelivery. Operators read the log line and treat the failure as
// "wait for next redelivery" rather than "investigate persistent
// error". Wrap the underlying error so callers can errors.As(...) to
// extract it.
type TransientError struct {
	Err error
}

// Error implements error.
func (t *TransientError) Error() string {
	if t == nil || t.Err == nil {
		return "transient error"
	}
	return "transient: " + t.Err.Error()
}

// Unwrap permits errors.Is / errors.As traversal.
func (t *TransientError) Unwrap() error {
	if t == nil {
		return nil
	}
	return t.Err
}

// IsTransient reports whether err is (or wraps) a *TransientError.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var te *TransientError
	return errors.As(err, &te)
}

// Subscriber wraps a HandlerFunc with ack-after-processing semantics.
// The wrapper logs distinctive records on success / transient / terminal
// / panic outcomes so on-call can grep the log stream for triage. The
// log subsystem is log/slog (the canonical Go structured logger);
// callers can swap to chora-observability slog handler at boot.
type Subscriber struct {
	handler HandlerFunc
	logger  *slog.Logger
}

// NewSubscriber wraps the supplied handler. Panics on nil handler —
// passing nil is a programming bug, not a runtime failure mode.
func NewSubscriber(handler HandlerFunc, opts ...Option) *Subscriber {
	if handler == nil {
		panic("ackafterprocessing: nil handler")
	}
	s := &Subscriber{handler: handler, logger: slog.Default()}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Option tunes a Subscriber at construction time.
type Option func(*Subscriber)

// WithLogger overrides the default slog logger.
func WithLogger(l *slog.Logger) Option {
	return func(s *Subscriber) {
		if l != nil {
			s.logger = l
		}
	}
}

// ProcessOne handles exactly one message. The return value mirrors the
// handler outcome: nil on success, the handler error (wrapped or
// classified) on failure. Production wires this into a streaming-pull
// callback; tests call it directly.
//
// Context cancellation BEFORE invoking the handler yields Nack — the
// message will be redelivered to a still-running pod.
func (s *Subscriber) ProcessOne(ctx context.Context, msg Message) (rerr error) {
	attrs := msg.Attributes()

	// Pre-handler context check — fail fast if the pod is shutting down.
	if err := ctx.Err(); err != nil {
		s.logger.Info(
			"subscriber_ctx_cancelled_nack",
			slog.String("tenant_id", attrs["tenant_id"]),
			slog.String("event_id", attrs["event_id"]),
			slog.String("saga_id", attrs["saga_id"]),
			slog.String("error", err.Error()),
		)
		msg.Nack()
		return fmt.Errorf("ackafterprocessing: context cancelled: %w", err)
	}

	// Recover panics in the handler — classified as terminal (handler bug).
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error(
				"subscriber_handler_panic_nack",
				slog.String("tenant_id", attrs["tenant_id"]),
				slog.String("event_id", attrs["event_id"]),
				slog.String("saga_id", attrs["saga_id"]),
				slog.Any("panic", r),
			)
			msg.Nack()
			rerr = fmt.Errorf("ackafterprocessing: handler panicked: %v", r)
		}
	}()

	err := s.handler(ctx, attrs, msg.Data())
	if err == nil {
		msg.Ack()
		return nil
	}

	if IsTransient(err) {
		s.logger.Info(
			"subscriber_transient_nack",
			slog.String("tenant_id", attrs["tenant_id"]),
			slog.String("event_id", attrs["event_id"]),
			slog.String("saga_id", attrs["saga_id"]),
			slog.String("error", trunc(err.Error(), 300)),
		)
		msg.Nack()
		return err
	}

	s.logger.Warn(
		"subscriber_handler_failed_nack",
		slog.String("tenant_id", attrs["tenant_id"]),
		slog.String("event_id", attrs["event_id"]),
		slog.String("saga_id", attrs["saga_id"]),
		slog.String("error", trunc(err.Error(), 300)),
	)
	msg.Nack()
	return err
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

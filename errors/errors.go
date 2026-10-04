// Package errors provides a typed error helper for Chora services.
//
// Every error returned at a service boundary carries:
//   - a machine code (e.g. "CHORA_NOT_FOUND")
//   - a human reason (e.g. "atom missing")
//   - an optional trace ID for correlation with Cloud Trace spans
//
// Per CLAUDE.md §6 (OTLP everywhere), the trace ID enables operators to
// follow a customer-reported error all the way to the originating span.
package errors

import (
	stderrors "errors"
	"fmt"
)

// Error is the typed Chora error. It implements the standard error interface
// and supports errors.Is / errors.As / errors.Unwrap.
type Error struct {
	code    string
	reason  string
	traceID string
	cause   error
}

// New constructs a Chora error with no underlying cause.
func New(code, reason string) *Error {
	return &Error{code: code, reason: reason}
}

// Wrap decorates an existing error with a Chora code + reason. Returns nil
// if cause is nil — convenient for early-return idioms.
func Wrap(cause error, code, reason string) *Error {
	if cause == nil {
		return nil
	}
	return &Error{code: code, reason: reason, cause: cause}
}

// Code returns the machine-readable error code.
func (e *Error) Code() string { return e.code }

// Reason returns the human-readable reason.
func (e *Error) Reason() string { return e.reason }

// TraceID returns the W3C trace ID associated with this error, if any.
func (e *Error) TraceID() string { return e.traceID }

// WithTraceID returns a copy of the error with the trace ID attached.
func (e *Error) WithTraceID(traceID string) *Error {
	return &Error{code: e.code, reason: e.reason, traceID: traceID, cause: e.cause}
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.code, e.reason, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.code, e.reason)
}

// Unwrap returns the underlying cause for errors.Is / errors.As traversal.
func (e *Error) Unwrap() error { return e.cause }

// Is reports whether target is an Error with the same code. This makes
// errors.Is(e, &Error{code: "X"}) work.
func (e *Error) Is(target error) bool {
	var t *Error
	if !stderrors.As(target, &t) {
		return false
	}
	return e.code == t.code
}

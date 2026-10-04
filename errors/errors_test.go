package errors_test

import (
	"errors"
	"testing"

	choraerr "github.com/apollo-chora/chora-common/errors"
)

func TestNew_BuildsTypedError(t *testing.T) {
	e := choraerr.New("CHORA_NOT_FOUND", "atom missing")
	if e == nil {
		t.Fatal("expected non-nil error")
	}
	if e.Code() != "CHORA_NOT_FOUND" {
		t.Errorf("Code=%q want CHORA_NOT_FOUND", e.Code())
	}
	if e.Reason() != "atom missing" {
		t.Errorf("Reason=%q want %q", e.Reason(), "atom missing")
	}
	if e.Error() == "" {
		t.Error("Error() returned empty string")
	}
}

func TestWrap_PreservesUnderlying(t *testing.T) {
	root := errors.New("root cause")
	wrapped := choraerr.Wrap(root, "CHORA_INTERNAL", "lookup failed")
	if wrapped == nil {
		t.Fatal("expected non-nil wrapped error")
	}
	if !errors.Is(wrapped, root) {
		t.Error("errors.Is should match wrapped underlying error")
	}
	if wrapped.Code() != "CHORA_INTERNAL" {
		t.Errorf("Code=%q want CHORA_INTERNAL", wrapped.Code())
	}
}

func TestWrap_NilReturnsNil(t *testing.T) {
	got := choraerr.Wrap(nil, "ANY", "any")
	if got != nil {
		t.Errorf("Wrap(nil) should return nil, got %v", got)
	}
}

func TestError_WithTraceID(t *testing.T) {
	e := choraerr.New("CODE_X", "reason X").WithTraceID("00-aabb-ccdd-01")
	if e.TraceID() != "00-aabb-ccdd-01" {
		t.Errorf("TraceID=%q want 00-aabb-ccdd-01", e.TraceID())
	}
}

func TestError_AsTypedFromGenericError(t *testing.T) {
	var target *choraerr.Error
	root := errors.New("plain")
	wrapped := choraerr.Wrap(root, "CODE_Y", "reason Y")
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should match Error type")
	}
	if target.Code() != "CODE_Y" {
		t.Errorf("Unwrap target Code=%q want CODE_Y", target.Code())
	}
}

func TestError_FormatIncludesCodeAndReason(t *testing.T) {
	e := choraerr.New("CHORA_TEST", "the test reason")
	msg := e.Error()
	if !contains(msg, "CHORA_TEST") {
		t.Errorf("Error msg missing code: %q", msg)
	}
	if !contains(msg, "the test reason") {
		t.Errorf("Error msg missing reason: %q", msg)
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

package agentengine

import (
	"errors"
	"fmt"
	"testing"
)

func TestEngineError_FormatsWithStatusAndExcerpt(t *testing.T) {
	e := &EngineError{Sentinel: ErrEngineUnavailable, HTTPStatus: 503, BodyExcerpt: "cold"}
	got := e.Error()
	want := "agentengine: engine unavailable (http 503): cold"
	if got != want {
		t.Errorf("Error() = %q; want %q", got, want)
	}
}

func TestEngineError_FormatsWithoutExcerpt(t *testing.T) {
	e := &EngineError{Sentinel: ErrEngineUnavailable, HTTPStatus: 502}
	got := e.Error()
	want := "agentengine: engine unavailable (http 502)"
	if got != want {
		t.Errorf("Error() = %q; want %q", got, want)
	}
}

func TestEngineError_NilSafe(t *testing.T) {
	var e *EngineError
	if got := e.Error(); got != "<nil>" {
		t.Errorf("nil Error() = %q; want <nil>", got)
	}
}

func TestEngineError_UnwrapPreservesSentinel(t *testing.T) {
	e := &EngineError{Sentinel: ErrEngineUnavailable, HTTPStatus: 503}
	wrapped := fmt.Errorf("wrapper: %w", e)
	if !errors.Is(wrapped, ErrEngineUnavailable) {
		t.Error("errors.Is failed to chain through EngineError")
	}
}

func TestIsNotConfigured_Matches(t *testing.T) {
	if !IsNotConfigured(ErrEngineNotConfigured) {
		t.Error("IsNotConfigured(ErrEngineNotConfigured) = false; want true")
	}
	wrapped := fmt.Errorf("ctx: %w", ErrEngineNotConfigured)
	if !IsNotConfigured(wrapped) {
		t.Error("IsNotConfigured failed to chain through wrap")
	}
}

func TestIsNotConfigured_DoesNotMatchOthers(t *testing.T) {
	if IsNotConfigured(ErrEngineUnavailable) {
		t.Error("IsNotConfigured matched ErrEngineUnavailable (false positive)")
	}
}

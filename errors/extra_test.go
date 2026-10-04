// Package errors — supplementary typed-error tests.
package errors_test

import (
	stderrors "errors"
	"strings"
	"testing"

	choraerrors "github.com/apollo-chora/chora-common/errors"
)

func TestError_ErrorIncludesCause(t *testing.T) {
	t.Parallel()
	cause := stderrors.New("boom")
	e := choraerrors.Wrap(cause, "CHORA_PAYMENTS", "tx failed")
	if e == nil {
		t.Fatal("Wrap returned nil for non-nil cause")
	}
	msg := e.Error()
	if !strings.Contains(msg, "CHORA_PAYMENTS") || !strings.Contains(msg, "tx failed") || !strings.Contains(msg, "boom") {
		t.Errorf("Error() = %q, want code/reason/cause all present", msg)
	}
	if !stderrors.Is(e, cause) {
		t.Errorf("errors.Is(e, cause) = false, want Unwrap to expose the cause")
	}
}

func TestError_Is_MatchesSameCode(t *testing.T) {
	t.Parallel()
	e := choraerrors.New("CHORA_NOT_FOUND", "atom missing")
	if !stderrors.Is(e, choraerrors.New("CHORA_NOT_FOUND", "anything")) {
		t.Error("errors.Is with same code must be true (code-based equality)")
	}
	if stderrors.Is(e, choraerrors.New("CHORA_OTHER", "anything")) {
		t.Error("errors.Is with a different code must be false")
	}
}

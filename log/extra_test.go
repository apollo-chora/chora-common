// Package log — supplementary no-op branch tests.
package log_test

import (
	"context"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/log"
)

// TestLogger_WithContextNil_ReturnsSameLogger — a nil context must not
// panic and must return the original logger unchanged.
func TestLogger_WithContextNil_ReturnsSameLogger(t *testing.T) {
	t.Parallel()
	l := log.New("chora-lib-test")
	if got := l.WithContext(nil); got != l {
		t.Error("WithContext(nil) returned a different logger instance")
	}
}

// TestLogger_WithContextNoTracefields_ReturnsSameLogger — a bare context
// (no traceparent / tenant / gcid) must return the original logger.
func TestLogger_WithContextNoTracefields_ReturnsSameLogger(t *testing.T) {
	t.Parallel()
	l := log.New("chora-lib-test")
	if got := l.WithContext(context.Background()); got != l {
		t.Error("WithContext(empty ctx) returned a different logger instance")
	}
}

func TestLogger_Sync_ReturnsNoError(t *testing.T) {
	t.Parallel()
	l := log.New("chora-lib-test")
	if err := l.Sync(); err != nil {
		t.Errorf("Sync: %v", err)
	}
}

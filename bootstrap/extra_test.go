// Package bootstrap — supplementary OTLP-handle edge tests: WaitContext
// after completion, Wait timeout, init panic recovery, nil shutdown.
package bootstrap_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/bootstrap"
)

func TestStartOTLPAsync_WaitContext_AfterCompletion(t *testing.T) {
	h := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: func(context.Context) (func(context.Context) error, error) {
			return func(context.Context) error { return nil }, nil
		},
	})
	res := h.Wait(0)
	if !res.Initialized {
		t.Fatalf("Wait(0): Initialized = false, want true")
	}
	// Done already closed → WaitContext must return immediately via the
	// <-h.done branch.
	res2 := h.WaitContext(context.Background())
	if !res2.Initialized {
		t.Errorf("WaitContext(after completion): Initialized = false, want true")
	}
	if res2.Shutdown == nil {
		t.Error("WaitContext returned nil shutdown")
	}
}

func TestStartOTLPAsync_WaitTimeout_ReturnsTimedOut(t *testing.T) {
	h := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: func(ctx context.Context) (func(context.Context) error, error) {
			// Block until the helper's own init deadline fires.
			<-ctx.Done()
			return nil, ctx.Err()
		},
		InitTimeout: 300 * time.Millisecond,
	})
	start := time.Now()
	res := h.Wait(50 * time.Millisecond)
	if !res.TimedOut {
		t.Fatalf("Wait(50ms) TimedOut = false, want true (InitFunc still blocked)")
	}
	if res.Initialized {
		t.Error("TimedOut result must not be Initialized")
	}
	if res.Shutdown == nil {
		t.Error("timed-out result must carry a no-op shutdown")
	}
	if time.Since(start) > 2*time.Second {
		t.Errorf("Wait(50ms) took %s", time.Since(start))
	}
	// Let the async goroutine settle so it does not leak past the test.
	_ = h.Wait(2 * time.Second)
}

func TestStartOTLPAsync_InitPanic_RecoveredFailSoft(t *testing.T) {
	h := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: func(context.Context) (func(context.Context) error, error) {
			panic("boom in init")
		},
	})
	res := h.Wait(0)
	if res.Initialized {
		t.Error("panicked init must not be Initialized")
	}
	if res.InitError == nil || !strings.Contains(res.InitError.Error(), "panicked") {
		t.Errorf("InitError = %v, want panic recovery error", res.InitError)
	}
	if res.Shutdown == nil {
		t.Error("panicked result must carry a no-op shutdown")
	}
}

func TestStartOTLPAsync_NilShutdownFromInitFunc(t *testing.T) {
	h := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: func(context.Context) (func(context.Context) error, error) {
			return nil, nil // contract violation — nil shutdown
		},
	})
	res := h.Wait(0)
	if res.Initialized {
		t.Error("nil-shutdown init must not be Initialized")
	}
	if res.InitError == nil || !strings.Contains(res.InitError.Error(), "nil shutdown") {
		t.Errorf("InitError = %v, want nil-shutdown error", res.InitError)
	}
}

// Package bootstrap_test exercises the split-deadlines helper that
// decouples OTLP exporter init from the rest of a service's
// bootstrap deadline.
//
// Per C(a).S1 path (b): tracker #151. The pre-fix call pattern blocks
// OTLP init synchronously inside the same deadline as pgx pool init;
// when OTLP times out (slow metadata-server during 4-container cold
// start), the deadline cascades into pgx pool init and crash-loops
// the pod. The new helper runs OTLP in its own goroutine with its
// own (shorter) deadline + fail-soft, so the pgx caller gets the
// full bootstrap deadline.
//
// Coverage target: ≥ 85% (treated as domain per .claude/rules/
// development-execution.md).
package bootstrap_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/bootstrap"
)

// envKeys is the set of env vars StartOTLPAsync reads. Snapshot+restore
// via helper to keep tests hermetic.
var envKeys = []string{
	"CHORA_OTLP_INIT_TIMEOUT_SECONDS",
}

func snapshotEnv(t *testing.T) {
	t.Helper()
	saved := make(map[string]string, len(envKeys))
	for _, k := range envKeys {
		saved[k] = os.Getenv(k)
		_ = os.Unsetenv(k)
	}
	t.Cleanup(func() {
		for _, k := range envKeys {
			if v, ok := saved[k]; ok && v != "" {
				_ = os.Setenv(k, v)
			} else {
				_ = os.Unsetenv(k)
			}
		}
	})
}

// envMu serialises env-mutating tests.
var envMu sync.Mutex

// -----------------------------------------------------------------------------
// StartOTLPAsync — happy path
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_HappyPath_ReturnsShutdown(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	wantShutdown := func(context.Context) error { return nil }
	var initCalled atomic.Bool
	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		initCalled.Store(true)
		return wantShutdown, nil
	}

	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: initFn,
	})
	res := handle.Wait(2 * time.Second)
	if res.Err != nil {
		t.Fatalf("Wait: unexpected err: %v", res.Err)
	}
	if res.Shutdown == nil {
		t.Fatalf("Wait: shutdown must be non-nil after success")
	}
	if !res.Initialized {
		t.Errorf("Wait: Initialized must be true on happy path")
	}
	if !initCalled.Load() {
		t.Errorf("InitFunc was never invoked")
	}
	if err := res.Shutdown(context.Background()); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Timeout — degrades to no-op shutdown, no error
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_Timeout_FailSoftNoOpShutdown(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	released := make(chan struct{})
	defer close(released)

	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		// Block until ctx fires OR test releases. Mimics slow Cloud
		// Trace API call.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-released:
			return nil, nil
		}
	}

	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc:    initFn,
		InitTimeout: 50 * time.Millisecond,
	})
	res := handle.Wait(2 * time.Second)
	// Fail-soft contract: no error bubbles up to caller.
	if res.Err != nil {
		t.Errorf("Wait: expected nil err on timeout (fail-soft); got %v", res.Err)
	}
	if res.Shutdown == nil {
		t.Fatalf("Wait: shutdown must be non-nil even on timeout (no-op)")
	}
	if res.Initialized {
		t.Errorf("Wait: Initialized must be false on timeout")
	}
	if err := res.Shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned err: %v", err)
	}
	if res.TimedOut != true {
		t.Errorf("Wait: TimedOut must be true on timeout")
	}
}

// -----------------------------------------------------------------------------
// Error — degrades to no-op shutdown + structured log, no error to caller
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_InitError_FailSoftNoOpShutdown(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	wantErr := errors.New("cloudtrace: ADC unavailable")
	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		return nil, wantErr
	}

	var logged strings.Builder
	logFn := func(msg string) { logged.WriteString(msg) }

	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: initFn,
		Logf:     logFn,
	})
	res := handle.Wait(2 * time.Second)
	if res.Err != nil {
		t.Errorf("Wait: expected nil err on init error (fail-soft); got %v", res.Err)
	}
	if res.Shutdown == nil {
		t.Fatalf("Wait: shutdown must be non-nil even on init error (no-op)")
	}
	if res.Initialized {
		t.Errorf("Wait: Initialized must be false on init error")
	}
	if res.InitError == nil {
		t.Errorf("Wait: InitError must surface underlying error for diagnostics")
	}
	if !strings.Contains(logged.String(), "otel") {
		t.Errorf("expected log to mention otel; got %q", logged.String())
	}
	if err := res.Shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned err: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Caller context cancelled BEFORE Start — must still produce a no-op handle
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_ParentCtxCancelled_BeforeStart_DegradesGracefully(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		// Should bail on the cancelled context.
		<-ctx.Done()
		return nil, ctx.Err()
	}

	handle := bootstrap.StartOTLPAsync(ctx, bootstrap.OTLPOptions{
		InitFunc:    initFn,
		InitTimeout: 200 * time.Millisecond,
	})
	res := handle.Wait(1 * time.Second)
	if res.Err != nil {
		t.Errorf("Wait: expected nil err under fail-soft; got %v", res.Err)
	}
	if res.Shutdown == nil {
		t.Errorf("Wait: shutdown must be non-nil under fail-soft")
	}
	if res.Initialized {
		t.Errorf("Wait: must be false when parent ctx is cancelled")
	}
}

// -----------------------------------------------------------------------------
// Wait can be called concurrently and is idempotent
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_Wait_IdempotentAndConcurrent(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	var initCalls atomic.Int32
	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		initCalls.Add(1)
		return func(context.Context) error { return nil }, nil
	}

	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: initFn,
	})

	const n = 8
	results := make(chan bootstrap.OTLPResult, n)
	for i := 0; i < n; i++ {
		go func() { results <- handle.Wait(2 * time.Second) }()
	}
	for i := 0; i < n; i++ {
		res := <-results
		if res.Err != nil {
			t.Errorf("concurrent Wait got err: %v", res.Err)
		}
		if !res.Initialized {
			t.Errorf("concurrent Wait expected Initialized=true")
		}
	}
	if got := initCalls.Load(); got != 1 {
		t.Errorf("InitFunc should run exactly once; got %d", got)
	}
}

// -----------------------------------------------------------------------------
// Env-var knob: CHORA_OTLP_INIT_TIMEOUT_SECONDS controls the default timeout.
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_EnvVarTimeoutKnob(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "1")

	got := bootstrap.OTLPTimeoutFromEnv()
	if got != 1*time.Second {
		t.Errorf("OTLPTimeoutFromEnv: want 1s; got %v", got)
	}
}

func TestStartOTLPAsync_EnvVarTimeoutKnob_DefaultWhenUnset(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	got := bootstrap.OTLPTimeoutFromEnv()
	// Default is 15s per design — operator can tune down on slow regions.
	if got != bootstrap.DefaultOTLPInitTimeout {
		t.Errorf("OTLPTimeoutFromEnv: want default %v; got %v",
			bootstrap.DefaultOTLPInitTimeout, got)
	}
}

func TestStartOTLPAsync_EnvVarTimeoutKnob_IgnoresGarbage(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "not-a-number")
	got := bootstrap.OTLPTimeoutFromEnv()
	if got != bootstrap.DefaultOTLPInitTimeout {
		t.Errorf("garbage env -> default; got %v", got)
	}
}

func TestStartOTLPAsync_EnvVarTimeoutKnob_IgnoresZeroAndNegative(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	for _, v := range []string{"0", "-5"} {
		t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", v)
		got := bootstrap.OTLPTimeoutFromEnv()
		if got != bootstrap.DefaultOTLPInitTimeout {
			t.Errorf("env %q -> default; got %v", v, got)
		}
	}
}

// -----------------------------------------------------------------------------
// StartOTLPAsync uses CHORA_OTLP_INIT_TIMEOUT_SECONDS when InitTimeout is 0.
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_DefaultTimeoutFromEnv(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	t.Setenv("CHORA_OTLP_INIT_TIMEOUT_SECONDS", "1")

	released := make(chan struct{})
	defer close(released)
	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	start := time.Now()
	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: initFn,
		// InitTimeout intentionally left zero — must pick up env.
	})
	res := handle.Wait(5 * time.Second)
	elapsed := time.Since(start)
	if res.Err != nil {
		t.Errorf("fail-soft: want nil err; got %v", res.Err)
	}
	if !res.TimedOut {
		t.Errorf("expected TimedOut=true")
	}
	// Allow some headroom for scheduling jitter.
	if elapsed > 3*time.Second {
		t.Errorf("env timeout not respected: elapsed=%v", elapsed)
	}
}

// -----------------------------------------------------------------------------
// Nil InitFunc — must produce a usable no-op handle (not panic).
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_NilInitFunc_NoOp(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: nil,
	})
	res := handle.Wait(500 * time.Millisecond)
	if res.Err != nil {
		t.Errorf("expected nil err under nil-init guard; got %v", res.Err)
	}
	if res.Initialized {
		t.Errorf("Initialized must be false when InitFunc is nil")
	}
	if res.Shutdown == nil {
		t.Fatalf("Shutdown must be non-nil")
	}
	if err := res.Shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned: %v", err)
	}
}

// -----------------------------------------------------------------------------
// WaitContext — caller can pass its own ctx as the upper bound.
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_WaitContext_CtxCancelled(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	released := make(chan struct{})
	defer close(released)
	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-released:
			return nil, nil
		}
	}

	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc:    initFn,
		InitTimeout: 1 * time.Second,
	})

	waitCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	res := handle.WaitContext(waitCtx)
	// WaitContext exiting on ctx cancellation must not look like success.
	if res.Initialized {
		t.Errorf("Initialized must be false when WaitContext ctx fired")
	}
	if res.Shutdown == nil {
		t.Errorf("Shutdown must be non-nil")
	}
}

// -----------------------------------------------------------------------------
// Wait with explicit upperBound=0 means "block until done" — useful for tests
// -----------------------------------------------------------------------------

func TestStartOTLPAsync_Wait_ZeroUpperBoundBlocks(t *testing.T) {
	envMu.Lock()
	defer envMu.Unlock()
	snapshotEnv(t)

	initFn := func(ctx context.Context) (func(context.Context) error, error) {
		time.Sleep(20 * time.Millisecond)
		return func(context.Context) error { return nil }, nil
	}
	handle := bootstrap.StartOTLPAsync(context.Background(), bootstrap.OTLPOptions{
		InitFunc: initFn,
	})
	res := handle.Wait(0)
	if !res.Initialized {
		t.Errorf("Wait(0) must block to completion; got Initialized=false")
	}
}

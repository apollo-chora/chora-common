// Package secrets backoff helper — RED phase first.
//
// Mirrors the JWKS retry-backoff pattern at
// chora-common/auth/identityplatform/identityplatform.go
// (TestValidator_TransientFailures_ResolvedByRetryBackoff +
// TestValidator_PersistentFailure_FailsClosedAfterBackoff +
// TestValidator_StartupBackoff_HonoursContextCancel).
//
// Rationale: E2E-INFRA-COLD-START §3b — Secret Manager occasionally
// returns Unavailable / DeadlineExceeded during cold start on a fresh
// node (Workload Identity Federation token propagation lag). A naive
// AccessSecretVersion call inside the boot sequence then fails the
// whole pod — even when the next call 1-2s later would succeed.
//
// The helper applies the same 6-attempt / 500ms-base / 8s-cap /
// ~30s-budget exponential backoff used for JWKS, with the same
// fail-loud contract on persistent failure (no silent fallbacks).
package secrets

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryFetcher is a SecretFetcher-shaped stub whose GetSecret can be
// programmed to fail N times before returning a programmed value /
// permanent error.
type retryFetcher struct {
	calls         atomic.Int32
	failFirstN    int32
	failCode      codes.Code
	failForever   bool
	successValue  string
	successErr    error
	sleepPerCall  time.Duration
	failOnAllWith error
}

func (f *retryFetcher) GetSecret(ctx context.Context, name string) (string, error) {
	if f.sleepPerCall > 0 {
		t := time.NewTimer(f.sleepPerCall)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-t.C:
		}
	}
	n := f.calls.Add(1)
	if f.failOnAllWith != nil {
		return "", f.failOnAllWith
	}
	if f.failForever {
		return "", status.Errorf(f.failCode, "simulated persistent %s", f.failCode)
	}
	if n <= f.failFirstN {
		return "", status.Errorf(f.failCode, "simulated transient %s (attempt %d)", f.failCode, n)
	}
	return f.successValue, f.successErr
}

// TestFetchSecretWithBackoff_TransientFailures_ResolvedByRetryBackoff —
// 3× Unavailable then OK with payload. Expect success, attempts >= 4,
// total elapsed within the retry budget.
func TestFetchSecretWithBackoff_TransientFailures_ResolvedByRetryBackoff(t *testing.T) {
	f := &retryFetcher{
		failFirstN:   3,
		failCode:     codes.Unavailable,
		successValue: "postgres://u:p@host:5432/db",
	}
	start := time.Now()
	got, err := FetchSecretWithBackoff(context.Background(), f, "demo")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("expected success after transient failures; got %v (after %s, %d attempts)",
			err, elapsed, f.calls.Load())
	}
	if got != f.successValue {
		t.Fatalf("payload mismatch: got %q want %q", got, f.successValue)
	}
	if f.calls.Load() < 4 {
		t.Fatalf("expected >= 4 attempts (3 transient + 1 success); got %d", f.calls.Load())
	}
	if elapsed > retryTotalBudget+2*time.Second {
		t.Errorf("retry-backoff exceeded budget: elapsed %s budget %s", elapsed, retryTotalBudget)
	}
}

// TestFetchSecretWithBackoff_PersistentFailure_FailsClosedAfterBackoff —
// Always Unavailable. Expect ErrSecretFetch wrapped, multiple attempts.
func TestFetchSecretWithBackoff_PersistentFailure_FailsClosedAfterBackoff(t *testing.T) {
	f := &retryFetcher{
		failForever: true,
		failCode:    codes.Unavailable,
	}
	got, err := FetchSecretWithBackoff(context.Background(), f, "demo")
	if err == nil {
		t.Fatalf("expected error on persistent Unavailable; got payload=%q", got)
	}
	if !errors.Is(err, ErrSecretFetch) {
		t.Fatalf("expected errors.Is(err, ErrSecretFetch); got %v", err)
	}
	if f.calls.Load() < 2 {
		t.Errorf("expected backoff to retry at least once; got %d attempts", f.calls.Load())
	}
	if !strings.Contains(err.Error(), "demo") {
		t.Errorf("error should name the secret being fetched, got %q", err.Error())
	}
}

// TestFetchSecretWithBackoff_HonoursContextCancel — Caller cancels mid-
// backoff. Expect ctx.Err() bubbled up (wrapped in ErrSecretFetch),
// loop does not block past cancellation.
func TestFetchSecretWithBackoff_HonoursContextCancel(t *testing.T) {
	f := &retryFetcher{
		failForever: true,
		failCode:    codes.Unavailable,
	}
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after enough time for the first backoff sleep to begin.
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := FetchSecretWithBackoff(ctx, f, "demo")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected context-cancel error; got nil")
	}
	if !errors.Is(err, ErrSecretFetch) {
		t.Errorf("expected errors.Is(err, ErrSecretFetch); got %v", err)
	}
	if elapsed > 5*time.Second {
		t.Errorf("ctx-cancel did not abort backoff promptly: elapsed %s", elapsed)
	}
}

// TestFetchSecretWithBackoff_NotFound_FailsFast — gRPC NotFound is
// non-retriable. Expect 1 attempt, wrapped ErrSecretNotFound.
func TestFetchSecretWithBackoff_NotFound_FailsFast(t *testing.T) {
	f := &retryFetcher{
		failForever: true,
		failCode:    codes.NotFound,
	}
	got, err := FetchSecretWithBackoff(context.Background(), f, "missing")
	if err == nil {
		t.Fatalf("expected error on NotFound; got %q", got)
	}
	if !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("expected errors.Is(err, ErrSecretNotFound); got %v", err)
	}
	if f.calls.Load() != 1 {
		t.Errorf("expected exactly 1 attempt for non-retriable code; got %d", f.calls.Load())
	}
}

// TestFetchSecretWithBackoff_PermissionDenied_FailsFast — PermissionDenied
// is non-retriable (the SA grant is missing; retrying will never help).
func TestFetchSecretWithBackoff_PermissionDenied_FailsFast(t *testing.T) {
	f := &retryFetcher{
		failForever: true,
		failCode:    codes.PermissionDenied,
	}
	_, err := FetchSecretWithBackoff(context.Background(), f, "denied")
	if err == nil {
		t.Fatalf("expected error on PermissionDenied; got nil")
	}
	if !errors.Is(err, ErrSecretFetch) {
		t.Fatalf("expected errors.Is(err, ErrSecretFetch); got %v", err)
	}
	if f.calls.Load() != 1 {
		t.Errorf("expected exactly 1 attempt for non-retriable code; got %d", f.calls.Load())
	}
}

// TestFetchSecretWithBackoff_DeadlineExceeded_Retries — DeadlineExceeded
// is treated as a transient-grpc symptom and is retriable.
func TestFetchSecretWithBackoff_DeadlineExceeded_Retries(t *testing.T) {
	f := &retryFetcher{
		failFirstN:   2,
		failCode:     codes.DeadlineExceeded,
		successValue: "ok",
	}
	got, err := FetchSecretWithBackoff(context.Background(), f, "demo")
	if err != nil {
		t.Fatalf("expected success after transient DeadlineExceeded; got %v", err)
	}
	if got != "ok" {
		t.Fatalf("payload mismatch: got %q", got)
	}
	if f.calls.Load() < 3 {
		t.Errorf("expected >= 3 attempts (2 transient + 1 success); got %d", f.calls.Load())
	}
}

// TestFetchSecretWithBackoff_NilFetcher_FailsFast — Nil fetcher is a
// programmer error; surface it immediately.
func TestFetchSecretWithBackoff_NilFetcher_FailsFast(t *testing.T) {
	_, err := FetchSecretWithBackoff(context.Background(), nil, "demo")
	if err == nil {
		t.Fatalf("expected error on nil fetcher; got nil")
	}
}

// TestFetchSecretWithBackoff_EmptyName_FailsFast — Empty name is a
// programmer error; surface immediately without retrying.
func TestFetchSecretWithBackoff_EmptyName_FailsFast(t *testing.T) {
	f := &retryFetcher{successValue: "x"}
	_, err := FetchSecretWithBackoff(context.Background(), f, "")
	if err == nil {
		t.Fatalf("expected error on empty name; got nil")
	}
	if f.calls.Load() != 0 {
		t.Errorf("expected zero fetcher calls on empty name; got %d", f.calls.Load())
	}
}

// TestFetchSecretWithBackoff_SentinelErrSecretNotFound_FailsFast —
// When the underlying fetcher (e.g. StubClient) returns the wrapped
// ErrSecretNotFound sentinel rather than a raw gRPC NotFound, the
// helper must still recognise it as non-retriable.
func TestFetchSecretWithBackoff_SentinelErrSecretNotFound_FailsFast(t *testing.T) {
	stub := NewStubClient(nil) // empty stub returns ErrSecretNotFound for any name
	_, err := FetchSecretWithBackoff(context.Background(), stub, "missing-from-stub")
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	if !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("expected errors.Is(err, ErrSecretNotFound); got %v", err)
	}
}

// TestFetchSecretWithBackoff_RespectsCallerDeadline — When the caller
// supplies its OWN context deadline (e.g. db.Bootstrap passes a 90s
// CHORA_BOOTSTRAP_TIMEOUT_SECONDS ctx), the helper MUST respect that
// deadline and not impose its own tighter 30s budget on top. This
// was the regression that surfaced when the first rolled image came
// up under cold-start metadata-server latency: my helper was wrapping
// the caller's 90s ctx in a 30s sub-context.
func TestFetchSecretWithBackoff_RespectsCallerDeadline(t *testing.T) {
	// Caller-supplied 5s deadline — bigger than retryTotalBudget would
	// be (30s) is the typical prod case; we use a small deadline here
	// to keep the test fast. The key assertion is: elapsed time MUST
	// be > the would-be tighter limit (~50ms) so we can verify the
	// caller's deadline is what's enforced.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	f := &retryFetcher{
		failForever: true,
		failCode:    codes.Unavailable,
	}
	start := time.Now()
	_, err := FetchSecretWithBackoff(ctx, f, "demo")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	if !errors.Is(err, ErrSecretFetch) {
		t.Errorf("expected errors.Is(err, ErrSecretFetch); got %v", err)
	}
	// Should run until caller's 250ms deadline — not exceed it by much
	// and not finish dramatically before it.
	if elapsed > 500*time.Millisecond {
		t.Errorf("elapsed %s > caller deadline 250ms — helper ignored caller deadline", elapsed)
	}
	if elapsed < 200*time.Millisecond {
		t.Errorf("elapsed %s < caller deadline 250ms — helper failed to use the full deadline", elapsed)
	}
}

// TestFetchSecretWithBackoff_NonGRPCError_IsRetriable — A plain
// non-gRPC error (network blip, unwrapped IO error) should be treated
// as retriable. Persistent non-gRPC errors still exhaust the budget
// and return ErrSecretFetch.
func TestFetchSecretWithBackoff_NonGRPCError_IsRetriable(t *testing.T) {
	f := &retryFetcher{
		failOnAllWith: errors.New("plain non-grpc network blip"),
	}
	_, err := FetchSecretWithBackoff(context.Background(), f, "demo")
	if err == nil {
		t.Fatalf("expected error; got nil")
	}
	if !errors.Is(err, ErrSecretFetch) {
		t.Fatalf("expected errors.Is(err, ErrSecretFetch); got %v", err)
	}
	if f.calls.Load() < 2 {
		t.Errorf("expected backoff to retry; got %d attempts", f.calls.Load())
	}
}

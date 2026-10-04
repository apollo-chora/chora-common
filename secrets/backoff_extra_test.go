// Package secrets — supplementary backoff helper unit tests.
//
// These cover the remaining guard branches of FetchSecretWithBackoff and
// its predicates: a caller that cancels the context before the first
// attempt, and the nil-input behaviour of statusCode / isNonRetriable.
package secrets

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
)

// TestFetchSecretWithBackoff_CancelledBeforeFirstAttempt — a
// pre-cancelled context must be surfaced as a wrapped ErrSecretFetch
// naming zero completed attempts, without invoking the fetcher at all.
func TestFetchSecretWithBackoff_CancelledBeforeFirstAttempt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := &stubFetcherNoCall{}
	_, err := FetchSecretWithBackoff(ctx, f, "chora-x")
	if err == nil {
		t.Fatal("expected cancellation error; got nil")
	}
	if !errors.Is(err, ErrSecretFetch) {
		t.Errorf("expected wrapped ErrSecretFetch, got %v", err)
	}
	if !strings.Contains(err.Error(), "after 0 attempts") {
		t.Errorf("error should report zero attempts, got %q", err.Error())
	}
	if f.called {
		t.Error("fetcher must not be invoked on a pre-cancelled context")
	}
}

type stubFetcherNoCall struct {
	called bool
}

func (f *stubFetcherNoCall) GetSecret(_ context.Context, _ string) (string, error) {
	f.called = true
	return "", nil
}

func TestStatusCode_NilReturnsOK(t *testing.T) {
	t.Parallel()
	if got := statusCode(nil); got != codes.OK {
		t.Errorf("statusCode(nil) = %v, want codes.OK", got)
	}
}

func TestIsNonRetriable_NilReturnsFalse(t *testing.T) {
	t.Parallel()
	if isNonRetriable(nil) {
		t.Error("isNonRetriable(nil) = true, want false")
	}
}

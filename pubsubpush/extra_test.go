// Package pubsubpush — supplementary edge tests: body-read failure,
// whitespace-only bearer token, and defaulted verifier in NewHandler.
package pubsubpush_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/pubsubpush"
)

// errReader fails on every Read, forcing io.ReadAll's error path.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("read exploded") }

func TestDecode_ReadBodyErrorRejected(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/push", errReader{})
	_, err := pubsubpush.Decode(req)
	if err == nil || !errors.Is(err, pubsubpush.ErrBadRequest) {
		t.Fatalf("err = %v, want ErrBadRequest", err)
	}
	if !strings.Contains(err.Error(), "read body") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestVerifier_EmptyBearerTokenRejected(t *testing.T) {
	t.Parallel()
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://chora.example.com/push",
		ValidateToken: func(context.Context, string, string) (pubsubpush.TokenClaims, error) {
			t.Fatal("validator must not run for empty token")
			return pubsubpush.TokenClaims{}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/push", nil)
	req.Header.Set("Authorization", "Bearer    ") // whitespace-only token
	err := v.Verify(context.Background(), req)
	if err == nil || !errors.Is(err, pubsubpush.ErrUnauthorized) {
		t.Fatalf("err = %v, want ErrUnauthorized", err)
	}
}

func TestNewHandler_DefaultsNilVerifier(t *testing.T) {
	t.Parallel()
	dispatched := false
	h := pubsubpush.NewHandler(pubsubpush.HandlerConfig{
		Verifier: nil, // must default to a no-op verifier
		Dispatch: func(context.Context, pubsubpush.PushMessage) error {
			dispatched = true
			return nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/push", strings.NewReader(`{
		"message": {"data": "aGk=", "messageId": "m1"},
		"subscription": "projects/p/subscriptions/s"
	}`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !dispatched {
		t.Error("dispatch not invoked")
	}
}

// pubsubpush_test.go — RED phase tests for the canonical Pub/Sub push
// helper. Iter G.8 / m14.iter5g.code-p (#11 — Pub/Sub push subscription
// binding).
package pubsubpush_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/pubsubpush"
)

// ---------------------------------------------------------------------------
// Decode tests
// ---------------------------------------------------------------------------

func TestDecode_Success(t *testing.T) {
	payload := []byte(`{"hello":"world"}`)
	body := buildBody(t, payload, map[string]string{
		"event_id":   "evt-1",
		"tenant_id":  "tnt-1",
		"chora_imda": "transparency",
	}, "projects/p/subscriptions/s")

	req := httptest.NewRequest(http.MethodPost, "/internal/pubsub/x", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	got, err := pubsubpush.Decode(req)
	if err != nil {
		t.Fatalf("Decode unexpected err: %v", err)
	}
	if string(got.Data) != string(payload) {
		t.Errorf("Data = %q want %q", got.Data, payload)
	}
	if got.MessageID == "" {
		t.Errorf("MessageID empty")
	}
	if got.Attributes["event_id"] != "evt-1" {
		t.Errorf("Attributes[event_id]=%q want evt-1", got.Attributes["event_id"])
	}
	if got.Subscription != "projects/p/subscriptions/s" {
		t.Errorf("Subscription=%q", got.Subscription)
	}
}

func TestDecode_RejectsWrongMethod(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	if _, err := pubsubpush.Decode(req); err == nil {
		t.Fatalf("expected error for GET")
	} else if !errors.Is(err, pubsubpush.ErrBadRequest) {
		t.Errorf("err=%v want ErrBadRequest", err)
	}
}

func TestDecode_RejectsBadJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("not-json"))
	if _, err := pubsubpush.Decode(req); err == nil {
		t.Fatalf("expected error for bad json")
	}
}

func TestDecode_RejectsBadBase64(t *testing.T) {
	body := `{"message":{"data":"@@@not-base64@@@","messageId":"1","publishTime":"2026-01-01T00:00:00Z"},"subscription":"projects/p/subscriptions/s"}`
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	if _, err := pubsubpush.Decode(req); err == nil {
		t.Fatalf("expected error for bad base64 data")
	}
}

func TestDecode_EmptyDataAllowed(t *testing.T) {
	// Pub/Sub allows an empty data field (attributes-only message). Decode
	// must not fail in that case — the dispatcher decides how to handle it.
	body := `{"message":{"data":"","messageId":"1","publishTime":"2026-01-01T00:00:00Z","attributes":{"a":"b"}},"subscription":"projects/p/subscriptions/s"}`
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	got, err := pubsubpush.Decode(req)
	if err != nil {
		t.Fatalf("Decode empty data: %v", err)
	}
	if len(got.Data) != 0 {
		t.Errorf("Data len=%d want 0", len(got.Data))
	}
}

func TestDecode_MissingMessage(t *testing.T) {
	body := `{"subscription":"projects/p/subscriptions/s"}`
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	if _, err := pubsubpush.Decode(req); err == nil {
		t.Fatalf("expected error for missing message")
	}
}

func TestDecode_NilBodyRejected(t *testing.T) {
	// httptest.NewRequest with nil body still wraps with http.NoBody so
	// hit the explicit path via a bare struct.
	req := &http.Request{Method: http.MethodPost}
	if _, err := pubsubpush.Decode(req); err == nil {
		t.Fatalf("expected error for nil body")
	}
}

func TestVerifier_NoValidator_ButAudienceSet_Errors(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		// ValidateToken intentionally omitted — Enabled() is still true
		// because Audience is set; Verify must return ErrUnauthorized.
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err == nil {
		t.Fatalf("expected unauthorized when Audience set but no ValidateToken")
	}
}

// ---------------------------------------------------------------------------
// Verifier tests
// ---------------------------------------------------------------------------

func TestVerifier_Disabled_AllowsAnyRequest(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{})
	if err := v.Verify(context.Background(), httptest.NewRequest(http.MethodPost, "/x", nil)); err != nil {
		t.Fatalf("Disabled verifier should allow request: %v", err)
	}
}

func TestVerifier_MissingAuthHeader(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience:           "https://example.com/internal/pubsub/x",
		TrustedServiceAccs: []string{"svc@chora-489812.iam.gserviceaccount.com"},
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{}, errors.New("ValidateToken should not be called")
		},
	})
	if err := v.Verify(context.Background(), httptest.NewRequest(http.MethodPost, "/x", nil)); err == nil {
		t.Fatalf("expected error for missing Authorization")
	}
}

func TestVerifier_BadScheme(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Basic xxxx")
	if err := v.Verify(context.Background(), req); err == nil {
		t.Fatalf("expected error for non-Bearer auth")
	}
}

func TestVerifier_ValidatorRejects(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		ValidateToken: func(_ context.Context, token, aud string) (pubsubpush.TokenClaims, error) {
			if token != "abc" {
				t.Fatalf("token mismatch: %q", token)
			}
			return pubsubpush.TokenClaims{}, errors.New("invalid signature")
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err == nil {
		t.Fatalf("expected validator rejection")
	}
}

func TestVerifier_WrongIssuer(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{Email: "x@y.iam.gserviceaccount.com", EmailVerified: true, Issuer: "https://evil.example.com"}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err == nil {
		t.Fatalf("expected error for non-google issuer")
	}
}

func TestVerifier_UnverifiedEmail(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{Email: "x@y.iam.gserviceaccount.com", EmailVerified: false, Issuer: "https://accounts.google.com"}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err == nil {
		t.Fatalf("expected error for unverified email")
	}
}

func TestVerifier_TrustedSA_OK(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience:           "https://example.com/x",
		TrustedServiceAccs: []string{"svc@chora-489812.iam.gserviceaccount.com"},
		ValidateToken: func(_ context.Context, token, aud string) (pubsubpush.TokenClaims, error) {
			if aud != "https://example.com/x" {
				t.Fatalf("audience passed=%q want %q", aud, "https://example.com/x")
			}
			return pubsubpush.TokenClaims{
				Email:         "svc@chora-489812.iam.gserviceaccount.com",
				EmailVerified: true,
				Issuer:        "https://accounts.google.com",
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

func TestVerifier_UntrustedSA_Rejected(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience:           "https://example.com/x",
		TrustedServiceAccs: []string{"svc@chora-489812.iam.gserviceaccount.com"},
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{
				Email:         "evil@attacker.iam.gserviceaccount.com",
				EmailVerified: true,
				Issuer:        "https://accounts.google.com",
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err == nil {
		t.Fatalf("expected untrusted SA rejection")
	}
}

func TestVerifier_NoAllowlist_AnyVerifiedSAOK(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{
				Email:         "anything@chora-489812.iam.gserviceaccount.com",
				EmailVerified: true,
				Issuer:        "https://accounts.google.com",
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req.Header.Set("Authorization", "Bearer abc")
	if err := v.Verify(context.Background(), req); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Handler tests — combines decode + verify + dispatch
// ---------------------------------------------------------------------------

func TestHandler_OKDispatches(t *testing.T) {
	var got []byte
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{})
	h := pubsubpush.NewHandler(pubsubpush.HandlerConfig{
		Verifier: v,
		Dispatch: func(_ context.Context, msg pubsubpush.PushMessage) error {
			got = msg.Data
			return nil
		},
	})
	body := buildBody(t, []byte(`{"a":1}`), nil, "projects/p/subscriptions/s")
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	if string(got) != `{"a":1}` {
		t.Errorf("got=%q", got)
	}
}

func TestHandler_VerifyFails_401(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{
		Audience: "https://example.com/x",
		ValidateToken: func(_ context.Context, _, _ string) (pubsubpush.TokenClaims, error) {
			return pubsubpush.TokenClaims{}, errors.New("bad token")
		},
	})
	h := pubsubpush.NewHandler(pubsubpush.HandlerConfig{
		Verifier: v,
		Dispatch: func(_ context.Context, msg pubsubpush.PushMessage) error {
			t.Fatalf("dispatch should not be called")
			return nil
		},
	})
	body := buildBody(t, []byte(`{"a":1}`), nil, "projects/p/subscriptions/s")
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer bad")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestHandler_DecodeFails_400(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{})
	h := pubsubpush.NewHandler(pubsubpush.HandlerConfig{
		Verifier: v,
		Dispatch: func(_ context.Context, msg pubsubpush.PushMessage) error {
			t.Fatalf("dispatch should not be called")
			return nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("not-json"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestHandler_DispatchError_500(t *testing.T) {
	v := pubsubpush.NewVerifier(pubsubpush.VerifierConfig{})
	h := pubsubpush.NewHandler(pubsubpush.HandlerConfig{
		Verifier: v,
		Dispatch: func(_ context.Context, _ pubsubpush.PushMessage) error {
			return errors.New("boom")
		},
	})
	body := buildBody(t, []byte(`{"a":1}`), nil, "projects/p/subscriptions/s")
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestHandler_NoDispatchPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on nil dispatch")
		}
	}()
	pubsubpush.NewHandler(pubsubpush.HandlerConfig{
		Verifier: pubsubpush.NewVerifier(pubsubpush.VerifierConfig{}),
	})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func buildBody(t *testing.T, payload []byte, attrs map[string]string, sub string) string {
	t.Helper()
	type msg struct {
		Data        string            `json:"data"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		Attributes  map[string]string `json:"attributes,omitempty"`
	}
	type env struct {
		Message      msg    `json:"message"`
		Subscription string `json:"subscription"`
	}
	e := env{
		Message: msg{
			Data:        base64.StdEncoding.EncodeToString(payload),
			MessageID:   fmt.Sprintf("mid-%d", time.Now().UnixNano()),
			PublishTime: "2026-05-13T12:00:00Z",
			Attributes:  attrs,
		},
		Subscription: sub,
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

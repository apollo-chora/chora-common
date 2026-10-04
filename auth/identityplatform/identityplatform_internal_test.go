// identityplatform_internal_test.go — internal-package coverage for the
// unexported resilience internals: refreshJWKSWithBackoff context
// cancellation (loop-top + mid-sleep), fetchDiscovery / fetchJWKS error
// branches, and lookupKey's in-flight + empty-cache-refresh-failure
// paths. These are only reachable from inside the package (the
// constructor always succeeds or fails before exposing a Validator).
package identityplatform

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// rawValidator builds a Validator WITHOUT the eager boot fetch so tests
// can drive the private internals directly.
func rawValidator(cfg Config) *Validator {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return &Validator{
		cfg:             cfg,
		keys:            make(map[string]any),
		refreshCooldown: 30 * time.Second,
	}
}

func TestRefreshJWKSWithBackoff_TopLevelCancel(t *testing.T) {
	v := rawValidator(Config{IssuerURL: "http://127.0.0.1:1", Audience: "a"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := v.refreshJWKSWithBackoff(ctx)
	if !errors.Is(err, ErrJWKSFetch) {
		t.Fatalf("err = %v, want ErrJWKSFetch wrap", err)
	}
	if !strings.Contains(err.Error(), "backoff cancelled after 0 attempts") {
		t.Errorf("err = %v, want the loop-top cancellation message", err)
	}
}

func TestRefreshJWKSWithBackoff_CancelMidSleep(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "always 503", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	v := rawValidator(Config{IssuerURL: srv.URL, Audience: "a", HTTPClient: srv.Client()})
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel while the backoff timer is sleeping (first attempt fails
	// fast, the 500ms timer is still running at 100ms).
	done := make(chan struct{})
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
		close(done)
	}()

	start := time.Now()
	err := v.refreshJWKSWithBackoff(ctx)
	elapsed := time.Since(start)
	<-done

	if !errors.Is(err, ErrJWKSFetch) {
		t.Fatalf("err = %v, want ErrJWKSFetch wrap", err)
	}
	if !strings.Contains(err.Error(), "mid-sleep") {
		t.Errorf("err = %v, want the mid-sleep cancellation message", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("backoff did not abort promptly: %s", elapsed)
	}
}

func TestFetchDiscovery_NewRequestError(t *testing.T) {
	v := rawValidator(Config{IssuerURL: "://not-a-url", Audience: "a"})
	if _, err := v.fetchDiscovery(context.Background()); err == nil {
		t.Fatal("expected NewRequest error for an unparseable issuer URL")
	}
}

func TestFetchDiscovery_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("this is not json"))
	}))
	defer srv.Close()

	v := rawValidator(Config{IssuerURL: srv.URL, Audience: "a", HTTPClient: srv.Client()})
	if _, err := v.fetchDiscovery(context.Background()); err == nil {
		t.Fatal("expected discovery decode error")
	}
}

func TestFetchJWKS_NewRequestError(t *testing.T) {
	v := rawValidator(Config{IssuerURL: "http://x", Audience: "a"})
	if _, err := v.fetchJWKS(context.Background(), "://bad-jwks-url"); err == nil {
		t.Fatal("expected NewRequest error for an unparseable jwks_uri")
	}
}

func TestFetchJWKS_Non200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer srv.Close()

	v := rawValidator(Config{IssuerURL: srv.URL, Audience: "a", HTTPClient: srv.Client()})
	if _, err := v.fetchJWKS(context.Background(), srv.URL); err == nil {
		t.Fatal("expected jwks status error")
	}
}

func TestFetchJWKS_TransportError(t *testing.T) {
	// A request error surfaced by the HTTP client's Do (unroutable
	// address) must be wrapped as ErrJWKSFetch.
	v := rawValidator(Config{IssuerURL: "http://x", Audience: "a"})
	if _, err := v.fetchJWKS(context.Background(), "http://127.0.0.1:1"); err == nil {
		t.Fatal("expected jwks transport error")
	}
}

func TestFetchJWKS_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("not json either"))
	}))
	defer srv.Close()

	v := rawValidator(Config{IssuerURL: srv.URL, Audience: "a", HTTPClient: srv.Client()})
	if _, err := v.fetchJWKS(context.Background(), srv.URL); err == nil {
		t.Fatal("expected jwks decode error")
	}
}

func TestLookupKey_InFlightRefreshBranch(t *testing.T) {
	// Simulate a concurrent refresh already in flight: cache empty (so
	// the cooldown guard does not fire) and inFlightRefresh pre-set.
	v := rawValidator(Config{IssuerURL: "http://x", Audience: "a"})
	v.keys = map[string]any{} // empty
	v.inFlightRefresh = true

	_, err := v.lookupKey(context.Background(), "some-kid")
	if !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("err = %v, want ErrUnknownKey", err)
	}
	if !strings.Contains(err.Error(), "concurrent refresh in flight") {
		t.Errorf("err = %v, want the in-flight refresh message", err)
	}
}

func TestLookupKey_EmptyCacheRefreshFailure(t *testing.T) {
	// Fail-closed at startup: cache empty + refresh fails must surface
	// the refresh error itself, not ErrUnknownKey.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "perma-503", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	v := rawValidator(Config{IssuerURL: srv.URL, Audience: "a", HTTPClient: srv.Client()})
	v.keys = map[string]any{} // empty cache

	_, err := v.lookupKey(context.Background(), "kid")
	if !errors.Is(err, ErrJWKSFetch) {
		t.Fatalf("err = %v, want ErrJWKSFetch (fail-closed with empty cache)", err)
	}
}

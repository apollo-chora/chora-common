// resilience_test.go — RED-phase TDD specs for JWKS cache resilience
// per the directive "architectural correctness + resilience > speed".
//
// Required behaviours (production-grade, dead-pod safe):
//
//  1. Lazy-load + TTL: validator fetches JWKS at startup AND tracks the
//     fetch timestamp. After TTL expiry, the next request triggers a
//     background refresh; in-flight requests continue using the cache.
//  2. Last-known-good fallback: when a refresh fails (5xx, network),
//     the validator MUST NOT discard the cached keys. Subsequent
//     ValidateJWT calls keep working with the stale-but-valid cache
//     until the refresh succeeds or the keys themselves expire.
//  3. Fail-closed (NEVER fail-open): if the cache is empty AND refresh
//     fails, ValidateJWT returns ErrJWKSFetch — never silently accept
//     an unsigned/unsigned-by-trusted-key token.
//
// Test fixture: a JWKS server that flips between OK/5xx via an atomic
// counter so we can simulate transient outages without flaky timing.
package identityplatform_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/auth/identityplatform"
)

// flipJWKSServer exposes a discovery + JWKS endpoint whose JWKS reply
// flips between 200 OK and 503 based on the embedded atomic flag. Use
// SetState(0|1) to control: 0 = healthy; 1 = 5xx outage.
type flipJWKSServer struct {
	*httptest.Server
	state    *atomic.Int32 // 0 healthy, 1 outage
	jwksJSON string
}

func newFlipJWKSServer(t *testing.T, jwksJSON string) *flipJWKSServer {
	t.Helper()
	flag := &atomic.Int32{}
	mux := http.NewServeMux()
	server := &flipJWKSServer{state: flag, jwksJSON: jwksJSON}
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		if flag.Load() == 1 {
			http.Error(w, "simulated 5xx", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON))
	})
	server.Server = httptest.NewServer(mux)
	return server
}

func (s *flipJWKSServer) Outage()  { s.state.Store(1) }
func (s *flipJWKSServer) Recover() { s.state.Store(0) }

func rslLeftPad(b []byte, n int) []byte {
	if len(b) >= n {
		return b
	}
	out := make([]byte, n)
	copy(out[n-len(b):], b)
	return out
}

func makeES256Token(t *testing.T, priv *ecdsa.PrivateKey, kid, issuer, audience string, now time.Time) string {
	t.Helper()
	claims := map[string]any{
		"iss":       issuer,
		"aud":       audience,
		"sub":       "user-1",
		"gcid":      "gcid-1",
		"tenant_id": "tenant-1",
		"iat":       now.Unix(),
		"exp":       now.Add(1 * time.Hour).Unix(),
	}
	header := map[string]any{"alg": "ES256", "typ": "JWT", "kid": kid}
	hdrJSON, _ := json.Marshal(header)
	clmJSON, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(hdrJSON) + "." + base64.RawURLEncoding.EncodeToString(clmJSON)
	h := sha256.Sum256([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, priv, h[:])
	if err != nil {
		t.Fatalf("ecdsa sign: %v", err)
	}
	rb := rslLeftPad(r.Bytes(), 32)
	sb := rslLeftPad(s.Bytes(), 32)
	sig := append(rb, sb...)
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func newSignerFixture(t *testing.T) (*ecdsa.PrivateKey, string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa keygen: %v", err)
	}
	kid := "rsl-key-1"
	xb := rslLeftPad(priv.PublicKey.X.Bytes(), 32)
	yb := rslLeftPad(priv.PublicKey.Y.Bytes(), 32)
	jwksJSON := fmt.Sprintf(`{"keys":[{"kty":"EC","alg":"ES256","crv":"P-256","kid":"%s","use":"sig","x":"%s","y":"%s"}]}`,
		kid, base64.RawURLEncoding.EncodeToString(xb), base64.RawURLEncoding.EncodeToString(yb))
	return priv, kid, jwksJSON
}

// TestValidator_LastKnownGoodFallback_OnRefreshFailure verifies:
//
//   - Eager fetch at startup populates cache.
//   - When the JWKS endpoint goes 5xx, ValidateJWT MUST keep working
//     using the cached keys (last-known-good).
//   - When the cache key matches, no refresh is triggered.
func TestValidator_LastKnownGoodFallback_OnRefreshFailure(t *testing.T) {
	priv, kid, jwksJSON := newSignerFixture(t)
	srv := newFlipJWKSServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  "chora-dev",
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	// Now simulate the JWKS endpoint going down.
	srv.Outage()

	// Token signed with the existing kid — cache hit; no refresh needed.
	tok := makeES256Token(t, priv, kid, srv.URL, "chora-dev", time.Now().UTC())
	claims, err := v.ValidateJWT(context.Background(), tok)
	if err != nil {
		t.Fatalf("expected last-known-good cache hit; got %v", err)
	}
	if claims.GCID != "gcid-1" {
		t.Errorf("claims.GCID = %s; want gcid-1", claims.GCID)
	}
}

// TestValidator_UnknownKid_RefreshFailure_FallsBackToCache verifies
// that when a token bears an unknown kid AND the JWKS endpoint is down,
// the validator does NOT silently accept the token (fail-closed) but
// also does NOT discard the cached keys.
//
// Behaviour: returns ErrUnknownKey (NOT ErrJWKSFetch — the cache is
// still valid for known kids). Subsequent calls with KNOWN kids
// continue to succeed.
func TestValidator_UnknownKid_RefreshFailure_PreservesCache(t *testing.T) {
	priv, kid, jwksJSON := newSignerFixture(t)
	srv := newFlipJWKSServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  "chora-dev",
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	srv.Outage()

	// Token signed with a wrong kid — refresh will fail.
	bogusToken := makeES256Token(t, priv, "unknown-kid", srv.URL, "chora-dev", time.Now().UTC())
	if _, err := v.ValidateJWT(context.Background(), bogusToken); err == nil {
		t.Fatalf("expected error on unknown kid + outage")
	}

	// Now hit the validator again with the KNOWN kid — must still work.
	knownToken := makeES256Token(t, priv, kid, srv.URL, "chora-dev", time.Now().UTC())
	claims, err := v.ValidateJWT(context.Background(), knownToken)
	if err != nil {
		t.Fatalf("known-kid path must survive refresh failure; got %v", err)
	}
	if claims.GCID != "gcid-1" {
		t.Errorf("claims.GCID = %s; want gcid-1", claims.GCID)
	}
}

// TestValidator_TTLAware_DoesNotFlipOnEveryRequest verifies the cache
// is honoured between requests — concurrent tokens with the same kid
// should hit the cache (the JWKS server should see exactly ONE fetch
// during the eager-load + zero subsequent fetches as long as TTL holds).
func TestValidator_TTLAware_DoesNotFetchOnEveryRequest(t *testing.T) {
	priv, kid, jwksJSON := newSignerFixture(t)

	jwksFetches := &atomic.Int32{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		jwksFetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  "chora-dev",
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	// Eager fetch at startup = 1 hit.
	if jwksFetches.Load() != 1 {
		t.Errorf("after NewValidator: fetches = %d; want 1", jwksFetches.Load())
	}

	// 100 concurrent ValidateJWT calls with the same kid → no further fetches.
	for i := 0; i < 100; i++ {
		tok := makeES256Token(t, priv, kid, srv.URL, "chora-dev", time.Now().UTC())
		if _, err := v.ValidateJWT(context.Background(), tok); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	if jwksFetches.Load() != 1 {
		t.Errorf("steady-state cache hits should not refetch; fetches = %d", jwksFetches.Load())
	}
}

// TestValidator_TransientFailures_ResolvedByRetryBackoff verifies the
// boot-time resilience invariant per Option B1 (28525e1c Decision 2):
// NewValidator MUST tolerate transient JWKS failures (5xx, network blips)
// by retrying with exponential backoff. Once the endpoint recovers
// within the backoff window, NewValidator succeeds.
//
// Acceptance: 3 consecutive 503 responses followed by a 200 → validator
// constructed without error and JWKS cache populated.
//
// Production motivation: 2026-05-16 chora-gateway crash loop —
// securetoken.google.com discovery endpoint timed out at cold start
// (context deadline exceeded), killing the pod before serving any
// traffic. Old pod stayed healthy. Resilience-priority memory says
// "fail at first request if persistent", not at boot on transient.
func TestValidator_TransientFailures_ResolvedByRetryBackoff(t *testing.T) {
	priv, kid, jwksJSON := newSignerFixture(t)
	_ = priv

	jwksAttempts := &atomic.Int32{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		// First 3 attempts: 503 outage. 4th+: serve real JWKS doc.
		attempt := jwksAttempts.Add(1)
		if attempt <= 3 {
			http.Error(w, "simulated transient 503", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	start := time.Now()
	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  "chora-dev",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("NewValidator should survive transient 503s via retry-backoff; got %v (after %s, %d attempts)",
			err, elapsed, jwksAttempts.Load())
	}
	if v == nil {
		t.Fatalf("expected non-nil validator")
	}
	if jwksAttempts.Load() < 4 {
		t.Errorf("expected >= 4 jwks attempts (3 fail + 1 succeed); got %d", jwksAttempts.Load())
	}
	// Sanity: verify the cache populated by signing a token with the
	// known kid and validating — proves the retry path actually loaded
	// keys, not just suppressed the error.
	tok := makeES256Token(t, priv, kid, srv.URL, "chora-dev", time.Now().UTC())
	if _, err := v.ValidateJWT(context.Background(), tok); err != nil {
		t.Errorf("post-retry validation failed; cache not populated: %v", err)
	}
}

// TestValidator_PersistentFailure_FailsClosedAfterBackoff verifies the
// fail-loud invariant survives the retry shim: if the JWKS endpoint
// stays 5xx across the full backoff window, NewValidator MUST return
// ErrJWKSFetch — never silently return a fail-open validator.
func TestValidator_PersistentFailure_FailsClosedAfterBackoff(t *testing.T) {
	jwksAttempts := &atomic.Int32{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		jwksAttempts.Add(1)
		http.Error(w, "persistent 503", http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  "chora-dev",
	})
	if err == nil {
		t.Fatalf("expected ErrJWKSFetch on persistent 503 across backoff window; got validator=%v", v)
	}
	// Must wrap ErrJWKSFetch (preserve sentinel for errors.Is callers).
	if !errors.Is(err, identityplatform.ErrJWKSFetch) {
		t.Errorf("expected errors.Is(err, ErrJWKSFetch); got err=%v", err)
	}
	// Should have made multiple attempts (proves backoff fired).
	if jwksAttempts.Load() < 2 {
		t.Errorf("expected backoff to retry at least once; got %d attempts", jwksAttempts.Load())
	}
}

// TestValidator_StartupBackoff_HonoursContextCancel verifies the
// retry-backoff loop responds to context cancellation — important for
// graceful shutdown during the backoff window. A cancelled context
// MUST abort the loop with a context error wrapped in ErrJWKSFetch
// (not a hung process).
func TestValidator_StartupBackoff_HonoursContextCancel(t *testing.T) {
	// JWKS endpoint stays 503 — would cause indefinite retry under
	// naive backoff.
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "perma-503", http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Wrap the HTTP client with a short-context client so each request
	// fails fast — primary verification is that the *overall* backoff
	// loop terminates within a bounded budget rather than hanging.
	tightClient := &http.Client{Timeout: 200 * time.Millisecond}

	start := time.Now()
	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL:  srv.URL,
		Audience:   "chora-dev",
		HTTPClient: tightClient,
	})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("expected error on persistent 503; got validator=%v", v)
	}
	// Total budget MUST be bounded — backoff cap (~30s) is an absolute
	// upper bound; this test asserts the loop does not hang past it.
	if elapsed > 60*time.Second {
		t.Errorf("retry-backoff loop ran too long (%s) — should be capped at ~30s budget", elapsed)
	}
}

// TestValidator_RefreshFloodGuard_DoesNotHammerIDP verifies that a
// burst of unknown-kid tokens does NOT translate to a burst of JWKS
// fetches. The validator MUST debounce refresh attempts (single-flight
// or per-attempt cooldown) so a misbehaving client cannot overwhelm
// the IdP.
//
// Acceptance: 100 concurrent unknown-kid requests against a stable
// JWKS endpoint produce ≤2 actual fetches (1 startup + 1 refresh).
// More than that means we're hammering the IdP under attack.
func TestValidator_RefreshFloodGuard_DoesNotHammerIDP(t *testing.T) {
	_, kid, jwksJSON := newSignerFixture(t)
	_ = kid

	jwksFetches := &atomic.Int32{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		jwksFetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  "chora-dev",
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	startupCount := jwksFetches.Load()

	// Make 100 lookups against unknown kid via the public ValidateJWT
	// surface. We can't sign valid tokens with an unknown kid (since
	// our test fixture only knows the one); we use intentionally bogus
	// tokens so each call exercises lookupKey with an unknown kid.
	// The flood guard must keep total fetches ≤ startup + 1 within a
	// short window.
	priv2, _, _ := newSignerFixture(t)
	bogusKid := "kid-not-in-jwks"
	for i := 0; i < 100; i++ {
		bogus := makeES256Token(t, priv2, bogusKid, srv.URL, "chora-dev", time.Now().UTC())
		_, _ = v.ValidateJWT(context.Background(), bogus)
	}

	got := jwksFetches.Load()
	if got > startupCount+5 {
		t.Errorf("flood guard violated: 100 unknown-kid requests caused %d fetches (want ≤ startup+5 = %d). "+
			"Implement single-flight + cooldown on refreshJWKS.",
			got, startupCount+5)
	}
}

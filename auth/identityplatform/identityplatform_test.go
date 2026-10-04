// Package identityplatform_test holds the RED-phase TDD specs for the JWT
// validation library used by chora-bff-gateway (and any other BFF/edge service
// that sits at the trust boundary).
//
// Per S3.6 spec (JWT validation flow):
//   - Identity Platform mints + signs the JWT with custom claims (gcid,
//     tenant_id, kyc_status, role_summary).
//   - chora-bff-gateway validates the JWT signature against Identity Platform
//     JWKS, verifies issuer + audience + expiration, extracts custom claims.
//   - Backend services trust BFF via Cloud Service Mesh mTLS.
//
// This test exercises:
//  1. JWKS fetch from Identity Platform discovery URL.
//  2. JWT signature verification (HS256 for the test fixture; RS256 in prod).
//  3. Claims extraction (gcid, tenant_id, kyc_status, role_summary, email, sub).
//  4. Audience + issuer + expiration enforcement.
//  5. Middleware that drops Claims into request context.
//
// All endpoints/secrets sourced from env (IDP_ISSUER_URL, IDP_AUDIENCE) — no
// inline config.
package identityplatform_test

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/identityplatform"
)

const (
	testAudience = "chora-dev"
)

// ─────────────────────────────────────────────────────────────────────────────
// JWKS fetch + caching
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_FetchesJWKSFromIssuer(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)))
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, map[string]any{
		"iss":          srv.URL,
		"aud":          testAudience,
		"sub":          "ip-uid-1",
		"email":        "phyllis@mightymind.sg",
		"gcid":         "01970000-0000-7000-8000-0000000000aa",
		"tenant_id":    "01970000-0000-7000-8000-0000000000bb",
		"kyc_status":   "verified",
		"role_summary": map[string]any{"learner": []string{"any"}},
		"iat":          now.Unix(),
		"exp":          now.Add(1 * time.Hour).Unix(),
	})

	claims, err := v.ValidateJWT(context.Background(), tok)
	if err != nil {
		t.Fatalf("ValidateJWT: %v", err)
	}
	if claims.GCID != "01970000-0000-7000-8000-0000000000aa" {
		t.Errorf("GCID = %q", claims.GCID)
	}
	if claims.TenantID != "01970000-0000-7000-8000-0000000000bb" {
		t.Errorf("TenantID = %q", claims.TenantID)
	}
	if claims.KYCStatus != "verified" {
		t.Errorf("KYCStatus = %q", claims.KYCStatus)
	}
	if claims.Email != "phyllis@mightymind.sg" {
		t.Errorf("Email = %q", claims.Email)
	}
	if claims.Sub != "ip-uid-1" {
		t.Errorf("Sub = %q", claims.Sub)
	}
	if len(claims.RoleSummary) == 0 {
		t.Errorf("RoleSummary should be populated")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Audience enforcement
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_RejectsWrongAudience(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, map[string]any{
		"iss": srv.URL,
		"aud": "wrong-audience",
		"sub": "ip-uid-1",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatalf("expected audience rejection")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Issuer enforcement
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_RejectsWrongIssuer(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, map[string]any{
		"iss": "https://malicious.example.com",
		"aud": testAudience,
		"sub": "x",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatalf("expected issuer rejection")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Expiration enforcement
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_RejectsExpiredToken(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	past := time.Now().UTC().Add(-2 * time.Hour)
	tok := signES256(t, priv, kid, map[string]any{
		"iss": srv.URL,
		"aud": testAudience,
		"sub": "x",
		"iat": past.Unix(),
		"exp": past.Add(time.Hour).Unix(),
	})
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatalf("expected expiration rejection")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Tampered signature rejection
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_RejectsTamperedSignature(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, map[string]any{
		"iss": srv.URL,
		"aud": testAudience,
		"sub": "x",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	// Tamper by flipping a byte in the signature segment.
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3-part JWT, got %d", len(parts))
	}
	parts[2] = parts[2][:len(parts[2])-2] + "AA"
	tampered := strings.Join(parts, ".")
	if _, err := v.ValidateJWT(context.Background(), tampered); err == nil {
		t.Fatalf("expected tampered-signature rejection")
	}
}

func TestValidator_RejectsMalformedToken(t *testing.T) {
	t.Parallel()
	_, _, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})
	if _, err := v.ValidateJWT(context.Background(), "not-a-jwt"); err == nil {
		t.Fatalf("expected malformed rejection")
	}
	if _, err := v.ValidateJWT(context.Background(), ""); err == nil {
		t.Fatalf("expected empty token rejection")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Middleware — drops Claims into request context.
// ─────────────────────────────────────────────────────────────────────────────

func TestMiddleware_AttachesClaimsToContext(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, map[string]any{
		"iss":       srv.URL,
		"aud":       testAudience,
		"sub":       "ip-uid-1",
		"gcid":      "01970000-0000-7000-8000-0000000000aa",
		"tenant_id": "01970000-0000-7000-8000-0000000000bb",
		"iat":       now.Unix(),
		"exp":       now.Add(time.Hour).Unix(),
	})

	var capturedClaims *identityplatform.Claims
	handler := v.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := identityplatform.ClaimsFromContext(r.Context())
		if ok {
			capturedClaims = c
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if capturedClaims == nil {
		t.Fatalf("expected claims in context")
	}
	if capturedClaims.GCID != "01970000-0000-7000-8000-0000000000aa" {
		t.Errorf("GCID = %q", capturedClaims.GCID)
	}
}

func TestMiddleware_Returns401WhenMissingAuthorizationHeader(t *testing.T) {
	t.Parallel()
	_, _, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})
	handler := v.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestMiddleware_Returns401OnInvalidToken(t *testing.T) {
	t.Parallel()
	_, _, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})
	handler := v.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Config validation
// ─────────────────────────────────────────────────────────────────────────────

func TestNewValidator_RejectsEmptyConfig(t *testing.T) {
	t.Parallel()
	if _, err := identityplatform.NewValidator(identityplatform.Config{}); err == nil {
		t.Fatalf("expected error on empty config")
	}
	if _, err := identityplatform.NewValidator(identityplatform.Config{IssuerURL: "https://x"}); err == nil {
		t.Fatalf("expected error empty audience")
	}
	if _, err := identityplatform.NewValidator(identityplatform.Config{Audience: testAudience}); err == nil {
		t.Fatalf("expected error empty issuer")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func jwksServer(t *testing.T, jwksJSON string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(fmt.Sprintf(`{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)))
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(jwksJSON))
	})
	return httptest.NewServer(mux)
}

func newES256TestKey(t *testing.T) (*ecdsa.PrivateKey, string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa keygen: %v", err)
	}
	kid := "test-key-1"
	xb := priv.PublicKey.X.Bytes()
	yb := priv.PublicKey.Y.Bytes()
	// Pad to 32 bytes for P-256.
	xb = leftPad(xb, 32)
	yb = leftPad(yb, 32)
	jwksJSON := fmt.Sprintf(`{
		"keys": [{
			"kty": "EC",
			"alg": "ES256",
			"crv": "P-256",
			"kid": "%s",
			"use": "sig",
			"x": "%s",
			"y": "%s"
		}]
	}`, kid, base64URLEncode(xb), base64URLEncode(yb))
	return priv, kid, jwksJSON
}

func signES256(t *testing.T, priv *ecdsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header := map[string]any{
		"alg": "ES256",
		"typ": "JWT",
		"kid": kid,
	}
	hdrJSON, _ := json.Marshal(header)
	clmJSON, _ := json.Marshal(claims)
	signing := base64URLEncode(hdrJSON) + "." + base64URLEncode(clmJSON)

	// ES256: SHA256 hash + ECDSA sign + r||s concatenation (each 32B).
	h := sha256Sum([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, priv, h)
	if err != nil {
		t.Fatalf("ecdsa sign: %v", err)
	}
	rb := leftPad(r.Bytes(), 32)
	sb := leftPad(s.Bytes(), 32)
	sig := append(rb, sb...)
	return signing + "." + base64URLEncode(sig)
}

func sha256Sum(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

func leftPad(b []byte, n int) []byte {
	if len(b) >= n {
		return b
	}
	out := make([]byte, n)
	copy(out[n-len(b):], b)
	return out
}

func base64URLEncode(b []byte) string {
	return base64.RawURLEncoding.EncodeToString(b)
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional coverage tests (RSA path, []string audience, JWKS errors).
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_AcceptsAudienceListClaim(t *testing.T) {
	t.Parallel()
	priv, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, map[string]any{
		"iss": srv.URL,
		"aud": []string{"chora-other", testAudience},
		"sub": "ip-uid-1",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	claims, err := v.ValidateJWT(context.Background(), tok)
	if err != nil {
		t.Fatalf("ValidateJWT: %v", err)
	}
	if claims.Sub != "ip-uid-1" {
		t.Errorf("Sub = %q", claims.Sub)
	}
}

func TestValidator_RejectsUnknownKid(t *testing.T) {
	t.Parallel()
	priv, _, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})

	now := time.Now().UTC()
	// Sign with a kid that's not in the JWKS.
	tok := signES256(t, priv, "unknown-kid", map[string]any{
		"iss": srv.URL,
		"aud": testAudience,
		"sub": "x",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatalf("expected unknown-kid rejection")
	}
}

func TestNewValidator_ReturnsErrorOnJWKSFetchFailure(t *testing.T) {
	t.Parallel()
	// Use a clearly-invalid issuer URL that the HTTP client will fail to dial.
	if _, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: "http://127.0.0.1:1",
		Audience:  testAudience,
	}); err == nil {
		t.Fatalf("expected JWKS fetch error")
	}
}

func TestNewValidator_ReturnsErrorOnDiscoveryNon200(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	if _, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	}); err == nil {
		t.Fatalf("expected discovery 500 error")
	}
}

func TestNewValidator_ReturnsErrorOnMissingJWKSURI(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	if _, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	}); err == nil {
		t.Fatalf("expected missing-jwks-uri error")
	}
}

func TestValidator_RejectsUnsupportedAlg(t *testing.T) {
	t.Parallel()
	_, kid, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})

	now := time.Now().UTC()
	// Hand-craft a JWT with alg=none.
	header := map[string]any{"alg": "none", "typ": "JWT", "kid": kid}
	hdrJSON, _ := json.Marshal(header)
	claims := map[string]any{"iss": srv.URL, "aud": testAudience, "sub": "x", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	clmJSON, _ := json.Marshal(claims)
	tok := base64URLEncode(hdrJSON) + "." + base64URLEncode(clmJSON) + "."
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatalf("expected unsupported-alg rejection")
	}
}

func TestValidator_RejectsMalformedHeaderBase64(t *testing.T) {
	t.Parallel()
	_, _, jwksJSON := newES256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()
	v, _ := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})
	if _, err := v.ValidateJWT(context.Background(), "@@@.eyJzdWIiOiJ4In0.AA"); err == nil {
		t.Fatalf("expected base64 decode error")
	}
}

func TestValidator_AcceptsRSA256KeySet(t *testing.T) {
	t.Parallel()
	// Build a fake RSA-only JWKS from a real RSA key, sign an RS256 JWT.
	priv, kid, jwksJSON := newRS256TestKey(t)
	srv := jwksServer(t, jwksJSON)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	now := time.Now().UTC()
	tok := signRS256(t, priv, kid, map[string]any{
		"iss": srv.URL,
		"aud": testAudience,
		"sub": "ip-uid-1",
		"iat": now.Unix(),
		"exp": now.Add(time.Hour).Unix(),
	})
	claims, err := v.ValidateJWT(context.Background(), tok)
	if err != nil {
		t.Fatalf("ValidateJWT RS256: %v", err)
	}
	if claims.Sub != "ip-uid-1" {
		t.Errorf("Sub = %q", claims.Sub)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// RS256 helpers
// ─────────────────────────────────────────────────────────────────────────────

func newRS256TestKey(t *testing.T) (*rsa.PrivateKey, string, string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa keygen: %v", err)
	}
	kid := "rsa-test-key-1"

	n := priv.PublicKey.N.Bytes()
	// Encode public exponent as big-endian — typically 0x010001.
	ebytes := []byte{}
	e := priv.PublicKey.E
	for e > 0 {
		ebytes = append([]byte{byte(e & 0xff)}, ebytes...)
		e >>= 8
	}
	jwksJSON := fmt.Sprintf(`{
		"keys": [{
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"kid": "%s",
			"n": "%s",
			"e": "%s"
		}]
	}`, kid, base64URLEncode(n), base64URLEncode(ebytes))
	return priv, kid, jwksJSON
}

func signRS256(t *testing.T, priv *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	header := map[string]any{
		"alg": "RS256",
		"typ": "JWT",
		"kid": kid,
	}
	hdrJSON, _ := json.Marshal(header)
	clmJSON, _ := json.Marshal(claims)
	signing := base64URLEncode(hdrJSON) + "." + base64URLEncode(clmJSON)
	hashed := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("rsa sign: %v", err)
	}
	return signing + "." + base64URLEncode(sig)
}

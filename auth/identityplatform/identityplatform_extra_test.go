// identityplatform_extra_test.go — statement-coverage extension for the
// remaining ValidateJWT / verifySignature / JWKS-parse / middleware
// branches. Test-only; does not weaken existing assertions in
// identityplatform_test.go / resilience_test.go.
package identityplatform_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/auth/identityplatform"
)

// mustValidator builds a validator from a JWKS-serving server, failing
// the test on construction error, and returns both so callers can sign
// tokens against the server URL.
func mustValidator(t *testing.T, jwksJSON string) (*identityplatform.Validator, *httptest.Server) {
	t.Helper()
	srv := jwksServer(t, jwksJSON)
	t.Cleanup(srv.Close)
	v, err := identityplatform.NewValidator(identityplatform.Config{
		IssuerURL: srv.URL,
		Audience:  testAudience,
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return v, srv
}

// basicClaims is the standard claim envelope used by most error tests.
func basicClaims(issuer string, now time.Time) map[string]any {
	return map[string]any{
		"iss": issuer, "aud": testAudience, "sub": "x",
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
}

func TestValidator_RejectsClaimsAndSignatureBase64Errors(t *testing.T) {
	_, _, jwksJSON := newES256TestKey(t)
	v, _ := mustValidator(t, jwksJSON)

	// Valid header encoding but garbage claims base64.
	tok := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256","kid":"k"}`)) + ".@@@.AA"
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Error("expected claims base64 decode error")
	}
	// Valid header + claims encodings but garbage signature base64.
	tok = base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256","kid":"k"}`)) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(`{"iss":"x"}`)) + ".@@@"
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Error("expected signature base64 decode error")
	}
}

func TestValidator_RejectsNonJSONHeader(t *testing.T) {
	_, _, jwksJSON := newES256TestKey(t)
	v, _ := mustValidator(t, jwksJSON)

	tok := base64.RawURLEncoding.EncodeToString([]byte("not-json")) + "." +
		base64.RawURLEncoding.EncodeToString([]byte(`{}`)) + ".AA"
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Error("expected header JSON parse error")
	}
}

// TestValidator_RejectsNonJSONClaims covers the claims JSON-unmarshal
// error AFTER a valid signature: the token is signed over the exact
// signing input, so verification passes and the parse failure is the
// first error surfaced.
func TestValidator_RejectsNonJSONClaims(t *testing.T) {
	priv, kid, jwksJSON := newES256TestKey(t)
	v, _ := mustValidator(t, jwksJSON)

	clmB64 := base64.RawURLEncoding.EncodeToString([]byte("not-json"))
	tok := signSegmentsES256(t, priv, kid, `{"alg":"ES256","typ":"JWT","kid":"`+kid+`"}`, clmB64)
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Error("expected claims JSON parse error")
	}
}

func TestValidator_RejectsNonStringAudience(t *testing.T) {
	// A numeric audience falls through the string and []any cases to the
	// final false return in audienceMatches.
	priv, kid, jwksJSON := newES256TestKey(t)
	v, srv := mustValidator(t, jwksJSON)

	now := time.Now().UTC()
	claims := basicClaims(srv.URL, now)
	claims["aud"] = 12345
	tok := signES256(t, priv, kid, claims)
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatal("expected numeric audience to be rejected")
	}
}

func TestValidator_RS256WithECKeyRejected(t *testing.T) {
	// JWKS holds an EC key under kid; the token claims alg=RS256. The
	// RSA type check must fail before any RSA verification.
	_, kid, jwksJSON := newES256TestKey(t)
	v, srv := mustValidator(t, jwksJSON)

	now := time.Now().UTC()
	header := map[string]any{"alg": "RS256", "typ": "JWT", "kid": kid}
	hdrJSON, _ := json.Marshal(header)
	clmJSON, _ := json.Marshal(basicClaims(srv.URL, now))
	tok := base64URLEncode(hdrJSON) + "." + base64URLEncode(clmJSON) + ".AA"
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatal("expected RS256-over-EC-key rejection")
	}
}

func TestValidator_RS256TamperedSignature(t *testing.T) {
	priv, kid, jwksJSON := newRS256TestKey(t)
	v, srv := mustValidator(t, jwksJSON)

	now := time.Now().UTC()
	tok := signRS256(t, priv, kid, basicClaims(srv.URL, now))
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("bad token shape")
	}
	parts[2] = base64URLEncode([]byte("tampered-signature-bytes"))
	if _, err := v.ValidateJWT(context.Background(), strings.Join(parts, ".")); err == nil {
		t.Fatal("expected RSA signature verification failure")
	}
}

func TestValidator_ES256WithRSAKeyRejected(t *testing.T) {
	// JWKS holds an RSA key under kid; the token claims alg=ES256. The
	// ECDSA type check must fail (an RSA key cannot satisfy ES256).
	_, kid, jwksJSON := newRS256TestKey(t)
	v, srv := mustValidator(t, jwksJSON)

	now := time.Now().UTC()
	header := map[string]any{"alg": "ES256", "typ": "JWT", "kid": kid}
	hdrJSON, _ := json.Marshal(header)
	clmJSON, _ := json.Marshal(basicClaims(srv.URL, now))
	tok := base64URLEncode(hdrJSON) + "." + base64URLEncode(clmJSON) + ".AA"
	if _, err := v.ValidateJWT(context.Background(), tok); err == nil {
		t.Fatal("expected ES256-over-RSA-key rejection")
	}
}

func TestValidator_ES256ShortSignature(t *testing.T) {
	priv, kid, jwksJSON := newES256TestKey(t)
	v, srv := mustValidator(t, jwksJSON)

	now := time.Now().UTC()
	tok := signES256(t, priv, kid, basicClaims(srv.URL, now))
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("bad token shape")
	}
	// A 6-byte signature is not the required 64-byte r||s concatenation.
	parts[2] = base64URLEncode([]byte("short!"))
	if _, err := v.ValidateJWT(context.Background(), strings.Join(parts, ".")); err == nil {
		t.Fatal("expected ES256 signature-length rejection")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// JWKS parse-error branches (RSA n/e + EC crv/x/y)
// ─────────────────────────────────────────────────────────────────────────────

func TestValidator_JWKS_RSAParseFailures(t *testing.T) {
	priv, kid, ecDoc := newES256TestKey(t)

	t.Run("bad n skipped", func(t *testing.T) {
		broken := `{"kty":"RSA","kid":"bad-n","n":"@@@not-base64@@@","e":"AQAB"}`
		v, srv := mustValidator(t, buildJWKSDoc(ecDoc, broken))
		assertValidates(t, v, srv, priv, kid)
	})

	t.Run("bad e skipped", func(t *testing.T) {
		broken := `{"kty":"RSA","kid":"bad-e","n":"AQID","e":"@@@not-base64@@@"}`
		v, srv := mustValidator(t, buildJWKSDoc(ecDoc, broken))
		assertValidates(t, v, srv, priv, kid)
	})
}

func TestValidator_JWKS_ECParseFailures(t *testing.T) {
	priv, kid, ecDoc := newES256TestKey(t)

	cases := []struct {
		name   string
		broken string
	}{
		{"unsupported curve", `{"kty":"EC","kid":"bad-crv","crv":"P-384","x":"AQID","y":"AQID"}`},
		{"corrupt x", `{"kty":"EC","kid":"bad-x","crv":"P-256","x":"@@@","y":"AQID"}`},
		{"corrupt y", `{"kty":"EC","kid":"bad-y","crv":"P-256","x":"AQID","y":"@@@"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v, srv := mustValidator(t, buildJWKSDoc(ecDoc, tc.broken))
			assertValidates(t, v, srv, priv, kid)
		})
	}
}

// TestValidator_RotationRefreshAddsNewKid covers lookupKey's
// refresh-success-then-found path: a kid added by a JWKS rotation is
// picked up lazily on the next validation.
func TestValidator_RotationRefreshAddsNewKid(t *testing.T) {
	_, _, doc1 := newSignerFixture(t)
	// Second key with a UNIQUE kid (newSignerFixture hardcodes
	// rs l-key-1, which would just hit the boot cache).
	priv2, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	const kid2 = "rot-key-2"
	xb := leftPad(priv2.PublicKey.X.Bytes(), 32)
	yb := leftPad(priv2.PublicKey.Y.Bytes(), 32)
	doc2 := fmt.Sprintf(`{"keys":[{"kty":"EC","alg":"ES256","crv":"P-256","kid":"%s","use":"sig","x":"%s","y":"%s"}]}`,
		kid2, base64URLEncode(xb), base64URLEncode(yb))

	// Serve an EMPTY key set during construction: with cacheNonEmpty=false
	// the refresh-cooldown flood guard (30s) is bypassed, so the next
	// lookup re-fetches instead of returning "refresh cooldown active" —
	// exactly the lazy rotation pick-up this test drives.
	current := `{"keys":[]}`
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, r *http.Request) {
		base := "http://" + r.Host
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"jwks_uri":"%s/.well-known/jwks.json","issuer":"%s"}`, base, base)
	})
	mux.HandleFunc("/.well-known/jwks.json", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(current))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v, err := identityplatform.NewValidator(identityplatform.Config{IssuerURL: srv.URL, Audience: testAudience})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}

	// Rotate: serve a doc containing BOTH keys.
	current = mergeJWKSDocs(doc1, doc2)

	now := time.Now().UTC()
	tok := makeES256Token(t, priv2, kid2, srv.URL, testAudience, now)
	claims, err := v.ValidateJWT(context.Background(), tok)
	if err != nil {
		t.Fatalf("rotation refresh should pick up the new kid: %v", err)
	}
	if claims.Sub != "user-1" {
		t.Errorf("claims.Sub = %q", claims.Sub)
	}
}

func TestMiddleware_RejectsWrongSchemeAndEmptyToken(t *testing.T) {
	_, _, jwksJSON := newES256TestKey(t)
	v, _ := mustValidator(t, jwksJSON)
	handler := v.Middleware()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong scheme: expected 401, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	req.Header.Set("Authorization", "Bearer   ")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("empty bearer token: expected 401, got %d", rec.Code)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers unique to this file
// ─────────────────────────────────────────────────────────────────────────────

// assertValidates signs an ES256 token under the given validator's
// server and asserts the valid key still validates despite a broken
// sibling key entry.
func assertValidates(t *testing.T, v *identityplatform.Validator, srv *httptest.Server, priv *ecdsa.PrivateKey, kid string) {
	t.Helper()
	now := time.Now().UTC()
	tok := signES256(t, priv, kid, basicClaims(srv.URL, now))
	if _, err := v.ValidateJWT(context.Background(), tok); err != nil {
		t.Fatalf("valid EC key must still validate: %v", err)
	}
}

// signSegmentsES256 signs an arbitrary header-JSON + claims-base64 pair,
// producing a token whose signature verifies against the EC key (used to
// reach post-signature parse errors).
func signSegmentsES256(t *testing.T, priv *ecdsa.PrivateKey, kid, hdrJSON, clmB64 string) string {
	t.Helper()
	hdrB64 := base64URLEncode([]byte(hdrJSON))
	signing := hdrB64 + "." + clmB64
	h := sha256Sum([]byte(signing))
	r, s, err := ecdsa.Sign(rand.Reader, priv, h)
	if err != nil {
		t.Fatalf("ecdsa sign: %v", err)
	}
	sig := append(leftPad(r.Bytes(), 32), leftPad(s.Bytes(), 32)...)
	return signing + "." + base64URLEncode(sig)
}

// buildJWKSDoc embeds a broken key entry alongside an existing JWKS
// document's key objects.
func buildJWKSDoc(ecDoc, brokenEntry string) string {
	return fmt.Sprintf(`{"keys": [%s, %s]}`, brokenEntry, jwksKeysInner(ecDoc))
}

// jwksKeysInner extracts the key OBJECTS out of an existing JWKS
// document via JSON round-tripping (string surgery on the raw doc is
// fragile against indentation + trailing braces).
func jwksKeysInner(doc string) string {
	var parsed struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.Unmarshal([]byte(doc), &parsed); err != nil {
		return doc
	}
	parts := make([]string, 0, len(parsed.Keys))
	for _, k := range parsed.Keys {
		parts = append(parts, string(k))
	}
	return strings.Join(parts, ", ")
}

func mergeJWKSDocs(a, b string) string {
	return fmt.Sprintf(`{"keys": [%s, %s]}`, jwksKeysInner(a), jwksKeysInner(b))
}

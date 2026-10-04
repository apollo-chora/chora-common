// Package chorasession — supplementary token-shape edge tests: signature
// segment decoding, claims segment decoding/parsing, and missing exp.
package chorasession_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/chorasession"
)

func newValidator(t *testing.T) *chorasession.Validator {
	t.Helper()
	v, err := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	return v
}

// jsonMarshal is the local no-fail marshal used while crafting raw tokens.
func jsonMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// TestValidate_RejectsNonBase64Signature — the signature segment must be
// RawURL-base64; garbage yields ErrMalformedToken.
func TestValidate_RejectsNonBase64Signature(t *testing.T) {
	t.Parallel()
	v := newValidator(t)
	now := time.Now().UTC()
	hdr := jsonMarshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	claims := jsonMarshal(validClaims(now, nil))
	tok := base64.RawURLEncoding.EncodeToString(hdr) + "." +
		base64.RawURLEncoding.EncodeToString(claims) + "." + "!!!not-base64!!!"
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrMalformedToken) {
		t.Fatalf("err = %v, want ErrMalformedToken", err)
	}
}

// signOver signs an EXACT signing string so tests can inject a malformed
// claims segment while keeping the HMAC valid (MAC check precedes decode).
func signOver(key []byte, signing string) string {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestValidate_RejectsNonBase64ClaimsSegment(t *testing.T) {
	t.Parallel()
	v := newValidator(t)
	hdr := jsonMarshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	hdrB64 := base64.RawURLEncoding.EncodeToString(hdr)
	tok := signOver(testSigner, hdrB64+".not-base64!!")
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrMalformedToken) {
		t.Fatalf("err = %v, want ErrMalformedToken", err)
	}
}

func TestValidate_RejectsClaimsSegmentNotJSON(t *testing.T) {
	t.Parallel()
	v := newValidator(t)
	hdr := jsonMarshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	hdrB64 := base64.RawURLEncoding.EncodeToString(hdr)
	clmB64 := base64.RawURLEncoding.EncodeToString([]byte("this is not json"))
	tok := signOver(testSigner, hdrB64+"."+clmB64)
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrMalformedToken) {
		t.Fatalf("err = %v, want ErrMalformedToken", err)
	}
}

// TestValidate_RejectsMissingExp — exp absent (or non-numeric) maps to
// ErrMissingClaim via jsonNumberToInt64's default branch.
func TestValidate_RejectsMissingExp(t *testing.T) {
	t.Parallel()
	v := newValidator(t)
	tok := signHS256(t, testSigner, validClaims(time.Now().UTC(), map[string]any{"exp": nil}))
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrMissingClaim) {
		t.Fatalf("err = %v, want ErrMissingClaim", err)
	}
}

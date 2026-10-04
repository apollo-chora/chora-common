// validator_test.go — TDD RED specs for the ChoraSession HS256 JWT validator.
//
// The ChoraSession validator handles JWTs MINTED by chora-gateway's
// POST /api/v1/auth/session/mint endpoint after a successful Firebase ID
// token exchange. These tokens are:
//
//   - HS256 signed against a shared signing key loaded from Secret Manager
//   - Carry claims: iss, aud, sub, gcid, tenant_id, email, role_summary,
//     iat, exp
//   - Used by chora-gateway as the inbound auth artifact for /api/* requests
//
// This validator REPLACES the Firebase-JWKS-backed identityplatform.Validator
// on the /api/* path — that validator now ONLY runs inside MintHandler to
// validate the incoming Firebase ID token during the exchange.
//
// Strict TDD: tests written BEFORE the implementation per
// .claude/rules/development-execution.md. RED → GREEN → REFACTOR.
package chorasession_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/auth/chorasession"
)

const (
	testIssuer   = "https://api.chora.site"
	testAudience = "chora-489812"
)

var testSigner = []byte("test-chora-session-signer-key-must-be-32-bytes-or-more")

// signHS256 builds an HS256 JWT manually for tests — mirrors mint_handler.go's
// signSessionJWT path but with arbitrary claims so test cases can inject
// missing / wrong / tampered values.
func signHS256(t *testing.T, key []byte, claims map[string]any) string {
	t.Helper()
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(hb) + "." +
		base64.RawURLEncoding.EncodeToString(cb)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signing))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signing + "." + sig
}

func validClaims(now time.Time, overrides map[string]any) map[string]any {
	c := map[string]any{
		"iss":          testIssuer,
		"aud":          testAudience,
		"sub":          "gcid-test-uuid",
		"gcid":         "01970000-0000-7000-8000-0000000000aa",
		"tenant_id":    "01970000-0000-7000-8000-0000000000bb",
		"email":        "alice@example.com",
		"role_summary": "",
		"iat":          now.Add(-30 * time.Second).Unix(),
		"exp":          now.Add(time.Hour).Unix(),
	}
	for k, v := range overrides {
		c[k] = v
	}
	return c
}

// --- NewValidator construction errors ---------------------------------------

func TestNewValidator_RejectsShortSigner(t *testing.T) {
	t.Parallel()
	_, err := chorasession.NewValidator([]byte("too-short"), testIssuer, testAudience)
	if err == nil {
		t.Fatalf("expected error for signer < 32 bytes")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "32") {
		t.Errorf("error should mention 32-byte minimum, got %q", err)
	}
}

func TestNewValidator_RejectsEmptyIssuer(t *testing.T) {
	t.Parallel()
	_, err := chorasession.NewValidator(testSigner, "", testAudience)
	if err == nil {
		t.Fatalf("expected error for empty issuer")
	}
}

func TestNewValidator_RejectsEmptyAudience(t *testing.T) {
	t.Parallel()
	_, err := chorasession.NewValidator(testSigner, testIssuer, "")
	if err == nil {
		t.Fatalf("expected error for empty audience")
	}
}

func TestNewValidator_RejectsNilSigner(t *testing.T) {
	t.Parallel()
	_, err := chorasession.NewValidator(nil, testIssuer, testAudience)
	if err == nil {
		t.Fatalf("expected error for nil signer")
	}
}

// --- Valid token round-trip -------------------------------------------------

func TestValidate_HappyPath(t *testing.T) {
	t.Parallel()
	v, err := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, nil))
	claims, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if claims.GCID != "01970000-0000-7000-8000-0000000000aa" {
		t.Errorf("GCID = %q, want fixture", claims.GCID)
	}
	if claims.TenantID != "01970000-0000-7000-8000-0000000000bb" {
		t.Errorf("TenantID = %q", claims.TenantID)
	}
	if claims.Email != "alice@example.com" {
		t.Errorf("Email = %q", claims.Email)
	}
	if claims.Sub != "gcid-test-uuid" {
		t.Errorf("Sub = %q", claims.Sub)
	}
	if claims.Raw == nil {
		t.Errorf("Raw escape hatch should be populated")
	}
	if claims.ExpiresAt.Before(now) {
		t.Errorf("ExpiresAt should be in future")
	}
}

// --- Negative cases ---------------------------------------------------------

func TestValidate_RejectsBadSignature(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, nil))
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("expected 3-part JWT")
	}
	// XOR the entire signature with a non-zero byte to GUARANTEE a different
	// MAC. Simply replacing the last base64 char is flaky because base64
	// encoding's last char only carries 2 meaningful bits — a swap can
	// silently round-trip to the same bytes ~6% of the time.
	sigBytes, decodeErr := base64.RawURLEncoding.DecodeString(parts[2])
	if decodeErr != nil {
		t.Fatalf("decode original sig: %v", decodeErr)
	}
	for i := range sigBytes {
		sigBytes[i] ^= 0xff
	}
	parts[2] = base64.RawURLEncoding.EncodeToString(sigBytes)
	tampered := strings.Join(parts, ".")
	_, err := v.Validate(tampered)
	if !errors.Is(err, chorasession.ErrInvalidSignature) {
		t.Errorf("got %v, want ErrInvalidSignature", err)
	}
}

func TestValidate_RejectsWrongSigner(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	otherKey := []byte("a-different-signing-key-that-is-long-enough-for-32-bytes")
	now := time.Now().UTC()
	tok := signHS256(t, otherKey, validClaims(now, nil))
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrInvalidSignature) {
		t.Errorf("got %v, want ErrInvalidSignature", err)
	}
}

func TestValidate_RejectsExpired(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	past := time.Now().UTC().Add(-2 * time.Hour)
	tok := signHS256(t, testSigner, validClaims(past, map[string]any{
		"iat": past.Add(-time.Hour).Unix(),
		"exp": past.Unix(), // expired 2h ago
	}))
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrTokenExpired) {
		t.Errorf("got %v, want ErrTokenExpired", err)
	}
}

func TestValidate_AllowsSkewWithinTolerance(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	// exp is 30s in the past but within the 60s default skew tolerance.
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{
		"iat": now.Add(-time.Hour).Unix(),
		"exp": now.Add(-30 * time.Second).Unix(),
	}))
	_, err := v.Validate(tok)
	if err != nil {
		t.Errorf("expected token within 60s skew to pass, got %v", err)
	}
}

func TestValidate_RejectsMissingGCID(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	claims := validClaims(now, nil)
	delete(claims, "gcid")
	tok := signHS256(t, testSigner, claims)
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrMissingClaim) {
		t.Errorf("got %v, want ErrMissingClaim for missing gcid", err)
	}
}

func TestValidate_RejectsEmptyGCID(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{"gcid": ""}))
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrMissingClaim) {
		t.Errorf("got %v, want ErrMissingClaim for empty gcid", err)
	}
}

// CHO-1653 — bootstrap-mode JWTs (CHO-1648) carry `tenant_id=""` so a user
// with no memberships can call POST /api/v1/tenants/bootstrap. The validator
// MUST accept both `tenant_id` missing-from-claims and `tenant_id: ""`
// shapes, returning Claims{TenantID: ""} so route-level handlers can enforce
// the empty-tenant policy themselves. Pre-CHO-1653 these two tests asserted
// ErrMissingClaim — the strict check defeated Phase 5 once CHO-1652 added
// the bootstrap route to DefaultJWTGatedPrefixes.

func TestValidate_AcceptsMissingTenantID_BootstrapMode(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	claims := validClaims(now, nil)
	delete(claims, "tenant_id")
	tok := signHS256(t, testSigner, claims)
	got, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("want nil err for missing tenant_id (bootstrap mode); got %v", err)
	}
	if got.TenantID != "" {
		t.Errorf("Claims.TenantID = %q, want empty (bootstrap mode)", got.TenantID)
	}
	if got.GCID == "" {
		t.Errorf("Claims.GCID must still be populated; got empty")
	}
}

func TestValidate_AcceptsEmptyTenantID_BootstrapMode(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{"tenant_id": ""}))
	got, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("want nil err for empty tenant_id (bootstrap mode); got %v", err)
	}
	if got.TenantID != "" {
		t.Errorf("Claims.TenantID = %q, want empty (bootstrap mode)", got.TenantID)
	}
	if got.GCID == "" {
		t.Errorf("Claims.GCID must still be populated; got empty")
	}
}

func TestValidate_RejectsWrongIssuer(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{
		"iss": "https://evil.example.com",
	}))
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrInvalidIssuer) {
		t.Errorf("got %v, want ErrInvalidIssuer", err)
	}
}

func TestValidate_RejectsWrongAudience(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{
		"aud": "some-other-project",
	}))
	_, err := v.Validate(tok)
	if !errors.Is(err, chorasession.ErrInvalidAudience) {
		t.Errorf("got %v, want ErrInvalidAudience", err)
	}
}

func TestValidate_RejectsTamperedClaimsSegment(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, nil))
	parts := strings.Split(tok, ".")
	// Modify claim segment in place (tamper with gcid by encoding new claims)
	// without re-signing. Signature MAC over old segments won't match.
	tamperedClaims := validClaims(now, map[string]any{"gcid": "ATTACKER-INJECTED-GCID"})
	cb, _ := json.Marshal(tamperedClaims)
	parts[1] = base64.RawURLEncoding.EncodeToString(cb)
	tampered := strings.Join(parts, ".")
	_, err := v.Validate(tampered)
	if !errors.Is(err, chorasession.ErrInvalidSignature) {
		t.Errorf("got %v, want ErrInvalidSignature for tampered claims", err)
	}
}

func TestValidate_RejectsMalformedToken(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	cases := []string{
		"",
		"not.a.jwt",
		"only-two.parts",
		"a.b.c.d", // 4 parts
		"!@#.$%^.&*(",
	}
	for _, c := range cases {
		_, err := v.Validate(c)
		if err == nil {
			t.Errorf("expected error for malformed token %q", c)
		}
	}
}

func TestValidate_RejectsUnsupportedAlg(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	// Build a header with alg=none.
	header := map[string]string{"alg": "none", "typ": "JWT"}
	hb, _ := json.Marshal(header)
	cb, _ := json.Marshal(validClaims(time.Now().UTC(), nil))
	signing := base64.RawURLEncoding.EncodeToString(hb) + "." +
		base64.RawURLEncoding.EncodeToString(cb)
	tok := signing + ".AAAA"
	_, err := v.Validate(tok)
	if err == nil {
		t.Errorf("expected error for alg=none")
	}
}

func TestValidate_AcceptsEmptyRoleSummary(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	// role_summary is empty string per Bucket 1 — that MUST be accepted.
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{
		"role_summary": "",
	}))
	claims, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("Validate with empty role_summary: %v", err)
	}
	if claims.RoleSummary != "" {
		t.Errorf("RoleSummary = %q, want empty", claims.RoleSummary)
	}
	if len(claims.Roles) != 0 {
		t.Errorf("Roles must be empty when role_summary is empty, got %v", claims.Roles)
	}
}

// Bucket 4: Roles []string is the native typed claim shape; the validator
// MUST recognise the `roles` JSON array claim and populate Claims.Roles +
// derive Claims.RoleSummary as the `+`-joined string.
func TestValidate_RolesArrayNativeShape(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	claims := validClaims(now, map[string]any{
		"roles": []any{"learner", "author", "instructor"},
	})
	// Remove the legacy field so we exercise the array-only path.
	delete(claims, "role_summary")
	tok := signHS256(t, testSigner, claims)
	got, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got.Roles) != 3 {
		t.Fatalf("Roles len = %d, want 3", len(got.Roles))
	}
	want := []string{"learner", "author", "instructor"}
	for i, r := range want {
		if got.Roles[i] != r {
			t.Errorf("Roles[%d] = %q want %q", i, got.Roles[i], r)
		}
	}
	if got.RoleSummary != "learner+author+instructor" {
		t.Errorf("RoleSummary derived = %q, want learner+author+instructor", got.RoleSummary)
	}
}

// Bucket 4: legacy role_summary string fallback — JWTs minted before the
// roles[] migration still parse cleanly with Roles populated by splitting
// the `+`-joined string. This keeps the validator backwards-compatible
// during the cutover.
func TestValidate_LegacyRoleSummaryStringFallback(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{
		"role_summary": "learner+author",
	}))
	got, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got.Roles) != 2 {
		t.Fatalf("Roles len = %d, want 2 (parsed from legacy role_summary)", len(got.Roles))
	}
	if got.Roles[0] != "learner" || got.Roles[1] != "author" {
		t.Errorf("Roles = %v, want [learner, author]", got.Roles)
	}
}

// Bucket 4: array wins over legacy string — both present, array takes
// precedence per the validator contract.
func TestValidate_RolesArrayWinsOverLegacyString(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	tok := signHS256(t, testSigner, validClaims(now, map[string]any{
		"roles":        []any{"tenant_admin"},
		"role_summary": "learner+author", // legacy hint, ignored
	}))
	got, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got.Roles) != 1 || got.Roles[0] != "tenant_admin" {
		t.Errorf("expected roles=[tenant_admin] when array wins, got %v", got.Roles)
	}
	if got.RoleSummary != "tenant_admin" {
		t.Errorf("RoleSummary derived = %q, want tenant_admin", got.RoleSummary)
	}
}

// Bucket 4: empty roles array still parses (no error). Same posture as
// empty role_summary string.
func TestValidate_EmptyRolesArrayAccepted(t *testing.T) {
	t.Parallel()
	v, _ := chorasession.NewValidator(testSigner, testIssuer, testAudience)
	now := time.Now().UTC()
	claims := validClaims(now, map[string]any{
		"roles": []any{},
	})
	delete(claims, "role_summary")
	tok := signHS256(t, testSigner, claims)
	got, err := v.Validate(tok)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if len(got.Roles) != 0 {
		t.Errorf("Roles must be empty, got %v", got.Roles)
	}
}

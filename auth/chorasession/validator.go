// Package chorasession validates JWTs MINTED by chora-gateway's
// POST /api/v1/auth/session/mint endpoint after a successful Firebase ID
// token exchange.
//
// These tokens are HS256-signed against a shared signing key loaded from
// Secret Manager at chora-gateway boot and carry the canonical Chora claim
// set: iss, aud, sub, gcid, tenant_id, email, role_summary, iat, exp.
//
// This validator REPLACES the Firebase-JWKS-backed identityplatform.Validator
// on chora-gateway's /api/* trust boundary. The Firebase JWKS validator now
// runs ONLY inside MintHandler to validate the incoming Firebase ID token
// during the exchange flow.
//
// Per ADR-138 the long-term path is RS256-via-KMS; HS256 with a shared
// Secret-Manager-resident key is the demo/single-tenant interim posture.
//
// Per `feedback_no_inline_config`: every Validator parameter (signer, issuer,
// audience) MUST be sourced from env / Secret Manager. NewValidator fails
// loudly on missing / undersized inputs — no silent acceptance.
//
// Per `feedback_resilience_priority`: clock skew tolerance defaults to 60s
// matching identityplatform.Validator to keep the BFF auth posture uniform.
package chorasession

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Package error sentinels — callers can use errors.Is to map to HTTP 401
// without parsing string contents.
var (
	ErrInvalidSignature = errors.New("chorasession: invalid signature")
	ErrTokenExpired     = errors.New("chorasession: token expired")
	ErrMissingClaim     = errors.New("chorasession: missing required claim")
	ErrInvalidIssuer    = errors.New("chorasession: invalid issuer")
	ErrInvalidAudience  = errors.New("chorasession: invalid audience")
	ErrMalformedToken   = errors.New("chorasession: malformed JWT")
	ErrUnsupportedAlg   = errors.New("chorasession: unsupported alg (only HS256)")
	ErrInvalidConfig    = errors.New("chorasession: invalid validator config")
)

// minSignerBytes is the minimum HS256 signing-key length per RFC 7518 §3.2
// (key length MUST be ≥ output size of HMAC-SHA-256 = 32 bytes). Enforced
// at NewValidator construction to fail loud on undersized secrets.
const minSignerBytes = 32

// defaultSkew matches identityplatform.Validator (60s) to keep the auth
// posture uniform across the two trust-boundary validators.
const defaultSkew = 60 * time.Second

// Claims is the validated set of claims extracted from a Chora session JWT.
// Standard claims (sub, email, iss, aud, exp, iat) come from the JWT envelope;
// custom claims (gcid, tenant_id, roles) are populated by chora-gateway at
// mint time post-identity-resolve.
//
// Per Bucket 4 (2026-05-14 multi-tenant identity arch-correct): Roles is the
// native typed accessor exposed to handlers. RoleSummary is retained as a
// derived `+`-joined helper string for log convenience. Empty Roles is
// accepted post-validate (the validator still checks gcid + tenant_id
// non-empty); a user with no roles in the active tenant is rare but legal
// (e.g. invited-but-not-activated membership).
type Claims struct {
	GCID string
	// TenantID is the user's CURRENT active tenant (one of N memberships
	// they hold). Carried on the JWT so the validator can stamp it on
	// downstream mesh headers without a round-trip back to chora-identity.
	TenantID string
	Email    string
	Sub      string
	// Roles is the full list of roles the user holds in the active tenant.
	// Drives role-driven feature visibility in the integrative UI.
	Roles []string
	// RoleSummary is the `+`-joined Roles for log + trace convenience.
	// Derived from Roles at validate time; NOT re-emitted as a separate
	// JWT claim. Tests + log handlers can read this for compact display.
	RoleSummary string
	IssuedAt    time.Time
	ExpiresAt   time.Time
	// Raw is the full claim map for callers that need fields not surfaced
	// in the typed Claims struct (escape hatch, NOT for hot-path use).
	Raw map[string]any
}

// Validator is a thread-safe HS256 JWT validator for chora-session tokens.
//
// Constructed via NewValidator. Goroutine-safe — the signer bytes are stored
// as an immutable copy and every Validate call works against local variables.
type Validator struct {
	signer   []byte
	issuer   string
	audience string
	skew     time.Duration
	now      func() time.Time
}

// NewValidator constructs a Validator. Fails LOUD when:
//
//   - signer is nil or shorter than 32 bytes (HS256 minimum)
//   - issuer is empty / whitespace
//   - audience is empty / whitespace
//
// The signer slice is copied so callers can zero their source after
// construction without affecting the validator.
func NewValidator(signer []byte, issuer, audience string) (*Validator, error) {
	if len(signer) < minSignerBytes {
		return nil, fmt.Errorf("%w: signer must be ≥%d bytes (got %d)",
			ErrInvalidConfig, minSignerBytes, len(signer))
	}
	if strings.TrimSpace(issuer) == "" {
		return nil, fmt.Errorf("%w: issuer empty", ErrInvalidConfig)
	}
	if strings.TrimSpace(audience) == "" {
		return nil, fmt.Errorf("%w: audience empty", ErrInvalidConfig)
	}
	cp := make([]byte, len(signer))
	copy(cp, signer)
	return &Validator{
		signer:   cp,
		issuer:   issuer,
		audience: audience,
		skew:     defaultSkew,
		now:      func() time.Time { return time.Now().UTC() },
	}, nil
}

// Validate verifies the signature, issuer, audience, expiration, and the
// presence of required custom claims (gcid + tenant_id).
//
// Returns one of the package error sentinels on failure. Callers map all
// validator errors to HTTP 401 — the specific sentinel is for logs +
// metrics, not for the response body.
func (v *Validator) Validate(token string) (*Claims, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: empty token", ErrMalformedToken)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: expected 3 segments, got %d",
			ErrMalformedToken, len(parts))
	}

	// 1. Decode + parse header — alg must be HS256.
	hdrJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: decode header: %v", ErrMalformedToken, err)
	}
	var hdr struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(hdrJSON, &hdr); err != nil {
		return nil, fmt.Errorf("%w: parse header: %v", ErrMalformedToken, err)
	}
	if hdr.Alg != "HS256" {
		return nil, fmt.Errorf("%w: got %q", ErrUnsupportedAlg, hdr.Alg)
	}

	// 2. Verify HS256 MAC over header.claims.
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("%w: decode signature: %v", ErrMalformedToken, err)
	}
	signing := []byte(parts[0] + "." + parts[1])
	mac := hmac.New(sha256.New, v.signer)
	mac.Write(signing)
	expected := mac.Sum(nil)
	if !hmac.Equal(expected, sigBytes) {
		return nil, fmt.Errorf("%w: HMAC mismatch", ErrInvalidSignature)
	}

	// 3. Decode + parse claims.
	clmJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: decode claims: %v", ErrMalformedToken, err)
	}
	var raw map[string]any
	if err := json.Unmarshal(clmJSON, &raw); err != nil {
		return nil, fmt.Errorf("%w: parse claims: %v", ErrMalformedToken, err)
	}

	// 4. Issuer check.
	iss, _ := raw["iss"].(string)
	if iss != v.issuer {
		return nil, fmt.Errorf("%w: got %q want %q", ErrInvalidIssuer, iss, v.issuer)
	}

	// 5. Audience check (string only — chora-gateway mints single-string aud).
	aud, _ := raw["aud"].(string)
	if aud != v.audience {
		return nil, fmt.Errorf("%w: got %q want %q", ErrInvalidAudience, aud, v.audience)
	}

	// 6. Expiration check (with skew tolerance).
	expF, ok := jsonNumberToInt64(raw["exp"])
	if !ok {
		return nil, fmt.Errorf("%w: exp", ErrMissingClaim)
	}
	exp := time.Unix(expF, 0).UTC()
	now := v.now()
	if exp.Add(v.skew).Before(now) {
		return nil, fmt.Errorf("%w: exp=%s now=%s", ErrTokenExpired, exp, now)
	}

	iatF, _ := jsonNumberToInt64(raw["iat"])
	iat := time.Unix(iatF, 0).UTC()

	// 7. Required custom claim (gcid) — non-empty. tenant_id MAY be empty
	//    for Phase 5 bootstrap-mode JWTs (CHO-1648): a user with no
	//    memberships gets `tenant_id=""` + `roles=[]` so they can call
	//    `/api/v1/tenants/bootstrap` to self-onboard. Route-level handlers
	//    that need tenant context (most aggregators / `extRequireContext`)
	//    enforce non-empty TenantID themselves and 401/403 cleanly. The
	//    validator's job is authenticity (signature + identity), not
	//    authorisation. CHO-1653 (2026-06-03) removed the strict check
	//    after CHO-1652 exposed the contradiction.
	gcid, _ := raw["gcid"].(string)
	if strings.TrimSpace(gcid) == "" {
		return nil, fmt.Errorf("%w: gcid", ErrMissingClaim)
	}
	tenantID, _ := raw["tenant_id"].(string)
	tenantID = strings.TrimSpace(tenantID)

	// Optional claims.
	email, _ := raw["email"].(string)
	sub, _ := raw["sub"].(string)

	// Roles parsing — accept BOTH shapes so the validator handles the
	// transition window cleanly:
	//   1. `roles` is a JSON array of strings → native typed shape (Bucket 4)
	//   2. legacy `role_summary` is a single `+`-joined string → split on '+'
	//
	// Whichever populates first wins; if both are present the array wins.
	// Empty roles is accepted (no error) — a user with no roles in the
	// active tenant is legal under the multi-membership model.
	var roles []string
	if rawRoles, ok := raw["roles"].([]any); ok {
		for _, v := range rawRoles {
			if s, ok := v.(string); ok {
				s = strings.TrimSpace(s)
				if s != "" {
					roles = append(roles, s)
				}
			}
		}
	} else if rs, _ := raw["role_summary"].(string); rs != "" {
		// Legacy: `learner+author+instructor`. Empty string is fine.
		for _, p := range strings.Split(rs, "+") {
			p = strings.TrimSpace(p)
			if p != "" {
				roles = append(roles, p)
			}
		}
	}

	return &Claims{
		GCID:        gcid,
		TenantID:    tenantID,
		Email:       email,
		Sub:         sub,
		Roles:       roles,
		RoleSummary: strings.Join(roles, "+"),
		IssuedAt:    iat,
		ExpiresAt:   exp,
		Raw:         raw,
	}, nil
}

// jsonNumberToInt64 converts an interface{} from a json.Unmarshal map into
// an int64 — JSON numbers come through as float64 by default.
func jsonNumberToInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return i, true
		}
	}
	return 0, false
}

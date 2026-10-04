// Package identityplatform validates JWTs minted by GCP Identity Platform
// (Firebase Auth) for the chora-bff-gateway and any other Chora service that
// sits at the trust boundary.
//
// Per S3.6 (Identity Platform Blocking Function flow):
//
//  1. End-user authenticates via Google/Microsoft/Singpass/etc → Identity
//     Platform mints + signs a JWT with custom claims (gcid, tenant_id,
//     kyc_status, role_summary).
//  2. chora-web stores the token + sends it to backend with
//     `Authorization: Bearer <jwt>`.
//  3. chora-bff-gateway validates the JWT (signature + audience + expiration)
//     using THIS lib, extracts claims, and forwards to backend services with
//     mTLS-bound metadata via Cloud Service Mesh.
//
// Identity Platform = claim issuer.
// chora-identity = claim minter (sets custom claims via Admin SDK).
// chora-bff-gateway = token validator at trust boundary (this lib).
//
// All endpoints + audience values come from env (IDP_ISSUER_URL, IDP_AUDIENCE,
// IDP_JWKS_URL) per .claude/skills/secrets-and-env/SKILL.md — no inline.
//
// JWKS is fetched from the issuer's discovery URL once at startup and cached
// in-memory; consumers should periodically rotate-restart (or accept the
// stale-cache risk during JWT signing-key rotation). For Firebase Auth /
// Identity Platform, key rotation is daily, so a per-process cache lifetime
// of ~1h is appropriate; production wiring may add explicit refresh.
package identityplatform

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Boot-time JWKS retry-backoff defaults (Option B1 per 28525e1c Decision 2 +
// ae7efd7f resilience-priority directive).
//
// Rationale: chora-gateway-fb5c9dffc-hdnbh crash-looped 2026-05-16 because
// securetoken.google.com discovery endpoint timed out at cold start (context
// deadline exceeded) and NewValidator returned ErrJWKSFetch synchronously.
// Per `feedback_resilience_priority`, "fail at first request if persistent"
// is the correct invariant — not "fail at boot on transient external API".
// The retry-backoff loop preserves fail-loud (ErrJWKSFetch after the full
// window) while tolerating the transient flake class that bit the rollout.
//
// Defaults sized to absorb a ~15s discovery cold start without exceeding the
// 90s CHORA_BOOTSTRAP_TIMEOUT_SECONDS production budget. Each attempt also
// rides v.cfg.HTTPClient.Timeout (default 5s); total worst-case wall time is
// ≈ 6×5s (attempt budget) + (0.5+1+2+4+8)s (backoff) ≈ 45.5s.
// ─────────────────────────────────────────────────────────────────────────────

const (
	jwksRetryAttempts    = 6
	jwksRetryBaseDelay   = 500 * time.Millisecond
	jwksRetryMaxDelay    = 8 * time.Second
	jwksRetryTotalBudget = 30 * time.Second
)

// ─────────────────────────────────────────────────────────────────────────────
// Errors
// ─────────────────────────────────────────────────────────────────────────────

var (
	ErrInvalidConfig    = errors.New("identityplatform: invalid config")
	ErrMalformedToken   = errors.New("identityplatform: malformed JWT")
	ErrInvalidSignature = errors.New("identityplatform: invalid signature")
	ErrTokenExpired     = errors.New("identityplatform: token expired")
	ErrInvalidIssuer    = errors.New("identityplatform: invalid issuer")
	ErrInvalidAudience  = errors.New("identityplatform: invalid audience")
	ErrUnknownKey       = errors.New("identityplatform: unknown signing key (kid)")
	ErrUnsupportedAlg   = errors.New("identityplatform: unsupported alg")
	ErrJWKSFetch        = errors.New("identityplatform: jwks fetch failed")
)

// ─────────────────────────────────────────────────────────────────────────────
// Config + Validator
// ─────────────────────────────────────────────────────────────────────────────

// Config holds the JWT validator wiring values. Both fields are mandatory.
type Config struct {
	// IssuerURL is the Identity Platform issuer (= the iss claim value AND the
	// base URL for the OIDC discovery document at /.well-known/openid-configuration).
	IssuerURL string

	// Audience is the expected aud claim value, typically `chora-{env}` (chora-dev,
	// chora-staging, chora-prod). Must match the audience the chora-web SPA
	// requests when it hands the token to chora-bff-gateway.
	Audience string

	// HTTPClient is optional; defaults to a 5-second-timeout client for the
	// JWKS fetch.
	HTTPClient *http.Client

	// Now is optional; defaults to time.Now. Test-friendly seam.
	Now func() time.Time

	// AllowedSkew is optional; defaults to 60 seconds clock skew for exp/iat.
	AllowedSkew time.Duration
}

// Validator is a thread-safe JWT validator with an in-memory JWKS cache.
//
// Resilience properties (per the production directive):
//
//   - Eager startup fetch with fail-closed: if the JWKS cannot be
//     fetched at startup, NewValidator returns an error rather than a
//     fail-open validator.
//   - Last-known-good fallback: refreshJWKS only writes to v.keys on
//     success. A transient 5xx leaves the cache intact; subsequent
//     ValidateJWT calls keep working with the cached keys until the
//     refresh succeeds or the keys legitimately rotate.
//   - Refresh flood guard: when a token bears an unknown kid, refresh
//     is throttled to at most one in-flight call (sync.Once-style)
//     PLUS a cooldown so a misbehaving client cannot DoS the IdP.
type Validator struct {
	cfg    Config
	mu     sync.RWMutex
	keys   map[string]any // kid → *rsa.PublicKey | *ecdsa.PublicKey
	loaded bool

	// refresh-flood guard state
	refreshMu       sync.Mutex
	lastRefreshTry  time.Time
	refreshCooldown time.Duration
	inFlightRefresh bool
}

// NewValidator constructs a Validator and eagerly fetches the JWKS at startup
// with exponential-backoff retry (Option B1 per 28525e1c Decision 2). The
// constructor tolerates transient JWKS endpoint failures (5xx, network
// timeouts, DNS blips) by retrying up to `jwksRetryAttempts` times with
// exponentially increasing delay capped at `jwksRetryMaxDelay`. Total wall
// budget is capped at `jwksRetryTotalBudget`.
//
// Returns ErrInvalidConfig if Config.IssuerURL or Config.Audience are blank.
// Returns ErrJWKSFetch wrapping the final attempt's error if the JWKS
// endpoint is unreachable across the entire backoff window (fail-loud at
// boot after retry exhaustion — never fail-open).
func NewValidator(cfg Config) (*Validator, error) {
	if strings.TrimSpace(cfg.IssuerURL) == "" {
		return nil, fmt.Errorf("%w: IssuerURL empty", ErrInvalidConfig)
	}
	if strings.TrimSpace(cfg.Audience) == "" {
		return nil, fmt.Errorf("%w: Audience empty", ErrInvalidConfig)
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = func() time.Time { return time.Now().UTC() }
	}
	if cfg.AllowedSkew == 0 {
		cfg.AllowedSkew = 60 * time.Second
	}
	v := &Validator{
		cfg:             cfg,
		keys:            make(map[string]any),
		refreshCooldown: 30 * time.Second, // flood guard — at most one refresh per 30s when keys are populated
	}
	bootCtx, cancel := context.WithTimeout(context.Background(), jwksRetryTotalBudget)
	defer cancel()
	if err := v.refreshJWKSWithBackoff(bootCtx); err != nil {
		return nil, err
	}
	return v, nil
}

// refreshJWKSWithBackoff drives refreshJWKS through up to jwksRetryAttempts
// retries with exponential-backoff (jwksRetryBaseDelay × 2^attempt, capped
// at jwksRetryMaxDelay). The loop respects ctx.Done() — a cancelled context
// aborts the loop and surfaces the wrapped error.
//
// Each failed attempt is logged at INFO via slog so SRE has cold-start
// visibility into transient flakes (e.g., NAT remap, securetoken.google.com
// discovery latency spikes) without DEBUG-level noise on the happy path.
//
// Returns nil on first success. Returns the wrapped final ErrJWKSFetch
// (with attempt count + total elapsed) if all attempts fail.
func (v *Validator) refreshJWKSWithBackoff(ctx context.Context) error {
	start := time.Now()
	var lastErr error
	delay := jwksRetryBaseDelay
	for attempt := 1; attempt <= jwksRetryAttempts; attempt++ {
		if ctx.Err() != nil {
			return fmt.Errorf("%w: backoff cancelled after %d attempts (%s): %v",
				ErrJWKSFetch, attempt-1, time.Since(start), ctx.Err())
		}
		err := v.refreshJWKS(ctx)
		if err == nil {
			if attempt > 1 {
				slog.Info("identityplatform: JWKS recovered after retry",
					"issuer", v.cfg.IssuerURL,
					"attempt", attempt,
					"total_elapsed", time.Since(start).String())
			}
			return nil
		}
		lastErr = err
		if attempt < jwksRetryAttempts {
			slog.Info("identityplatform: JWKS fetch failed; retry-backoff scheduled",
				"issuer", v.cfg.IssuerURL,
				"attempt", attempt,
				"max_attempts", jwksRetryAttempts,
				"next_delay", delay.String(),
				"err", err.Error())
			// Sleep with context awareness so ctx-cancel aborts promptly.
			t := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return fmt.Errorf("%w: backoff cancelled mid-sleep after %d attempts (%s): %v",
					ErrJWKSFetch, attempt, time.Since(start), ctx.Err())
			case <-t.C:
			}
			// Exponential backoff with cap. No jitter — boot path is
			// process-local; jitter would only matter for thundering-herd
			// behaviour across many simultaneous validators, which is not
			// the chora-gateway topology (1 replica per service-instance).
			delay *= 2
			if delay > jwksRetryMaxDelay {
				delay = jwksRetryMaxDelay
			}
		}
	}
	return fmt.Errorf("%w: %d attempts exhausted over %s: %v",
		ErrJWKSFetch, jwksRetryAttempts, time.Since(start), lastErr)
}

// ─────────────────────────────────────────────────────────────────────────────
// Claims
// ─────────────────────────────────────────────────────────────────────────────

// Claims is the validated set of claims extracted from a JWT. Custom claims
// (gcid, tenant_id, kyc_status, role_summary) are populated from Identity
// Platform's custom-claims feature; standard claims (sub, email, iss, aud,
// exp, iat) come from the OIDC envelope.
type Claims struct {
	GCID        string         `json:"gcid"`
	TenantID    string         `json:"tenant_id"`
	KYCStatus   string         `json:"kyc_status"`
	RoleSummary map[string]any `json:"role_summary"`
	Email       string         `json:"email"`
	Sub         string         `json:"sub"`
	Issuer      string         `json:"iss"`
	Audience    string         `json:"-"` // single string normalised
	IssuedAt    time.Time      `json:"-"`
	ExpiresAt   time.Time      `json:"-"`
	Raw         map[string]any `json:"-"` // full claim set for callers
}

// rawClaims is the on-wire representation; we coerce to Claims in ValidateJWT.
type rawClaims struct {
	GCID        string         `json:"gcid"`
	TenantID    string         `json:"tenant_id"`
	KYCStatus   string         `json:"kyc_status"`
	RoleSummary map[string]any `json:"role_summary"`
	Email       string         `json:"email"`
	Sub         string         `json:"sub"`
	Issuer      string         `json:"iss"`
	Audience    any            `json:"aud"` // string or []string per RFC 7519
	IssuedAt    int64          `json:"iat"`
	ExpiresAt   int64          `json:"exp"`
}

// ─────────────────────────────────────────────────────────────────────────────
// ValidateJWT — core entrypoint.
// ─────────────────────────────────────────────────────────────────────────────

// ValidateJWT verifies the signature against the cached JWKS, checks
// audience + issuer + expiration, and returns the typed Claims.
//
// On any failure returns one of the package error sentinels (suitable for
// errors.Is matching at the call site). The middleware uses these to map to
// HTTP 401.
func (v *Validator) ValidateJWT(ctx context.Context, token string) (*Claims, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: empty token", ErrMalformedToken)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: expected 3 segments, got %d", ErrMalformedToken, len(parts))
	}

	hdrJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("%w: decode header: %v", ErrMalformedToken, err)
	}
	clmJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: decode claims: %v", ErrMalformedToken, err)
	}
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("%w: decode signature: %v", ErrMalformedToken, err)
	}

	var hdr struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(hdrJSON, &hdr); err != nil {
		return nil, fmt.Errorf("%w: parse header: %v", ErrMalformedToken, err)
	}
	pubKey, err := v.lookupKey(ctx, hdr.Kid)
	if err != nil {
		return nil, err
	}
	signing := []byte(parts[0] + "." + parts[1])
	if err := verifySignature(hdr.Alg, pubKey, signing, sigBytes); err != nil {
		return nil, err
	}

	var raw rawClaims
	if err := json.Unmarshal(clmJSON, &raw); err != nil {
		return nil, fmt.Errorf("%w: parse claims: %v", ErrMalformedToken, err)
	}
	// Issuer enforcement.
	if raw.Issuer != v.cfg.IssuerURL {
		return nil, fmt.Errorf("%w: got %q want %q", ErrInvalidIssuer, raw.Issuer, v.cfg.IssuerURL)
	}
	// Audience enforcement (string or []string).
	if !audienceMatches(raw.Audience, v.cfg.Audience) {
		return nil, fmt.Errorf("%w: got %v want %q", ErrInvalidAudience, raw.Audience, v.cfg.Audience)
	}
	// Expiration enforcement.
	now := v.cfg.Now()
	exp := time.Unix(raw.ExpiresAt, 0).UTC()
	if exp.Add(v.cfg.AllowedSkew).Before(now) {
		return nil, fmt.Errorf("%w: exp=%s now=%s", ErrTokenExpired, exp, now)
	}

	// Capture raw claim map for callers that want to inspect un-typed claims.
	var rawMap map[string]any
	_ = json.Unmarshal(clmJSON, &rawMap)

	return &Claims{
		GCID:        raw.GCID,
		TenantID:    raw.TenantID,
		KYCStatus:   raw.KYCStatus,
		RoleSummary: raw.RoleSummary,
		Email:       raw.Email,
		Sub:         raw.Sub,
		Issuer:      raw.Issuer,
		Audience:    v.cfg.Audience,
		IssuedAt:    time.Unix(raw.IssuedAt, 0).UTC(),
		ExpiresAt:   exp,
		Raw:         rawMap,
	}, nil
}

func audienceMatches(audClaim any, expected string) bool {
	switch v := audClaim.(type) {
	case string:
		return v == expected
	case []any:
		for _, x := range v {
			if s, ok := x.(string); ok && s == expected {
				return true
			}
		}
	}
	return false
}

// ─────────────────────────────────────────────────────────────────────────────
// Signature verification (RS256 / ES256)
// ─────────────────────────────────────────────────────────────────────────────

func verifySignature(alg string, key any, signing, sig []byte) error {
	hashed := sha256.Sum256(signing)
	switch alg {
	case "RS256":
		rk, ok := key.(*rsa.PublicKey)
		if !ok {
			return fmt.Errorf("%w: RS256 needs RSA key", ErrInvalidSignature)
		}
		if err := rsa.VerifyPKCS1v15(rk, crypto.SHA256, hashed[:], sig); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidSignature, err)
		}
		return nil
	case "ES256":
		ek, ok := key.(*ecdsa.PublicKey)
		if !ok {
			return fmt.Errorf("%w: ES256 needs ECDSA key", ErrInvalidSignature)
		}
		// ES256 r||s concatenation — each 32 bytes.
		if len(sig) != 64 {
			return fmt.Errorf("%w: ES256 sig length=%d, expected 64", ErrInvalidSignature, len(sig))
		}
		r := new(big.Int).SetBytes(sig[:32])
		s := new(big.Int).SetBytes(sig[32:])
		if !ecdsa.Verify(ek, hashed[:], r, s) {
			return ErrInvalidSignature
		}
		return nil
	default:
		return fmt.Errorf("%w: %s", ErrUnsupportedAlg, alg)
	}
}

// silence unused-import lint for sha256 if RS256 path never executes.
var _ = sha256.Size

// ─────────────────────────────────────────────────────────────────────────────
// JWKS fetch + cache
// ─────────────────────────────────────────────────────────────────────────────

// JWKS is the JSON Web Key Set wire format.
type jwks struct {
	Keys []jwksKey `json:"keys"`
}

type jwksKey struct {
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	// RSA
	N string `json:"n,omitempty"`
	E string `json:"e,omitempty"`
	// EC
	Crv string `json:"crv,omitempty"`
	X   string `json:"x,omitempty"`
	Y   string `json:"y,omitempty"`
}

type oidcDiscovery struct {
	JWKSURI string `json:"jwks_uri"`
	Issuer  string `json:"issuer"`
}

// refreshJWKS pulls /.well-known/openid-configuration → jwks_uri → JWKS doc
// and replaces the cache.
func (v *Validator) refreshJWKS(ctx context.Context) error {
	disc, err := v.fetchDiscovery(ctx)
	if err != nil {
		return err
	}
	if disc.JWKSURI == "" {
		return fmt.Errorf("%w: discovery had no jwks_uri", ErrJWKSFetch)
	}

	doc, err := v.fetchJWKS(ctx, disc.JWKSURI)
	if err != nil {
		return err
	}

	keys := make(map[string]any, len(doc.Keys))
	for _, k := range doc.Keys {
		switch k.Kty {
		case "RSA":
			pk, perr := parseRSAJWK(k)
			if perr != nil {
				continue
			}
			keys[k.Kid] = pk
		case "EC":
			pk, perr := parseECJWK(k)
			if perr != nil {
				continue
			}
			keys[k.Kid] = pk
		}
	}
	v.mu.Lock()
	v.keys = keys
	v.loaded = true
	v.mu.Unlock()
	return nil
}

func (v *Validator) fetchDiscovery(ctx context.Context) (*oidcDiscovery, error) {
	url := strings.TrimRight(v.cfg.IssuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJWKSFetch, err)
	}
	resp, err := v.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJWKSFetch, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: discovery status %d", ErrJWKSFetch, resp.StatusCode)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	var d oidcDiscovery
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, fmt.Errorf("%w: discovery decode: %v", ErrJWKSFetch, err)
	}
	return &d, nil
}

func (v *Validator) fetchJWKS(ctx context.Context, url string) (*jwks, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJWKSFetch, err)
	}
	resp, err := v.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJWKSFetch, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: jwks status %d", ErrJWKSFetch, resp.StatusCode)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var doc jwks
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%w: jwks decode: %v", ErrJWKSFetch, err)
	}
	return &doc, nil
}

func (v *Validator) lookupKey(ctx context.Context, kid string) (any, error) {
	v.mu.RLock()
	if k, ok := v.keys[kid]; ok {
		v.mu.RUnlock()
		return k, nil
	}
	cacheNonEmpty := len(v.keys) > 0
	v.mu.RUnlock()

	// Refresh flood guard: if the cache is non-empty AND we attempted
	// a refresh within the cooldown window, do NOT hit the IdP again.
	// Surface ErrUnknownKey directly — the cached keys are still valid
	// for known kids, so this only affects rotation lag.
	v.refreshMu.Lock()
	now := time.Now()
	if cacheNonEmpty && !v.lastRefreshTry.IsZero() && now.Sub(v.lastRefreshTry) < v.refreshCooldown {
		v.refreshMu.Unlock()
		return nil, fmt.Errorf("%w: %q (refresh cooldown active)", ErrUnknownKey, kid)
	}
	// Single-flight: if another goroutine is currently refreshing, wait
	// for it to complete (briefly) rather than firing a parallel fetch.
	if v.inFlightRefresh {
		v.refreshMu.Unlock()
		// Poll briefly for completion — if the in-flight refresh adds
		// the kid we care about, return it; otherwise surface
		// ErrUnknownKey.
		v.mu.RLock()
		defer v.mu.RUnlock()
		if k, ok := v.keys[kid]; ok {
			return k, nil
		}
		return nil, fmt.Errorf("%w: %q (concurrent refresh in flight)", ErrUnknownKey, kid)
	}
	v.inFlightRefresh = true
	v.lastRefreshTry = now
	v.refreshMu.Unlock()
	defer func() {
		v.refreshMu.Lock()
		v.inFlightRefresh = false
		v.refreshMu.Unlock()
	}()

	// Attempt refresh. On failure, last-known-good cache is preserved
	// (refreshJWKS only writes on success). Return the refresh error
	// only when the cache is empty (fail-closed at startup); otherwise
	// surface ErrUnknownKey so the caller treats it as auth failure
	// without losing access to known-good keys.
	if err := v.refreshJWKS(ctx); err != nil {
		if !cacheNonEmpty {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %q (refresh failed: %v)", ErrUnknownKey, kid, err)
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrUnknownKey, kid)
}

func parseRSAJWK(k jwksKey) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, err
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
}

func parseECJWK(k jwksKey) (*ecdsa.PublicKey, error) {
	curve := elliptic.P256()
	if k.Crv != "P-256" {
		return nil, fmt.Errorf("ec curve %q unsupported", k.Crv)
	}
	xb, err := base64.RawURLEncoding.DecodeString(k.X)
	if err != nil {
		return nil, err
	}
	yb, err := base64.RawURLEncoding.DecodeString(k.Y)
	if err != nil {
		return nil, err
	}
	return &ecdsa.PublicKey{Curve: curve, X: new(big.Int).SetBytes(xb), Y: new(big.Int).SetBytes(yb)}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Middleware — drops Claims into request context.
// ─────────────────────────────────────────────────────────────────────────────

type ctxKey struct{}

// ClaimsFromContext returns the validated claims from context, if any.
func ClaimsFromContext(ctx context.Context) (*Claims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*Claims)
	return c, ok
}

// Middleware returns an http.Handler middleware that validates the
// `Authorization: Bearer <jwt>` header and attaches the parsed Claims to the
// request context. On failure it writes 401 and short-circuits.
func (v *Validator) Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok, ok := extractBearer(r.Header.Get("Authorization"))
			if !ok {
				w.Header().Set("WWW-Authenticate", `Bearer realm="chora"`)
				http.Error(w, "Authorization: Bearer required", http.StatusUnauthorized)
				return
			}
			claims, err := v.ValidateJWT(r.Context(), tok)
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="chora", error="invalid_token"`)
				http.Error(w, "invalid token", http.StatusUnauthorized)
				return
			}
			ctx := context.WithValue(r.Context(), ctxKey{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func extractBearer(authz string) (string, bool) {
	authz = strings.TrimSpace(authz)
	if authz == "" {
		return "", false
	}
	parts := strings.SplitN(authz, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	tok := strings.TrimSpace(parts[1])
	if tok == "" {
		return "", false
	}
	return tok, true
}

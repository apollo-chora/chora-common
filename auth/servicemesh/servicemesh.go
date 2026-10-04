// Package servicemesh implements the Cloud-Service-Mesh-bound metadata
// propagation layer. Per S3.6 spec:
//
//	chora-bff-gateway → backend services: gRPC metadata `chora-gcid`,
//	`chora-tenant-id`, `chora-role-summary` (JSON). Backend services trust
//	mTLS-bound metadata (Cloud Service Mesh asserts caller identity).
//
// Trust model:
//
//   - chora-bff-gateway is the ONLY entry point for inbound user traffic.
//     It validates the Identity Platform JWT (via auth/identityplatform) and
//     converts the claims into mesh metadata.
//
//   - Cloud Service Mesh (managed Istio) provides mTLS with the built-in
//     mesh CA. Each backend service trusts the BFF-gateway service identity
//     at the mesh layer; the BFF cannot be spoofed.
//
//   - Backend services therefore TRUST `chora-gcid`/`chora-tenant-id`
//     metadata as long as they accept it ONLY from the BFF-mTLS peer. Outside
//     the mesh, this header is meaningless and must be stripped at the
//     ingress GCLB. This package does NOT enforce mesh boundary — that's
//     ingress-level config (see chora-infra/m10-networking).
//
// The package is HTTP-first (chora-bff-gateway is HTTP/JSON to chora-web,
// gRPC inward) — for gRPC propagation see the wrapping interceptor in the
// caller; this lib provides the canonical header / metadata names.
package servicemesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Canonical mesh-metadata header names.
const (
	HeaderGCID        = "chora-gcid"
	HeaderTenantID    = "chora-tenant-id"
	HeaderRoleSummary = "chora-role-summary"
	// HeaderUserRoles (Bucket 4, 2026-05-14) — comma-separated typed roles
	// list. Distinct from chora-role-summary which carries a JSON object
	// describing per-course role nuances (legacy schema). Backend services
	// CAN read this header for fast role-gate decisions without parsing
	// the JSON RoleSummary.
	HeaderUserRoles = "x-mesh-user-roles"
)

// MeshClaims is the set of identity claims propagated from BFF to backend
// over the service mesh. RoleSummary is deliberately a map[string]any (not a
// fixed schema) — per-domain backends decode the slice subset they need.
//
// Per Bucket 4 (2026-05-14): Roles []string carries the user's typed role
// set in the active tenant. Coexists with the legacy RoleSummary map[string]any
// during the transition window — downstream services consume either as needed.
type MeshClaims struct {
	GCID        string
	TenantID    string
	Roles       []string
	RoleSummary map[string]any
}

// MarshalToHeaders writes claims into HTTP headers using the canonical
// mesh-metadata names. RoleSummary is JSON-encoded; an empty role summary
// is omitted entirely (downstream code treats absence as "default learner").
// Roles[] is comma-separated under HeaderUserRoles per Bucket 4.
func MarshalToHeaders(c MeshClaims) http.Header {
	h := http.Header{}
	if strings.TrimSpace(c.GCID) != "" {
		h.Set(HeaderGCID, c.GCID)
	}
	if strings.TrimSpace(c.TenantID) != "" {
		h.Set(HeaderTenantID, c.TenantID)
	}
	if len(c.Roles) > 0 {
		// Trim + drop empties so downstream parsers never see leading/
		// trailing commas. Stable order — caller controls.
		cleaned := make([]string, 0, len(c.Roles))
		for _, r := range c.Roles {
			r = strings.TrimSpace(r)
			if r != "" {
				cleaned = append(cleaned, r)
			}
		}
		if len(cleaned) > 0 {
			h.Set(HeaderUserRoles, strings.Join(cleaned, ","))
		}
	}
	if len(c.RoleSummary) > 0 {
		b, err := json.Marshal(c.RoleSummary)
		if err == nil {
			h.Set(HeaderRoleSummary, string(b))
		}
	}
	return h
}

// UnmarshalFromHeaders extracts claims from the mesh-metadata headers.
// Missing GCID / TenantID returns a zero-value MeshClaims with no error;
// callers can use ClaimsFromContext() with HasClaims() to detect populated
// vs empty. Malformed RoleSummary JSON returns an error.
func UnmarshalFromHeaders(h http.Header) (*MeshClaims, error) {
	c := &MeshClaims{
		GCID:     strings.TrimSpace(h.Get(HeaderGCID)),
		TenantID: strings.TrimSpace(h.Get(HeaderTenantID)),
	}
	// Roles header (Bucket 4) — comma-separated. Empty header is fine.
	if ur := strings.TrimSpace(h.Get(HeaderUserRoles)); ur != "" {
		for _, p := range strings.Split(ur, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				c.Roles = append(c.Roles, p)
			}
		}
	}
	rs := strings.TrimSpace(h.Get(HeaderRoleSummary))
	if rs != "" {
		var parsed map[string]any
		if err := json.Unmarshal([]byte(rs), &parsed); err != nil {
			return nil, fmt.Errorf("servicemesh: malformed %s header: %w", HeaderRoleSummary, err)
		}
		c.RoleSummary = parsed
	}
	return c, nil
}

// HasClaims returns true when GCID + TenantID are both populated.
func (c *MeshClaims) HasClaims() bool {
	return c != nil && c.GCID != "" && c.TenantID != ""
}

// ─────────────────────────────────────────────────────────────────────────────
// Context propagation
// ─────────────────────────────────────────────────────────────────────────────

type ctxKey struct{}

// ClaimsFromContext retrieves the previously-attached MeshClaims, if any.
func ClaimsFromContext(ctx context.Context) (*MeshClaims, bool) {
	c, ok := ctx.Value(ctxKey{}).(*MeshClaims)
	return c, ok && c.HasClaims()
}

// WithClaims returns ctx with claims attached. Useful in tests.
func WithClaims(ctx context.Context, c *MeshClaims) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP middleware
// ─────────────────────────────────────────────────────────────────────────────

// Middleware extracts mesh-metadata headers from the inbound request and
// drops them into the request context. Failure to parse RoleSummary is
// logged via http.Error 400; absent headers pass through unchanged
// (backends that REQUIRE claims should chain RequireClaims after this).
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := UnmarshalFromHeaders(r.Header)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Even if claims are empty, attach an empty struct so downstream
		// code can call ClaimsFromContext consistently.
		ctx := WithClaims(r.Context(), c)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ErrNoClaims indicates required claims absent on the inbound request.
var ErrNoClaims = errors.New("servicemesh: no chora-gcid / chora-tenant-id headers")

// RequireClaims is a stricter middleware that rejects requests lacking GCID
// or TenantID with HTTP 401. Compose AFTER Middleware so the parse step has
// run already.
func RequireClaims(next http.Handler) http.Handler {
	return Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, ok := ClaimsFromContext(r.Context())
		if !ok || !c.HasClaims() {
			http.Error(w, ErrNoClaims.Error(), http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	}))
}

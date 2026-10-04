// Package rls is the repository-layer helper for Chora's Row-Level Security
// (RLS) tenant + per-user isolation across the 11 domain databases.
//
// Source-of-truth:
//   - .claude/skills/multi-tenant-rls/SKILL.md (always-loaded conventions)
//   - .claude/rules/ddd-enforcement.md ("HARD RULE: cross-database queries forbidden")
//   - docs/architecture-review-inputs-2026-05-07.md Tier 2 D7 + Tier 3
//
// Two-layer isolation across the platform:
//
//  1. Database-per-domain — different domains live in different databases;
//     cross-DB queries forbidden (events only).
//  2. Row-Level Security (RLS) per tenant within each domain database —
//     different tenants = filtered rows; cross-tenant queries blocked at
//     the DB level.
//
// Naming convention (LOCKED — do NOT change):
//
//	SET LOCAL chora.tenant_id = '<uuid>';      // mandatory for tenant-scoped
//	SET LOCAL chora.user_gcid = '<uuid>';      // mandatory for GCID-scoped
//	                                           // (per ADR-143 per-user KG)
//
// All migrations across services use `current_setting('chora.tenant_id', true)`
// — the helper MUST emit the same name. The legacy `app.current_tenant_id`
// name from older M1-M9 migrations is NOT the active convention; new
// services MUST use `chora.*`.
//
// PgBouncer-safety: this helper always emits `SET LOCAL`, never `SET`.
// `SET LOCAL` is transaction-scoped; `SET` would persist across the
// connection in PgBouncer transaction-pooling mode and leak across
// requests.
package rls

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apollo-chora/chora-common/tracing"
)

// CommandTag is a tx-result placeholder. Real adapters use pgx.CommandTag
// (or database/sql.Result); this package keeps a tiny mirror to avoid
// pulling pgx into the import graph for non-DB consumers (e.g., HTTP
// middleware that wires session vars before delegating to a repo).
type CommandTag struct {
	RowsAffected int64
}

// Execer is the minimal contract this helper needs from a transaction.
// Both pgx.Tx and database/sql.Tx satisfy a thin adapter that wraps Exec.
//
// Adapters in services/{service}/internal/adapter/repo/ should expose a
// thin wrapper:
//
//	type pgxExecer struct{ tx pgx.Tx }
//	func (p pgxExecer) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
//	    tag, err := p.tx.Exec(ctx, sql, args...)
//	    return rls.CommandTag{RowsAffected: tag.RowsAffected()}, err
//	}
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (CommandTag, error)
}

// Errors surfaced by this package.
var (
	// ErrNoTenantContext means the caller forgot to set tenant_id on ctx
	// before opening a transaction. Failing fast here is preferable to
	// running queries that RLS will silently zero-out.
	ErrNoTenantContext = errors.New("rls: tenant_id missing on context — use tracing.WithTenantID before calling")

	// ErrInvalidIdentifier means the tenant_id or gcid contains characters
	// that would be unsafe to interpolate into a SET LOCAL statement.
	// Even though SET LOCAL is parsed by Postgres (not the prepared-stmt
	// path), defence-in-depth rejects suspicious bytes before they hit
	// the wire.
	ErrInvalidIdentifier = errors.New("rls: identifier contains forbidden characters")
)

// ApplySession sets `chora.tenant_id` and (when present) `chora.user_gcid`
// session variables on the supplied transaction.
//
// Caller MUST already be inside a transaction (Begin → ApplySession →
// queries → Commit). SET LOCAL is transaction-scoped; if the caller emits
// it on a connection-pool sibling instead of a transaction, the value
// persists across the pool and leaks across requests — the named hard
// guarantee multi-tenant-rls/SKILL.md describes.
//
// Returns:
//   - ErrNoTenantContext when tenant_id is missing on ctx.
//   - ErrInvalidIdentifier when tenant_id or gcid contain unsafe bytes.
//   - the underlying Exec error when Postgres rejects the SET LOCAL.
func ApplySession(ctx context.Context, e Execer) error {
	tenantID := tracing.TenantIDFromContext(ctx)
	if tenantID == "" {
		return ErrNoTenantContext
	}
	if err := ValidateTenantID(tenantID); err != nil {
		return err
	}
	if _, err := e.Exec(ctx, fmt.Sprintf("SET LOCAL chora.tenant_id = '%s'", tenantID)); err != nil {
		return fmt.Errorf("rls: SET LOCAL chora.tenant_id failed: %w", err)
	}

	// user_gcid — set when a user context is present (tenant-only is fine for
	// non-user-scoped tables, so an empty gcid is not an error).
	if gcid := tracing.GCIDFromContext(ctx); gcid != "" {
		if err := ValidateGCID(gcid); err != nil {
			return err
		}
		if _, err := e.Exec(ctx, fmt.Sprintf("SET LOCAL chora.user_gcid = '%s'", gcid)); err != nil {
			return fmt.Errorf("rls: SET LOCAL chora.user_gcid failed: %w", err)
		}
	}

	// user_roles — the caller's mesh role set, for role-aware RLS policies (e.g.
	// chora_delivery.courses state-gating exempts admin/training-admin). Only set
	// when present; existing services that never carry roles leave the GUC unset
	// and their policies are unaffected (backward-compatible).
	if roles := tracing.UserRolesFromContext(ctx); roles != "" {
		if err := ValidateUserRoles(roles); err != nil {
			return err
		}
		if _, err := e.Exec(ctx, fmt.Sprintf("SET LOCAL chora.user_roles = '%s'", roles)); err != nil {
			return fmt.Errorf("rls: SET LOCAL chora.user_roles failed: %w", err)
		}
	}
	return nil
}

// RunInTx is the convenience runner: ApplySession, then invoke fn.
// Preferred entry point in repository methods so the SET LOCAL pair
// always lands before user queries. fn receives the same Execer.
func RunInTx(ctx context.Context, e Execer, fn func(context.Context, Execer) error) error {
	if err := ApplySession(ctx, e); err != nil {
		return err
	}
	return fn(ctx, e)
}

// ValidateTenantID rejects values that contain characters unsafe to
// interpolate into a SET LOCAL statement. Allowed:
//   - the literal sentinel "platform" (per envelope.proto: cross-tenant
//     platform-level events use tenant_id="platform")
//   - hex/UUID-shaped strings (alphanum + dash)
//
// Anything else is rejected. Postgres's UUID cast in the RLS policy
// (`current_setting('chora.tenant_id', true)::uuid`) would catch most
// invalid values, but rejecting them at the helper layer means we never
// emit a SET LOCAL with a payload an auditor would frown at.
func ValidateTenantID(id string) error {
	if id == "" {
		return ErrInvalidIdentifier
	}
	if strings.TrimSpace(id) != id {
		return ErrInvalidIdentifier
	}
	if id == "platform" {
		return nil
	}
	return validateSafeIdentifier(id)
}

// ValidateGCID is the same as ValidateTenantID without the "platform"
// sentinel — GCIDs (and AGIDs) are always UUIDv7-shaped. AGID detection
// (the `0197a*` prefix per ddd-enforcement aggregate-invariant #10) is
// NOT enforced here — domain code is the right layer for that rejection,
// not the RLS plumbing.
func ValidateGCID(id string) error {
	if id == "" {
		return ErrInvalidIdentifier
	}
	if strings.TrimSpace(id) != id {
		return ErrInvalidIdentifier
	}
	return validateSafeIdentifier(id)
}

// ValidateUserRoles rejects a role list containing characters unsafe to
// interpolate into a SET LOCAL statement. The mesh role list is a comma-joined
// set of role tokens (e.g. "instructor,training-admin"); allowed characters are
// alphanumerics, dash, underscore, and comma. The caller MUST pre-sanitise
// (lowercase, no spaces) — internal whitespace is rejected here as defence in
// depth. Empty is rejected (the caller only sets the GUC for a non-empty list).
func ValidateUserRoles(roles string) error {
	if roles == "" {
		return ErrInvalidIdentifier
	}
	if strings.TrimSpace(roles) != roles {
		return ErrInvalidIdentifier
	}
	for _, r := range roles {
		switch {
		case r >= '0' && r <= '9':
			continue
		case r >= 'a' && r <= 'z':
			continue
		case r >= 'A' && r <= 'Z':
			continue
		case r == '-' || r == '_' || r == ',':
			continue
		default:
			return ErrInvalidIdentifier
		}
	}
	return nil
}

// validateSafeIdentifier permits alphanum + dash only (matches a hex UUID
// formatted with dashes). Single-quote, semicolon, whitespace, control
// chars, double-quote — all rejected.
func validateSafeIdentifier(id string) error {
	for _, r := range id {
		switch {
		case r >= '0' && r <= '9':
			continue
		case r >= 'a' && r <= 'z':
			continue
		case r >= 'A' && r <= 'Z':
			continue
		case r == '-':
			continue
		default:
			return ErrInvalidIdentifier
		}
	}
	return nil
}

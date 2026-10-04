# rls — Repository-layer Row-Level Security helper

> Aligned with **Architecture Review locked 2026-05-07**.
> Sources: `.claude/skills/multi-tenant-rls/SKILL.md`, `.claude/rules/ddd-enforcement.md`, `docs/architecture-review-inputs-2026-05-07.md` Tier 2 D7 + Tier 3.

## Why

Chora has two-layer tenant isolation:

1. **Database-per-domain** (Tier 2 D7) — different domains live in different DBs; cross-DB queries forbidden (events only).
2. **Row-Level Security (RLS) per tenant** within each domain DB — different tenants = filtered rows; cross-tenant queries blocked at the DB level.

Every active service migration uses the canonical session-var names:

```sql
ALTER TABLE atom ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON atom
  FOR ALL USING (tenant_id = current_setting('chora.tenant_id', true)::uuid);
```

For per-user tables (per ADR-143 per-user Knowledge Graph) policies also reference `chora.user_gcid`:

```sql
CREATE POLICY user_isolation ON kg_user_map_clusters
  FOR ALL USING (user_gcid = current_setting('chora.user_gcid', true)::uuid);
```

The `rls` package is the **repository-layer entry point** that emits both `SET LOCAL` statements before every transaction's queries.

## Hard rule (PgBouncer-safety)

Always emit `SET LOCAL`, **never** `SET`. `SET LOCAL` is transaction-scoped; `SET` would persist across the connection in PgBouncer transaction-pooling mode and **leak across requests** — a critical multi-tenant security bug.

This package does the right thing automatically. Domain code never composes the SQL by hand.

## How to wire into a service repository

```go
package atomrepo

import (
    "context"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"

    "github.com/5007-Capstone/chora/libs/chora-go-common/rls"
)

// pgxExecer adapts pgx.Tx to rls.Execer.
type pgxExecer struct{ tx pgx.Tx }

func (p pgxExecer) Exec(ctx context.Context, sql string, args ...any) (rls.CommandTag, error) {
    tag, err := p.tx.Exec(ctx, sql, args...)
    return rls.CommandTag{RowsAffected: tag.RowsAffected()}, err
}

type Repository struct {
    pool *pgxpool.Pool
}

func (r *Repository) GetAtom(ctx context.Context, atomID string) (*Atom, error) {
    var atom *Atom

    err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
        return rls.RunInTx(ctx, pgxExecer{tx}, func(ctx context.Context, _ rls.Execer) error {
            return tx.QueryRow(ctx, `
                SELECT id, tenant_id, title
                FROM atom
                WHERE id = $1 AND deleted_at IS NULL`, atomID).
                Scan(&atom.ID, &atom.TenantID, &atom.Title)
            // RLS auto-filters tenant_id (and user_gcid for ADR-143 tables)
        })
    })
    return atom, err
}
```

Tenant ID flows in via context (set by HTTP middleware that decodes the JWT):

```go
import "github.com/5007-Capstone/chora/libs/chora-go-common/tracing"

ctx = tracing.WithTenantID(ctx, claims.TenantID)
ctx = tracing.WithGCID(ctx, claims.GCID)  // for per-user dual-scoped tables
```

## Errors

| Error | Cause | Fix |
|---|---|---|
| `ErrNoTenantContext` | `tenant_id` not set on ctx before opening txn | call `tracing.WithTenantID(ctx, ...)` in HTTP middleware before delegating to repo |
| `ErrInvalidIdentifier` | tenant_id / gcid contains unsafe chars | sanitize at JWT-claim parse; the helper rejects whitespace, single-quote, semicolon, control chars |
| wrapped Exec err | Postgres rejected the SET LOCAL | check Cloud SQL connectivity / migrations applied |

## Cross-tenant leak harness

`tests/rls-isolation-active/` runs the leak harness against every active service migration set. Build-tagged `integration`; run with:

```sh
cd tests/rls-isolation-active
go test -tags integration -v ./...
```

The harness:

1. Spins up a Postgres testcontainer per service.
2. Applies all `services/chora-*/migrations/*.sql`.
3. Lists every `relrowsecurity = true` table from `pg_class`.
4. Asserts:
   - default-deny: no tenant context returns 0 rows
   - foreign tenant: switching context never increases visible rows
   - admin bypass: superuser sees everything

## ADR-142 economy tables

Five tables in `chora_identity.0002_user_economy.sql` were missing RLS at audit time:

- `user_subscriptions` (tenant + gcid scoped)
- `user_mana` (gcid only — applies tenant-scoped RLS via membership join is impractical; gcid scope is sufficient)
- `mana_ledger` (partitioned, gcid-scoped, append-only)
- `mana_subsidy_allocations` (tenant + gcid scoped)
- `kyc_verifications` (gcid-scoped — KYC is identity-personal, not tenant-bound)

Corrective migration: `services/chora-identity/migrations/0003_user_economy_rls.sql`. Apply via the standard `migrate up` invocation; the migration is idempotent (uses `DO $$ ... EXCEPTION WHEN duplicate_object`).

## Coverage

This package: 89.5% statement coverage (above 85% domain gate).

## References

- Skill: `.claude/skills/multi-tenant-rls/SKILL.md` (always-loaded)
- Rule: `.claude/rules/ddd-enforcement.md` (always-loaded — cross-DB-forbidden)
- ADR-142: `docs/architecture/adrs/adr-142-per-user-mana-economy.md`
- ADR-143: per-user Knowledge Graph (dual-scoped RLS)
- Companion package: `libs/chora-go-common/tracing` (context helpers)

-- ============================================================================
-- idempotency_keys + cleanup stored procedure (TEMPLATE — copy per service)
-- ============================================================================
--
-- This file is a TEMPLATE under sql_fixtures/. Each service that uses the
-- chora-go-common/idempotent.PostgresStore copies this DDL into its own
-- migration directory (e.g. services/chora-creation/migrations/000N_add_idempotency_keys.up.sql).
--
-- Operational dedup table for at-least-once Pub/Sub subscribers.
--
-- Per CLAUDE.md §6 + .claude/skills/data-consistency, every subscriber MUST
-- de-duplicate by event_id (or business idempotency_key) before applying
-- the event side effect. This table stores the dedup tokens.
--
-- Soft-delete invariant DOES NOT apply: this is an OPERATIONAL table
-- holding short-lived (TTL-bounded) tokens, not domain content. The
-- companion stored procedure `cleanup_idempotency_keys` performs hard
-- deletion of TTL-expired rows; the procedure body lives inside this
-- migration file, which is exempt from the domain-table hard-delete
-- prohibition per ddd-enforcement.md §Soft Deletes Exceptions.
--
-- Each domain that needs idempotency runs this migration in its own
-- database (chora_creation, chora_consumption, …). Cross-DB queries
-- remain forbidden.
-- ============================================================================

CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          TEXT        PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ttl_at       TIMESTAMPTZ NOT NULL,
    -- Optional opaque hash of the upstream operation result. Used by the
    -- chora-go-common/idempotent package only when a service needs to
    -- prove the cached outcome of a prior duplicate (rare).
    result_hash  TEXT
);

-- Index on ttl_at to make CleanupExpired range scans cheap.
CREATE INDEX IF NOT EXISTS idempotency_keys_ttl_at_idx
    ON idempotency_keys (ttl_at);

-- ----------------------------------------------------------------------------
-- cleanup_idempotency_keys(table_name TEXT) RETURNS BIGINT
--
-- Hard-purges TTL-expired rows from the supplied idempotency table.
-- Returns the count of rows removed.
--
-- SECURITY: SECURITY DEFINER — the function executes with the privileges
-- of its owner (typically `migrate` role) so the calling app role only
-- needs EXECUTE on the function, not direct DELETE on the table.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION cleanup_idempotency_keys(table_name TEXT DEFAULT 'idempotency_keys')
RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    removed_count BIGINT;
BEGIN
    -- Validate table_name is a safe identifier to prevent SQL injection.
    IF table_name !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'invalid table_name: %', table_name;
    END IF;

    EXECUTE format('DELETE FROM %I WHERE ttl_at <= now()', table_name);
    GET DIAGNOSTICS removed_count = ROW_COUNT;
    RETURN removed_count;
END;
$$;

COMMENT ON FUNCTION cleanup_idempotency_keys(TEXT) IS
'Operational TTL purge for idempotency_keys table. Called from
chora-go-common/idempotent.Store.CleanupExpired() via either pg_cron or a
Cloud Run Job on a regular schedule (typical: hourly).';

-- Grant EXECUTE to app roles. The exact role names per database follow
-- the chora-infra m10-data-plane convention (chora_{domain}_app_rw).
-- This grant is parameterised in the per-domain migration that wraps
-- this template.
--
--   GRANT EXECUTE ON FUNCTION cleanup_idempotency_keys(TEXT) TO chora_<domain>_app_rw;

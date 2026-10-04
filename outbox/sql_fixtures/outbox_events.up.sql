-- ============================================================================
-- outbox_events — transactional outbox table (TEMPLATE — copy per service)
-- ============================================================================
--
-- Per CLAUDE.md §6 + .claude/skills/data-consistency, every domain service
-- that publishes Pub/Sub events for cross-domain side effects MUST use the
-- transactional outbox pattern: write the domain state-change AND the
-- outbox row in ONE transaction, then have a separate Relay process drain
-- pending rows to Pub/Sub.
--
-- This file is a TEMPLATE under sql_fixtures/. Each service that uses the
-- chora-common/outbox.PostgresRecorder copies this DDL into its own
-- migration directory (e.g. services/chora-creation/migrations/000N_add_outbox.up.sql).
--
-- The table lives in the SAME database as the domain it serves
-- (chora_creation, chora_consumption, …). Cross-DB reads remain forbidden.
--
-- See chora-infra/topics/topics.yaml + chora-infra/terraform/modules/m10-data-plane
-- for the centralised topic + DLQ provisioning these rows fan out to.
-- ============================================================================

CREATE TABLE IF NOT EXISTS outbox_events (
    -- UUIDv7 — sortable by time, globally unique.
    id              TEXT        PRIMARY KEY,

    -- D6.2/D6.3 canonical fields. Load-bearing for the production
    -- dispatcher (services/{NAME}/internal/adapter/outbox/store.go):
    --   - tenant_id drives multi-tenant isolation indexing (D6.3); NULL
    --     for system / platform events that pre-date a tenant binding.
    --   - gcid is the subject GCID; NULL for system events.
    --   - idempotency_key dedupes re-emissions of the same logical event;
    --     NULL for legacy callers that pre-date the canonical envelope.
    --
    -- Root-cause history: the prior template shipped WITHOUT these columns
    -- (ZA paydown 2026-05-14 / tracker #157). Services that initialized
    -- the table from this template inherited the missing columns even
    -- though their PostgresStore INSERT + FetchPending queries reference
    -- them. chora-consumption hit this in production via the pgx
    -- "column tenant_id does not exist" error class. EE paydown 2026-05-14
    -- raised these to first-class columns of the template so future
    -- template-initialized services inherit the canonical shape.
    tenant_id       UUID,
    gcid            UUID,
    idempotency_key TEXT,

    -- Domain aggregate emitting the event.
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,

    -- Logical event_type matching the topic suffix (e.g. "atom.created.v1").
    event_type      TEXT        NOT NULL,

    -- Canonical Pub/Sub topic per .claude/skills/pub-sub-topology
    -- (chora.{domain}.{aggregate}.{event_type}.v{N}).
    topic           TEXT        NOT NULL,

    -- Marshalled Protobuf event body validated by Pub/Sub Schema Registry.
    payload         BYTEA       NOT NULL,

    -- Mandatory chora.common.v1.EventEnvelope, JSONB-serialised. Read-only
    -- alongside the binary payload — JSONB is for operator + observability
    -- queries (never for runtime decoding; the wire format remains binary
    -- Protobuf).
    envelope        JSONB       NOT NULL,

    -- Domain event time (drives reorder window in the Relay).
    occurred_at     TIMESTAMPTZ NOT NULL,

    -- Lifecycle.
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','published','failed','deadlettered')),

    -- Retry telemetry.
    retry_count     INT         NOT NULL DEFAULT 0,
    last_error      TEXT        NOT NULL DEFAULT '',
    last_attempt_at TIMESTAMPTZ,

    -- Set by the Relay on successful publish.
    published_at    TIMESTAMPTZ,

    -- Provenance for cross-database queries (NEVER cross-DB JOIN; this is
    -- for in-domain audit only).
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- Indexes
-- ----------------------------------------------------------------------------

-- Hot path: Relay scans for pending rows ordered by occurred_at.
CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (occurred_at ASC)
    WHERE status = 'pending';

-- Aggregate-driven replay queries.
CREATE INDEX IF NOT EXISTS outbox_events_aggregate_idx
    ON outbox_events (aggregate_type, aggregate_id, occurred_at DESC);

-- Topic monitoring.
CREATE INDEX IF NOT EXISTS outbox_events_topic_idx
    ON outbox_events (topic, status);

-- D6.3 multi-tenant isolation lookup — dispatcher MAY filter per-tenant.
-- Composite (tenant_id, status, occurred_at) matches the
-- WHERE status='pending' ORDER BY occurred_at ASC dispatcher query when
-- a per-tenant fairness shard is selected.
CREATE INDEX IF NOT EXISTS outbox_events_tenant_idx
    ON outbox_events (tenant_id, status, occurred_at);

-- Idempotency dedupe — partial unique to allow legacy rows with NULL key.
CREATE UNIQUE INDEX IF NOT EXISTS outbox_events_idempotency_idx
    ON outbox_events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- ----------------------------------------------------------------------------
-- RLS — multi-tenant isolation
-- ----------------------------------------------------------------------------
--
-- The Relay process needs to see ALL tenants' outbox rows (it's a
-- platform-internal worker). Domain services writing rows are already
-- tenant-scoped via the surrounding business transaction. RLS on
-- outbox_events would therefore add complexity without a security gain;
-- enforce tenant scoping at the WRITE site instead via the surrounding
-- domain repository.
--
-- If a service decides to enable RLS anyway, the policy below is the
-- recommended default. Uncomment and adapt:
--
--   ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
--   CREATE POLICY outbox_events_tenant_isolation ON outbox_events
--     USING (envelope->>'tenant_id' = current_setting('app.current_tenant_id', true));
--
-- ----------------------------------------------------------------------------
-- Retention
-- ----------------------------------------------------------------------------
--
-- Published rows accumulate. Default policy: rotate via partition every 7
-- days, archive to Coldline + GCS for 30 days, then drop. The partition
-- DDL is owned by m10-data-plane / a per-domain follow-up migration.

COMMENT ON TABLE outbox_events IS
'Transactional outbox per data-consistency skill. Domain writes state-change
+ outbox row in one txn; Relay drains pending rows to Cloud Pub/Sub. Single
writer per service (leader-elected via FOR UPDATE SKIP LOCKED).';

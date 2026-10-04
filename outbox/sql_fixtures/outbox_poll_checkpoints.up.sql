-- ============================================================================
-- outbox_poll_checkpoints — per-worker resume cursor for the Outbox Relay
-- ============================================================================
--
-- Per the resilience directive ("dead-pod handling: poll worker checkpoints
-- last_processed_outbox_id per (worker_id, topic) so a replacement pod
-- resumes from the checkpoint, never re-publishes"), every relay worker
-- writes a checkpoint row in the SAME transaction that publishes a batch
-- of outbox events. The next worker reads this checkpoint at startup and
-- skips ahead so:
--
--   - At-least-once delivery is preserved (Pub/Sub is at-least-once).
--   - Duplicate suppression is the SUBSCRIBER's job (idempotency-key
--     dedup); checkpoints minimise the duplicate window.
--   - On worker pod death, replacement skips already-published events
--     instead of republishing all rows since the dawn of time.
--
-- Schema:
--
--   - worker_id           — stable identifier of the relay worker process
--                           (e.g. Cloud Run service revision URL or pod IP).
--                           When the pod is replaced, a NEW worker_id rows
--                           in WITHOUT a checkpoint, so it starts from the
--                           lowest-id pending row.
--   - topic               — partition key. Each worker tracks per-topic
--                           progress so a single multi-topic relay can
--                           resume each independently.
--   - last_processed_outbox_id — UUIDv7 of the last successfully-published
--                           row. The Relay's "claim" query becomes
--                           "WHERE id > last_processed_outbox_id AND status = 'pending'".
--   - updated_at          — heartbeat; >5min stale = lease expired,
--                           another worker may take over.
--
-- This table lives alongside `outbox_events` in the SAME domain DB.
-- Cross-DB JOIN remains forbidden — checkpoints are local-only.
-- ============================================================================

CREATE TABLE IF NOT EXISTS outbox_poll_checkpoints (
    worker_id                  TEXT        NOT NULL,
    topic                      TEXT        NOT NULL,
    last_processed_outbox_id   TEXT        NOT NULL,
    last_processed_occurred_at TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Composite primary key — one row per (worker, topic).
    PRIMARY KEY (worker_id, topic)
);

-- Hot path: lookup-by-(worker_id, topic) is covered by the PK. We add a
-- secondary index so the relay's leader-election query
-- ("any active worker on this topic?") is fast.
CREATE INDEX IF NOT EXISTS outbox_poll_checkpoints_topic_idx
    ON outbox_poll_checkpoints (topic, updated_at DESC);

COMMENT ON TABLE outbox_poll_checkpoints IS
'Resume cursor for the Outbox Relay. UPSERT atomically with the publish
transaction so a pod restart resumes from the checkpoint instead of
re-publishing every pending row. Composite key (worker_id, topic) so a
multi-worker fleet can shard topics horizontally.';

-- ----------------------------------------------------------------------------
-- DLQ (dead-letter) tracking
-- ----------------------------------------------------------------------------
--
-- When an outbox event fails to publish after `max_retries` attempts, the
-- Relay moves it to status='deadlettered' on `outbox_events` AND inserts a
-- row in `outbox_dead_letters` for human triage. Cloud Monitoring alerting
-- watches this table's `count(*) WHERE resolved_at IS NULL`.

CREATE TABLE IF NOT EXISTS outbox_dead_letters (
    -- Same id as the outbox_events row that failed.
    outbox_event_id   TEXT        PRIMARY KEY REFERENCES outbox_events(id),

    -- Why we gave up.
    failure_reason    TEXT        NOT NULL,
    attempt_count     INT         NOT NULL,

    -- Who gave up.
    worker_id         TEXT        NOT NULL,

    -- Timestamps.
    deadlettered_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at       TIMESTAMPTZ,

    -- Operator action — when manually re-queued, set resolved_at AND
    -- update outbox_events.status back to 'pending' in the same txn.
    resolution_note   TEXT        NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS outbox_dead_letters_unresolved_idx
    ON outbox_dead_letters (deadlettered_at DESC)
    WHERE resolved_at IS NULL;

COMMENT ON TABLE outbox_dead_letters IS
'Permanent failure records for the Outbox Relay. Cloud Monitoring alerts
on count(*) WHERE resolved_at IS NULL. Operator manual triage moves rows
back to outbox_events.status=pending in a single txn.';

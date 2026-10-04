// Package outbox implements the transactional outbox primitive per
// CLAUDE.md §6 + .claude/skills/data-consistency.
//
// Atomicity contract: a service writes its domain state-change AND the
// resulting outbox row in one Postgres transaction. A separate Relay
// process polls the outbox table, publishes pending rows to Cloud Pub/Sub,
// and marks rows as published. This guarantees:
//
//   - No orphan events (events without committed state)
//   - No orphan state (state without published events) — events are
//     eventually-consistent but durably committed for retry/DLQ
//   - Safe replay across crashes (rows resume from `pending`)
//   - Single-writer leader election via FOR UPDATE SKIP LOCKED
//
// The package exposes:
//
//   - Outbox.Publish — atomic enqueue, called inside the domain service's
//     transaction
//   - Relay — long-running daemon that drains pending rows to Pub/Sub
//   - Recorder — Postgres-backed (or in-memory test) row store
//   - PostgresRecorder — production implementation
//
// Reorder buffer: Pub/Sub at-least-once delivery may interleave events
// from concurrent producers. Within a configurable ReorderWindow, the
// Relay sorts rows by `occurred_at` before publishing — gives subscribers
// best-effort domain-event ordering even under crash recovery.
//
// See sql_fixtures/outbox.up.sql for the migration template.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/tracing"
)

// Status is the lifecycle of an outbox row.
type Status string

const (
	// StatusPending indicates the row has been written by a domain
	// transaction but not yet published to Pub/Sub.
	StatusPending Status = "pending"

	// StatusPublished indicates the row has been published. PublishedAt is
	// set; the row is retained per service retention policy (typical: 7
	// days for audit, then partition-rotated out).
	StatusPublished Status = "published"

	// StatusFailed is a transient failure state; the row will be retried
	// up to MaxRetries before transitioning to deadlettered.
	StatusFailed Status = "failed"

	// StatusDeadlettered indicates the row exhausted retries and was
	// routed to its DLQ topic. Operator must review.
	StatusDeadlettered Status = "deadlettered"
)

// Row is the outbox table record.
type Row struct {
	ID            string // UUIDv7
	AggregateType string // e.g. "atom"
	AggregateID   string // domain entity ID
	EventType     string // e.g. "atom.created.v1"
	Topic         string // canonical chora.{domain}.{aggregate}.{event_type}.v{N}
	Payload       []byte // marshalled Protobuf
	Envelope      envelope.Envelope
	OccurredAt    time.Time // domain event time (drives reorder)
	PublishedAt   time.Time // set when Relay publishes
	Status        Status
	RetryCount    int
	LastError     string
	Locked        bool // in-memory recorder uses this; Postgres uses row-level locks
}

// PublishOpts captures the call-site values when enqueueing an event.
type PublishOpts struct {
	// AggregateType (e.g. "atom"). Required.
	AggregateType string
	// AggregateID — domain entity ID. Required.
	AggregateID string
	// EventType — e.g. "atom.created.v1". Required.
	EventType string
	// Topic — canonical chora.{domain}.{aggregate}.{event_type}.v{N}. Required.
	Topic string
	// Payload is the marshalled Protobuf body. Required, non-empty.
	Payload []byte
	// Envelope is the chora.common.v1.EventEnvelope. Required + Validated.
	Envelope envelope.Envelope
}

// Tx is the database/sql transaction surface used by Outbox.Publish. The
// caller passes its own *sql.Tx (or compatible) so the outbox row is
// written inside the same transaction as the domain state-change. Pass
// nil when no transaction is yet open (the Recorder is responsible for
// opening one if it needs to).
type Tx interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// Recorder is the persistence contract for outbox rows. The PostgresRecorder
// is the production default; in-memory implementations are useful for unit
// tests.
type Recorder interface {
	// Record persists a new outbox row inside the supplied tx. Tx may be
	// nil if the Recorder manages its own transactions (e.g. in-memory).
	Record(ctx context.Context, tx Tx, row *Row) error

	// Claim atomically locks up to batchSize pending rows and returns
	// them. The Postgres impl uses SELECT … FOR UPDATE SKIP LOCKED to
	// guarantee at-most-one Relay processes a given row.
	Claim(ctx context.Context, batchSize int) ([]*Row, error)

	// MarkPublished marks the supplied row IDs as published.
	MarkPublished(ctx context.Context, ids []string) error

	// MarkFailed records a publish failure. If deadletter is true, the
	// row transitions to StatusDeadlettered; otherwise it remains in
	// pending/failed for retry on the next claim cycle.
	MarkFailed(ctx context.Context, id string, errMsg string, deadletter bool) error
}

// Publisher is the wire-publish surface. Implementations: pubsub.Publisher
// (Cloud Pub/Sub), pubsub.InMemoryBus (tests).
type Publisher interface {
	Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error
}

// Outbox is the front-door: domain code calls Publish(...) inside its
// transaction.
type Outbox struct {
	rec Recorder
}

// NewWithRecorder constructs an Outbox bound to the supplied recorder.
// In production wire a PostgresRecorder; in tests use the in-memory mock.
func NewWithRecorder(rec Recorder) *Outbox {
	return &Outbox{rec: rec}
}

// Publish atomically writes an outbox row inside the supplied transaction.
// Validates the envelope + opts before recording. The caller is responsible
// for committing/rolling back the transaction.
func (o *Outbox) Publish(ctx context.Context, tx Tx, opts PublishOpts) error {
	if err := envelope.Validate(opts.Envelope); err != nil {
		return fmt.Errorf("outbox.Publish: %w", err)
	}
	if opts.AggregateType == "" {
		return errors.New("outbox.Publish: aggregate_type required")
	}
	if opts.AggregateID == "" {
		return errors.New("outbox.Publish: aggregate_id required")
	}
	if opts.EventType == "" {
		return errors.New("outbox.Publish: event_type required")
	}
	if opts.Topic == "" {
		return errors.New("outbox.Publish: topic required")
	}
	if len(opts.Payload) == 0 {
		return errors.New("outbox.Publish: payload required")
	}
	row := &Row{
		ID:            newRowID(),
		AggregateType: opts.AggregateType,
		AggregateID:   opts.AggregateID,
		EventType:     opts.EventType,
		Topic:         opts.Topic,
		Payload:       append([]byte(nil), opts.Payload...),
		Envelope:      opts.Envelope,
		OccurredAt:    opts.Envelope.OccurredAt,
		Status:        StatusPending,
	}
	return o.rec.Record(ctx, tx, row)
}

// ----------------------------------------------------------------------------
// Relay — long-running drain loop
// ----------------------------------------------------------------------------

// RelayConfig tunes the Relay daemon.
type RelayConfig struct {
	// BatchSize is the max rows claimed per poll.
	BatchSize int
	// PollInterval is the sleep between empty polls.
	PollInterval time.Duration
	// MaxRetries is the per-row publish retry ceiling. Exceeding this
	// count transitions the row to StatusDeadlettered.
	MaxRetries int
	// BackoffBase is the initial backoff between retries (exponential).
	BackoffBase time.Duration
	// BackoffCeiling caps the backoff growth.
	BackoffCeiling time.Duration
	// ReorderWindow is the longest the Relay will hold a claimed batch
	// before publishing, sorted by OccurredAt. 0 disables reorder
	// (publish strictly in claim order).
	ReorderWindow time.Duration
	// ReorderBatchSize caps the reorder buffer size.
	ReorderBatchSize int
}

// Relay drains pending outbox rows to Pub/Sub.
type Relay struct {
	rec Recorder
	pub Publisher
	cfg RelayConfig

	mu      sync.Mutex
	stopped bool
}

// NewRelay constructs a Relay. Run via Drain (runs until ctx canceled or
// the recorder is empty for one poll cycle) or Start (long-running daemon).
func NewRelay(rec Recorder, pub Publisher, cfg RelayConfig) *Relay {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = 10 * time.Millisecond
	}
	if cfg.BackoffCeiling <= 0 {
		cfg.BackoffCeiling = 600 * time.Millisecond
	}
	if cfg.ReorderBatchSize <= 0 {
		cfg.ReorderBatchSize = cfg.BatchSize
	}
	return &Relay{rec: rec, pub: pub, cfg: cfg}
}

// Drain runs the relay loop until the recorder yields zero rows on a
// claim cycle (or ctx is canceled). Returns the number of rows processed.
func (r *Relay) Drain(ctx context.Context) (int, error) {
	processed := 0
	for {
		select {
		case <-ctx.Done():
			return processed, nil
		default:
		}
		rows, err := r.rec.Claim(ctx, r.cfg.BatchSize)
		if err != nil {
			return processed, fmt.Errorf("outbox.Relay.Claim: %w", err)
		}
		if len(rows) == 0 {
			return processed, nil
		}

		// Reorder by OccurredAt within this batch.
		batch := r.applyReorder(rows)

		for _, row := range batch {
			if err := r.publishWithRetry(ctx, row); err != nil {
				// publishWithRetry already marked the row; continue.
				continue
			}
			processed++
		}
	}
}

// Start runs Drain in a loop until ctx is canceled, sleeping PollInterval
// between empty polls. Designed for long-running Cloud Run / GKE workers.
func (r *Relay) Start(ctx context.Context) error {
	for {
		n, err := r.Drain(ctx)
		if err != nil {
			return err
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(r.cfg.PollInterval):
			}
		}
	}
}

func (r *Relay) applyReorder(rows []*Row) []*Row {
	if r.cfg.ReorderWindow <= 0 {
		return rows
	}
	out := make([]*Row, len(rows))
	copy(out, rows)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].OccurredAt.Before(out[j].OccurredAt)
	})
	return out
}

func (r *Relay) publishWithRetry(ctx context.Context, row *Row) error {
	// Hydrate tenant context from the envelope so downstream in-process
	// subscribers (delivered synchronously by InMemoryBus) can satisfy
	// RLS-gated repo calls (CreditAuthor, etc.) that read tenant via
	// tracing.TenantIDFromContext. Without this, the dispatcher's
	// background ctx has no tenant → side-effect fails → bus retries →
	// idempotency Claim returns false → "duplicate_event_skipped" →
	// leaderboard XP never credits. Fixes the whole class of
	// claim-then-fail-on-RLS bugs for bus-delivered subscribers.
	if row.Envelope.TenantID != "" {
		ctx = tracing.WithTenantID(ctx, row.Envelope.TenantID)
	}
	backoff := r.cfg.BackoffBase
	for attempt := 1; attempt <= r.cfg.MaxRetries; attempt++ {
		err := r.pub.Publish(ctx, row.Topic, row.Envelope, row.Payload)
		if err == nil {
			return r.rec.MarkPublished(ctx, []string{row.ID})
		}
		// On last attempt, deadletter.
		if attempt >= r.cfg.MaxRetries {
			_ = r.rec.MarkFailed(ctx, row.ID, err.Error(), true)
			return err
		}
		// Mid-retry: record failure but don't deadletter yet.
		_ = r.rec.MarkFailed(ctx, row.ID, err.Error(), false)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > r.cfg.BackoffCeiling {
			backoff = r.cfg.BackoffCeiling
		}
		// Re-claim is not strictly needed because the recorder leaves
		// the row Locked (from the original Claim) until MarkPublished
		// or MarkFailed-deadletter. The in-memory recorder unlocks on
		// MarkFailed so the next Drain pass picks it up; the Postgres
		// recorder relies on row-level lock release via the surrounding
		// txn pattern (see PostgresRecorder docs).
	}
	return errors.New("outbox.Relay: retries exhausted")
}

// ----------------------------------------------------------------------------
// IDs
// ----------------------------------------------------------------------------

func newRowID() string {
	return uuid.Must(uuid.NewV7()).String()
}

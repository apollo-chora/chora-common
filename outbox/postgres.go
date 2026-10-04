package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
)

// SQLRows is the minimal database/sql Rows surface used by the recorder.
type SQLRows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Err() error
}

// SQLDB is the minimal database/sql surface required by PostgresRecorder.
// Production: pass *sql.DB or *sql.Tx. Tests stub via the SQLDB interface.
type SQLDB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (SQLRows, error)
}

// PostgresOptions tunes a PostgresRecorder at construction time.
type PostgresOptions struct {
	// Table is the outbox table name; defaults to "outbox_events".
	Table string
}

// PostgresRecorder is the production Recorder backed by a Postgres
// `outbox_events` table. Schema in sql_fixtures/outbox.up.sql.
type PostgresRecorder struct {
	db    SQLDB
	table string
}

// validTableName guards against SQL injection in fmt.Sprintf-built queries.
var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// NewPostgresRecorder constructs a PostgresRecorder. If opts.Table is
// invalid (empty or non-identifier), method calls return an error rather
// than panicking on construction — keeps the surface predictable for
// services that build the recorder behind a factory.
func NewPostgresRecorder(db SQLDB, opts PostgresOptions) *PostgresRecorder {
	tbl := opts.Table
	if tbl == "" {
		tbl = "outbox_events"
	}
	return &PostgresRecorder{db: db, table: tbl}
}

func (r *PostgresRecorder) checkTable() error {
	if !validTableName.MatchString(r.table) {
		return fmt.Errorf("outbox: invalid table name %q", r.table)
	}
	return nil
}

// Record inserts a new outbox row. If tx is non-nil, the insert runs inside
// the transaction; otherwise the recorder uses its own DB handle (no
// transaction).
func (r *PostgresRecorder) Record(ctx context.Context, tx Tx, row *Row) error {
	if err := r.checkTable(); err != nil {
		return err
	}
	envJSON, err := MarshalEnvelopeJSON(row.Envelope)
	if err != nil {
		return fmt.Errorf("outbox.Record: %w", err)
	}
	q := fmt.Sprintf(`INSERT INTO %s (
        id, aggregate_type, aggregate_id, event_type, topic,
        payload, envelope, occurred_at, status
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`, r.table)
	args := []interface{}{
		row.ID, row.AggregateType, row.AggregateID, row.EventType, row.Topic,
		row.Payload, envJSON, row.OccurredAt, string(row.Status),
	}
	if tx != nil {
		_, err = tx.ExecContext(ctx, q, args...)
	} else {
		_, err = r.db.ExecContext(ctx, q, args...)
	}
	if err != nil {
		return fmt.Errorf("outbox.Record exec: %w", err)
	}
	return nil
}

// Claim atomically locks up to batchSize pending rows via SELECT ... FOR
// UPDATE SKIP LOCKED. Caller must wrap in a transaction (or use a single
// implicit txn cursor) so the row-level locks survive until MarkPublished
// or MarkFailed releases them. The in-memory tests expose the same
// contract via the Locked field on Row.
func (r *PostgresRecorder) Claim(ctx context.Context, batchSize int) ([]*Row, error) {
	if err := r.checkTable(); err != nil {
		return nil, err
	}
	q := fmt.Sprintf(`
        SELECT id, aggregate_type, aggregate_id, event_type, topic,
               payload, envelope, occurred_at, status, retry_count, last_error
        FROM %s
        WHERE status = 'pending'
        ORDER BY occurred_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED`, r.table)
	rows, err := r.db.QueryContext(ctx, q, batchSize)
	if err != nil {
		return nil, fmt.Errorf("outbox.Claim: %w", err)
	}
	defer rows.Close()

	var out []*Row
	for rows.Next() {
		var (
			row        Row
			envJSON    []byte
			retryCount int64
			statusStr  string
		)
		if err := rows.Scan(
			&row.ID, &row.AggregateType, &row.AggregateID, &row.EventType, &row.Topic,
			&row.Payload, &envJSON, &row.OccurredAt, &statusStr, &retryCount, &row.LastError,
		); err != nil {
			return nil, fmt.Errorf("outbox.Claim scan: %w", err)
		}
		row.Status = Status(statusStr)
		row.RetryCount = int(retryCount)
		env, err := ParseEnvelopeJSON(envJSON)
		if err != nil {
			return nil, fmt.Errorf("outbox.Claim envelope: %w", err)
		}
		row.Envelope = env
		out = append(out, &row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox.Claim iter: %w", err)
	}
	return out, nil
}

// MarkPublished bulk-updates the supplied IDs to status='published' with
// published_at=now().
func (r *PostgresRecorder) MarkPublished(ctx context.Context, ids []string) error {
	if err := r.checkTable(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	q := fmt.Sprintf(`UPDATE %s
        SET status = 'published', published_at = now()
        WHERE id = ANY($1)`, r.table)
	if _, err := r.db.ExecContext(ctx, q, ids); err != nil {
		return fmt.Errorf("outbox.MarkPublished: %w", err)
	}
	return nil
}

// MarkFailed records a publish failure. If deadletter is true the row
// transitions to status='deadlettered'; otherwise it stays pending so the
// next Claim cycle picks it up. retry_count is always incremented.
func (r *PostgresRecorder) MarkFailed(ctx context.Context, id string, errMsg string, deadletter bool) error {
	if err := r.checkTable(); err != nil {
		return err
	}
	if id == "" {
		return errors.New("outbox.MarkFailed: id required")
	}
	var q string
	if deadletter {
		q = fmt.Sprintf(`UPDATE %s
            SET status = 'deadlettered',
                retry_count = retry_count + 1,
                last_error = $1,
                last_attempt_at = now()
            WHERE id = $2`, r.table)
	} else {
		q = fmt.Sprintf(`UPDATE %s
            SET retry_count = retry_count + 1,
                last_error = $1,
                last_attempt_at = now()
            WHERE id = $2`, r.table)
	}
	if _, err := r.db.ExecContext(ctx, q, errMsg, id); err != nil {
		return fmt.Errorf("outbox.MarkFailed: %w", err)
	}
	return nil
}

// Compile-time check.
var _ Recorder = (*PostgresRecorder)(nil)

// ----------------------------------------------------------------------------
// Envelope JSON serialisation (for the envelope JSONB column)
// ----------------------------------------------------------------------------

// envelopeWire is the JSON wire shape persisted in outbox_events.envelope.
// Time fields use RFC3339 strings to play nicely with JSONB readers.
type envelopeWire struct {
	EventID            string `json:"event_id"`
	IdempotencyKey     string `json:"idempotency_key"`
	TenantID           string `json:"tenant_id"`
	GCID               string `json:"gcid,omitempty"`
	OccurredAt         string `json:"occurred_at"`
	PublishedAt        string `json:"published_at"`
	Traceparent        string `json:"traceparent"`
	Tracestate         string `json:"tracestate,omitempty"`
	SourceProject      string `json:"source_project"`
	SourceService      string `json:"source_service"`
	SchemaVersion      int32  `json:"schema_version"`
	CorrelationID      string `json:"correlation_id,omitempty"`
	CausationID        string `json:"causation_id,omitempty"`
	ChoraImdaDimension string `json:"chora_imda_dimension,omitempty"`
	ImdaLifecycleStage string `json:"imda_lifecycle_stage,omitempty"`
}

// MarshalEnvelopeJSON serialises an Envelope to bytes for the JSONB column.
func MarshalEnvelopeJSON(env envelope.Envelope) ([]byte, error) {
	w := envelopeWire{
		EventID:            env.EventID,
		IdempotencyKey:     env.IdempotencyKey,
		TenantID:           env.TenantID,
		GCID:               env.GCID,
		OccurredAt:         env.OccurredAt.UTC().Format(time.RFC3339Nano),
		PublishedAt:        env.PublishedAt.UTC().Format(time.RFC3339Nano),
		Traceparent:        env.Traceparent,
		Tracestate:         env.Tracestate,
		SourceProject:      env.SourceProject,
		SourceService:      env.SourceService,
		SchemaVersion:      env.SchemaVersion,
		CorrelationID:      env.CorrelationID,
		CausationID:        env.CausationID,
		ChoraImdaDimension: env.ChoraImdaDimension,
		ImdaLifecycleStage: env.ImdaLifecycleStage,
	}
	return json.Marshal(w)
}

// ParseEnvelopeJSON deserialises an envelope from JSONB bytes.
func ParseEnvelopeJSON(b []byte) (envelope.Envelope, error) {
	var w envelopeWire
	if err := json.Unmarshal(b, &w); err != nil {
		return envelope.Envelope{}, fmt.Errorf("envelope JSON: %w", err)
	}
	occ, err := time.Parse(time.RFC3339Nano, w.OccurredAt)
	if err != nil {
		return envelope.Envelope{}, fmt.Errorf("envelope occurred_at: %w", err)
	}
	pub, err := time.Parse(time.RFC3339Nano, w.PublishedAt)
	if err != nil {
		return envelope.Envelope{}, fmt.Errorf("envelope published_at: %w", err)
	}
	return envelope.Envelope{
		EventID:            w.EventID,
		IdempotencyKey:     w.IdempotencyKey,
		TenantID:           w.TenantID,
		GCID:               w.GCID,
		OccurredAt:         occ,
		PublishedAt:        pub,
		Traceparent:        w.Traceparent,
		Tracestate:         w.Tracestate,
		SourceProject:      w.SourceProject,
		SourceService:      w.SourceService,
		SchemaVersion:      w.SchemaVersion,
		CorrelationID:      w.CorrelationID,
		CausationID:        w.CausationID,
		ChoraImdaDimension: w.ChoraImdaDimension,
		ImdaLifecycleStage: w.ImdaLifecycleStage,
	}, nil
}

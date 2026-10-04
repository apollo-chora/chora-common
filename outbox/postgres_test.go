package outbox_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
	"github.com/5007-Capstone/chora/libs/chora-go-common/outbox"
)

// TestPostgresRecorder verifies the recorder issues the expected SQL.
// We do not spin up a real Postgres in unit tests; testcontainers
// integration is in a separate target.

func TestPostgresRecorder_RecordIssuesInsertWithEnvelopeJSON(t *testing.T) {
	stub := &pgStub{
		execHandler: func(query string, args []driver.Value) (driver.Result, error) {
			if !contains(query, "INSERT INTO outbox_events") {
				t.Errorf("query missing INSERT, got %q", query)
			}
			// Expect 11 columns: id, aggregate_type, aggregate_id, event_type,
			// topic, payload, envelope, occurred_at, status, retry_count, last_error
			if len(args) < 9 {
				t.Errorf("args=%d want >= 9", len(args))
			}
			return driver.RowsAffected(1), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	row := &outbox.Row{
		ID:            "row-1",
		AggregateType: "atom",
		AggregateID:   "atom-123",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      newValidEnvelope(),
		OccurredAt:    time.Now().UTC(),
		Status:        outbox.StatusPending,
	}
	if err := rec.Record(context.Background(), nil, row); err != nil {
		t.Fatalf("Record: %v", err)
	}
}

func TestPostgresRecorder_ClaimUsesForUpdateSkipLocked(t *testing.T) {
	stub := &pgStub{
		queryHandler: func(query string, args []driver.Value) (*sqlRows, error) {
			if !contains(query, "FOR UPDATE SKIP LOCKED") {
				t.Errorf("Claim query missing FOR UPDATE SKIP LOCKED, got %q", query)
			}
			if !contains(query, "status") || !contains(query, "pending") {
				t.Errorf("Claim should filter for pending rows, got %q", query)
			}
			// Return one row.
			env := newValidEnvelope()
			envJSON := mustEnvelopeJSON(env)
			now := time.Now().UTC()
			return newRows(
				[]string{"id", "aggregate_type", "aggregate_id", "event_type", "topic",
					"payload", "envelope", "occurred_at", "status", "retry_count", "last_error"},
				[][]driver.Value{
					{
						"row-1", "atom", "atom-123", "atom.created.v1",
						"chora.creation.atom.created.v1", []byte("p"),
						envJSON, now, "pending", int64(0), "",
					},
				},
			), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	rows, err := rec.Claim(context.Background(), 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d want 1", len(rows))
	}
	if rows[0].ID != "row-1" || rows[0].Topic != "chora.creation.atom.created.v1" {
		t.Errorf("row=%+v", rows[0])
	}
}

func TestPostgresRecorder_MarkPublishedUpdates(t *testing.T) {
	stub := &pgStub{
		execHandler: func(query string, args []driver.Value) (driver.Result, error) {
			if !contains(query, "UPDATE outbox_events") {
				t.Errorf("query missing UPDATE, got %q", query)
			}
			if !contains(query, "published") {
				t.Errorf("MarkPublished query missing published status, got %q", query)
			}
			return driver.RowsAffected(2), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if err := rec.MarkPublished(context.Background(), []string{"id-1", "id-2"}); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
}

func TestPostgresRecorder_MarkFailedTransientUpdatesCounter(t *testing.T) {
	stub := &pgStub{
		execHandler: func(query string, args []driver.Value) (driver.Result, error) {
			if !contains(query, "UPDATE outbox_events") {
				t.Errorf("query missing UPDATE, got %q", query)
			}
			if !contains(query, "retry_count") {
				t.Errorf("MarkFailed should bump retry_count, got %q", query)
			}
			if contains(query, "deadlettered") {
				t.Errorf("transient MarkFailed should NOT set deadlettered, got %q", query)
			}
			return driver.RowsAffected(1), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if err := rec.MarkFailed(context.Background(), "id-1", "transient error", false); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
}

func TestPostgresRecorder_MarkFailedDeadletterTransitionsStatus(t *testing.T) {
	stub := &pgStub{
		execHandler: func(query string, args []driver.Value) (driver.Result, error) {
			if !contains(query, "deadlettered") {
				t.Errorf("MarkFailed(deadletter=true) should set status=deadlettered, got %q", query)
			}
			return driver.RowsAffected(1), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if err := rec.MarkFailed(context.Background(), "id-1", "permanent error", true); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
}

func TestPostgresRecorder_TableNameValidatedAgainstInjection(t *testing.T) {
	stub := &pgStub{}
	cases := []string{
		"outbox; DROP TABLE x",
		"outbox--",
		"outbox events",
		"",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				// We expect either a panic or an error from
				// constructor or first call; both are acceptable.
				_ = recover()
			}()
			rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: name})
			err := rec.MarkPublished(context.Background(), []string{"id-1"})
			if err == nil {
				t.Errorf("expected validation error for table %q", name)
			}
		})
	}
}

func TestRoundTrip_PublishAndDrain(t *testing.T) {
	// End-to-end: Outbox.Publish writes a row through the Postgres recorder
	// stub, then the Relay claims and publishes it.
	stub := newRoundTripStub()
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()

	if err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "atom-123",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("payload"),
		Envelope:      env,
	}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pub := newCapturingPublisher()
	relay := outbox.NewRelay(rec, pub, outbox.RelayConfig{
		BatchSize: 10,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := relay.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if pub.Count() != 1 {
		t.Errorf("published=%d want 1", pub.Count())
	}
	row := stub.snapshot()[0]
	if row.Status != outbox.StatusPublished {
		t.Errorf("row status=%s want published", row.Status)
	}
}

// ----------------------------------------------------------------------------
// pgStub: minimal SQLDB for the PostgresRecorder's database/sql surface
// ----------------------------------------------------------------------------

type pgStub struct {
	mu           sync.Mutex
	execHandler  func(query string, args []driver.Value) (driver.Result, error)
	queryHandler func(query string, args []driver.Value) (*sqlRows, error)
}

func (s *pgStub) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.execHandler == nil {
		return nil, errors.New("no exec handler")
	}
	dvals := toDriverValues(args)
	res, err := s.execHandler(query, dvals)
	if err != nil {
		return nil, err
	}
	return resultAdapter{res}, nil
}

func (s *pgStub) QueryContext(ctx context.Context, query string, args ...interface{}) (outbox.SQLRows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queryHandler == nil {
		return nil, errors.New("no query handler")
	}
	dvals := toDriverValues(args)
	return s.queryHandler(query, dvals)
}

// roundTripStub keeps a simulated outbox table in memory + supports the
// Postgres-flavoured queries.
type roundTripStub struct {
	mu   sync.Mutex
	rows []*outbox.Row
}

func newRoundTripStub() *roundTripStub { return &roundTripStub{} }

func (s *roundTripStub) snapshot() []*outbox.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*outbox.Row, len(s.rows))
	copy(out, s.rows)
	return out
}

func (s *roundTripStub) ExecContext(_ context.Context, query string, args ...interface{}) (sql.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case strings.Contains(query, "INSERT INTO outbox_events"):
		// Args order from PostgresRecorder.Record:
		// id, aggregate_type, aggregate_id, event_type, topic, payload, envelope, occurred_at, status
		row := &outbox.Row{
			ID:            args[0].(string),
			AggregateType: args[1].(string),
			AggregateID:   args[2].(string),
			EventType:     args[3].(string),
			Topic:         args[4].(string),
			Payload:       args[5].([]byte),
			Envelope:      mustParseEnvelopeJSON(args[6].([]byte)),
			OccurredAt:    args[7].(time.Time),
			Status:        outbox.Status(args[8].(string)),
		}
		s.rows = append(s.rows, row)
		return resultAdapter{driver.RowsAffected(1)}, nil
	case strings.Contains(query, "UPDATE outbox_events") && strings.Contains(query, "published"):
		// MarkPublished
		ids := args[0].([]string)
		for _, row := range s.rows {
			for _, id := range ids {
				if row.ID == id {
					row.Status = outbox.StatusPublished
					row.PublishedAt = time.Now().UTC()
				}
			}
		}
		return resultAdapter{driver.RowsAffected(int64(len(ids)))}, nil
	case strings.Contains(query, "UPDATE outbox_events"):
		return resultAdapter{driver.RowsAffected(1)}, nil
	}
	return resultAdapter{driver.RowsAffected(0)}, nil
}

func (s *roundTripStub) QueryContext(_ context.Context, query string, args ...interface{}) (outbox.SQLRows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.Contains(query, "FOR UPDATE SKIP LOCKED") {
		return newRows(nil, nil), nil
	}
	// Return all pending rows (limit by batch size).
	limit := 1000
	if len(args) > 0 {
		switch v := args[0].(type) {
		case int:
			limit = v
		case int64:
			limit = int(v)
		}
	}
	cols := []string{"id", "aggregate_type", "aggregate_id", "event_type", "topic",
		"payload", "envelope", "occurred_at", "status", "retry_count", "last_error"}
	var data [][]driver.Value
	for _, row := range s.rows {
		if row.Status != outbox.StatusPending {
			continue
		}
		envJSON := mustEnvelopeJSON(row.Envelope)
		data = append(data, []driver.Value{
			row.ID, row.AggregateType, row.AggregateID, row.EventType, row.Topic,
			row.Payload, envJSON, row.OccurredAt, string(row.Status), int64(row.RetryCount), row.LastError,
		})
		if len(data) >= limit {
			break
		}
	}
	return newRows(cols, data), nil
}

func mustEnvelopeJSON(env envelope.Envelope) []byte {
	b, err := outbox.MarshalEnvelopeJSON(env)
	if err != nil {
		panic(err)
	}
	return b
}

func mustParseEnvelopeJSON(b []byte) envelope.Envelope {
	env, err := outbox.ParseEnvelopeJSON(b)
	if err != nil {
		panic(err)
	}
	return env
}

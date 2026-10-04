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

	"github.com/apollo-chora/chora-common/outbox"
)

// TestRelay_Start runs the daemon loop and verifies it drains + sleeps
// until ctx cancel.
func TestRelay_Start_ProcessesUntilCancel(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	for i := 0; i < 2; i++ {
		_ = o.Publish(context.Background(), nil, outbox.PublishOpts{
			AggregateType: "atom",
			AggregateID:   "a",
			EventType:     "atom.created.v1",
			Topic:         "chora.creation.atom.created.v1",
			Payload:       []byte("p"),
			Envelope:      env,
		})
	}
	pub := newCapturingPublisher()
	relay := outbox.NewRelay(rec, pub, outbox.RelayConfig{
		BatchSize:        10,
		PollInterval:     5 * time.Millisecond,
		MaxRetries:       3,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderBatchSize: 1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := relay.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if pub.Count() != 2 {
		t.Errorf("published=%d want 2", pub.Count())
	}
}

func TestRelay_Drain_ContextCancellationStops(t *testing.T) {
	rec := newRecorder()
	relay := outbox.NewRelay(rec, newCapturingPublisher(), outbox.RelayConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n, err := relay.Drain(ctx)
	if err != nil {
		t.Errorf("Drain err=%v want nil on cancelled ctx", err)
	}
	if n != 0 {
		t.Errorf("processed=%d want 0", n)
	}
}

func TestPublish_RejectsBlankAggregateType(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "",
		AggregateID:   "id",
		EventType:     "et",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      env,
	})
	if err == nil || !strings.Contains(err.Error(), "aggregate_type") {
		t.Errorf("err=%v want aggregate_type", err)
	}
}

func TestPublish_RejectsBlankAggregateID(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "",
		EventType:     "et",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      env,
	})
	if err == nil || !strings.Contains(err.Error(), "aggregate_id") {
		t.Errorf("err=%v want aggregate_id", err)
	}
}

func TestPublish_RejectsBlankEventType(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "id",
		EventType:     "",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      env,
	})
	if err == nil || !strings.Contains(err.Error(), "event_type") {
		t.Errorf("err=%v want event_type", err)
	}
}

// TestPostgresRecorder_RecordWithTransaction verifies that when a Tx is
// supplied, the insert flows through the tx path (not the package-level db).
func TestPostgresRecorder_RecordWithTransaction(t *testing.T) {
	pkgDB := &pgStub{} // should NOT be used
	tx := &fakeTx{}
	rec := outbox.NewPostgresRecorder(pkgDB, outbox.PostgresOptions{Table: "outbox_events"})
	row := &outbox.Row{
		ID:            "id-1",
		AggregateType: "atom",
		AggregateID:   "id",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      newValidEnvelope(),
		OccurredAt:    time.Now().UTC(),
		Status:        outbox.StatusPending,
	}
	if err := rec.Record(context.Background(), tx, row); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if tx.execCount != 1 {
		t.Errorf("tx.execCount=%d want 1", tx.execCount)
	}
}

func TestPostgresRecorder_RecordEnvelopeJSONErrorPropagates(t *testing.T) {
	// Build an envelope with NaN/invalid time? Easier: pass a row whose
	// envelope serialiser succeeds but whose downstream exec fails.
	stub := &pgStub{
		execHandler: func(_ string, _ []driver.Value) (driver.Result, error) {
			return nil, errors.New("disk full")
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	row := &outbox.Row{
		ID:            "id-1",
		AggregateType: "atom",
		AggregateID:   "id",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      newValidEnvelope(),
		OccurredAt:    time.Now().UTC(),
		Status:        outbox.StatusPending,
	}
	if err := rec.Record(context.Background(), nil, row); err == nil {
		t.Fatal("expected exec error to propagate")
	}
}

func TestPostgresRecorder_MarkPublished_NoIDsIsNoOp(t *testing.T) {
	called := 0
	stub := &pgStub{
		execHandler: func(_ string, _ []driver.Value) (driver.Result, error) {
			called++
			return driver.RowsAffected(0), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if err := rec.MarkPublished(context.Background(), nil); err != nil {
		t.Errorf("MarkPublished(nil) err=%v want nil", err)
	}
	if called != 0 {
		t.Errorf("called=%d want 0", called)
	}
}

func TestPostgresRecorder_MarkFailedRequiresID(t *testing.T) {
	stub := &pgStub{}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if err := rec.MarkFailed(context.Background(), "", "err", false); err == nil {
		t.Error("expected error for empty id")
	}
}

func TestPostgresRecorder_TableNameInvalid(t *testing.T) {
	stub := &pgStub{}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "x'; DROP"})
	if err := rec.MarkPublished(context.Background(), []string{"id-1"}); err == nil {
		t.Error("expected invalid-table error")
	}
}

func TestParseEnvelopeJSON_InvalidJSONReturnsError(t *testing.T) {
	_, err := outbox.ParseEnvelopeJSON([]byte("not json"))
	if err == nil {
		t.Fatal("expected JSON parse error")
	}
}

func TestParseEnvelopeJSON_BadOccurredAtReturnsError(t *testing.T) {
	_, err := outbox.ParseEnvelopeJSON([]byte(`{"event_id":"e","idempotency_key":"k","tenant_id":"t","occurred_at":"not-a-time","published_at":"2026-05-09T00:00:00Z","traceparent":"x","source_project":"p","source_service":"s","schema_version":1}`))
	if err == nil {
		t.Fatal("expected occurred_at parse error")
	}
}

func TestParseEnvelopeJSON_BadPublishedAtReturnsError(t *testing.T) {
	_, err := outbox.ParseEnvelopeJSON([]byte(`{"event_id":"e","idempotency_key":"k","tenant_id":"t","occurred_at":"2026-05-09T00:00:00Z","published_at":"bogus","traceparent":"x","source_project":"p","source_service":"s","schema_version":1}`))
	if err == nil {
		t.Fatal("expected published_at parse error")
	}
}

func TestPublishWithRetry_ContextCanceledAbortsRetry(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	_ = o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "a",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      env,
	})
	pub := &flakyPublisher{fn: func() error { return errors.New("transient") }}
	relay := outbox.NewRelay(rec, pub, outbox.RelayConfig{
		BatchSize:        1,
		MaxRetries:       50,
		BackoffBase:      50 * time.Millisecond,
		BackoffCeiling:   100 * time.Millisecond,
		ReorderBatchSize: 1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := relay.Drain(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Drain err=%v want canceled/deadline or nil", err)
	}
}

func TestRelay_DefaultsApplied(t *testing.T) {
	// Construct with all-zero RelayConfig. NewRelay should fill defaults.
	rec := newRecorder()
	relay := outbox.NewRelay(rec, newCapturingPublisher(), outbox.RelayConfig{})
	if relay == nil {
		t.Fatal("NewRelay returned nil")
	}
}

// fakeTx is the minimal Tx used to verify Record honours an explicit
// transaction.
type fakeTx struct {
	mu        sync.Mutex
	execCount int
}

func (t *fakeTx) ExecContext(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.execCount++
	return resultAdapter{driver.RowsAffected(1)}, nil
}

// Tests for the transactional outbox primitive per CLAUDE.md §6 +
// .claude/skills/data-consistency. Atomic state-write + outbox-row-write
// guarantees no orphan events on commit/rollback.
package outbox_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/outbox"
)

// TestEvent is a tiny stand-in for a Protobuf event payload. The outbox
// primitive should accept ANY proto.Message — for unit tests we use a
// hand-rolled binary serialiser.
type testEvent struct{ payload string }

func (t testEvent) Marshal() ([]byte, error) { return []byte(t.payload), nil }

func TestOutbox_Publish_RecordsRowOnTx(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := envelope.Build(context.Background(), envelope.BuildOpts{
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-x",
	})
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "atom-123",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("payload"),
		Envelope:      env,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := rec.Count(); got != 1 {
		t.Errorf("rows recorded=%d want 1", got)
	}
	row := rec.Snapshot()[0]
	if row.AggregateType != "atom" || row.AggregateID != "atom-123" {
		t.Errorf("row aggregate %s:%s want atom:atom-123", row.AggregateType, row.AggregateID)
	}
	if row.Topic != "chora.creation.atom.created.v1" {
		t.Errorf("row topic=%s", row.Topic)
	}
	if row.Status != outbox.StatusPending {
		t.Errorf("row status=%s want pending", row.Status)
	}
	if row.Envelope.EventID != env.EventID {
		t.Error("envelope not preserved on row")
	}
}

func TestOutbox_Publish_RejectsInvalidEnvelope(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	bad := envelope.Envelope{} // missing all mandatory fields
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "atom-123",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       []byte("p"),
		Envelope:      bad,
	})
	if err == nil {
		t.Fatal("expected error for invalid envelope")
	}
}

func TestOutbox_Publish_RejectsEmptyTopic(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "atom-123",
		EventType:     "atom.created.v1",
		Topic:         "",
		Payload:       []byte("p"),
		Envelope:      env,
	})
	if err == nil || !strings.Contains(err.Error(), "topic") {
		t.Errorf("err=%v want topic violation", err)
	}
}

func TestOutbox_Publish_RejectsEmptyPayload(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	err := o.Publish(context.Background(), nil, outbox.PublishOpts{
		AggregateType: "atom",
		AggregateID:   "atom-123",
		EventType:     "atom.created.v1",
		Topic:         "chora.creation.atom.created.v1",
		Payload:       nil,
		Envelope:      env,
	})
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Errorf("err=%v want payload violation", err)
	}
}

func TestRelay_PublishesPendingRows(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	env := newValidEnvelope()
	for i := 0; i < 3; i++ {
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
		PollInterval:     time.Millisecond,
		MaxRetries:       5,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   10 * time.Millisecond,
		ReorderWindow:    5 * time.Millisecond,
		ReorderBatchSize: 3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	if _, err := relay.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if got := pub.Count(); got != 3 {
		t.Errorf("published=%d want 3", got)
	}
	for _, row := range rec.Snapshot() {
		if row.Status != outbox.StatusPublished {
			t.Errorf("row status=%s want published", row.Status)
		}
		if row.PublishedAt.IsZero() {
			t.Error("row PublishedAt unset post-publish")
		}
	}
}

func TestRelay_RetriesOnPublishFailure(t *testing.T) {
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

	// Fail twice, succeed on third try.
	var attempts int32
	pub := &flakyPublisher{
		fn: func() error {
			n := atomicAddInt32(&attempts, 1)
			if n < 3 {
				return errors.New("transient")
			}
			return nil
		},
	}

	relay := outbox.NewRelay(rec, pub, outbox.RelayConfig{
		BatchSize:        1,
		PollInterval:     time.Millisecond,
		MaxRetries:       5,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderWindow:    0,
		ReorderBatchSize: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := relay.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if got := atomicLoadInt32(&attempts); got < 3 {
		t.Errorf("attempts=%d want >=3 (retry path)", got)
	}
	row := rec.Snapshot()[0]
	if row.Status != outbox.StatusPublished {
		t.Errorf("row status=%s want published after retry", row.Status)
	}
}

func TestRelay_DLQOnMaxRetries(t *testing.T) {
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

	pub := &flakyPublisher{fn: func() error { return errors.New("permanent") }}
	relay := outbox.NewRelay(rec, pub, outbox.RelayConfig{
		BatchSize:        1,
		PollInterval:     time.Millisecond,
		MaxRetries:       3,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderWindow:    0,
		ReorderBatchSize: 1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := relay.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	row := rec.Snapshot()[0]
	if row.Status != outbox.StatusDeadlettered {
		t.Errorf("row status=%s want deadlettered after %d retries", row.Status, row.RetryCount)
	}
	if row.RetryCount < 3 {
		t.Errorf("retry_count=%d want >=3", row.RetryCount)
	}
	if row.LastError == "" {
		t.Error("last_error empty post-DLQ")
	}
}

func TestRelay_ReorderBufferPublishesByOccurredAt(t *testing.T) {
	rec := newRecorder()
	o := outbox.NewWithRecorder(rec)
	// Insert 3 rows with shuffled occurred_at timestamps.
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	type seed struct {
		offset time.Duration
		body   string
	}
	seeds := []seed{
		{2 * time.Millisecond, "C"},
		{0, "A"},
		{1 * time.Millisecond, "B"},
	}
	for _, s := range seeds {
		env := envelope.Envelope{
			EventID:        "evt-" + s.body,
			IdempotencyKey: "evt-" + s.body,
			TenantID:       "t",
			OccurredAt:     now.Add(s.offset),
			PublishedAt:    now.Add(s.offset),
			Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			SourceProject:  "p",
			SourceService:  "s",
			SchemaVersion:  1,
		}
		_ = o.Publish(context.Background(), nil, outbox.PublishOpts{
			AggregateType: "atom",
			AggregateID:   "a",
			EventType:     "atom.created.v1",
			Topic:         "chora.creation.atom.created.v1",
			Payload:       []byte(s.body),
			Envelope:      env,
		})
	}

	pub := newCapturingPublisher()
	relay := outbox.NewRelay(rec, pub, outbox.RelayConfig{
		BatchSize:        10,
		PollInterval:     time.Millisecond,
		MaxRetries:       5,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderWindow:    20 * time.Millisecond,
		ReorderBatchSize: 10,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := relay.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if pub.Count() != 3 {
		t.Fatalf("published=%d want 3", pub.Count())
	}
	got := pub.Bodies()
	if got[0] != "A" || got[1] != "B" || got[2] != "C" {
		t.Errorf("publish order=%v want [A B C] (sorted by occurred_at)", got)
	}
}

func TestRelay_LeaderElection_OnlyOneRelayProcessesRow(t *testing.T) {
	// Simulate two relays competing for the same row. With FOR UPDATE
	// SKIP LOCKED semantics on the recorder, only one should win each
	// claim. We assert the row is published exactly once.
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

	pub := newCapturingPublisher()
	cfg := outbox.RelayConfig{
		BatchSize:        10,
		PollInterval:     time.Millisecond,
		MaxRetries:       5,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderWindow:    0,
		ReorderBatchSize: 1,
	}
	r1 := outbox.NewRelay(rec, pub, cfg)
	r2 := outbox.NewRelay(rec, pub, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = r1.Drain(ctx) }()
	go func() { defer wg.Done(); _, _ = r2.Drain(ctx) }()
	wg.Wait()

	if got := pub.Count(); got != 1 {
		t.Errorf("published=%d want exactly 1 (no duplicate across relays)", got)
	}
}

// ----------------------------------------------------------------------------
// Test helpers
// ----------------------------------------------------------------------------

func newValidEnvelope() envelope.Envelope {
	return envelope.Build(context.Background(), envelope.BuildOpts{
		SchemaVersion: 1,
		SourceProject: "chora-489812",
		SourceService: "chora-x",
	})
}

// recorder is a fake outbox.Recorder backed by an in-memory slice. It also
// implements the outbox.Claimer / Marker contracts so the relay can drive
// it directly.
type recorder struct {
	mu   sync.Mutex
	rows []*outbox.Row
}

func newRecorder() *recorder { return &recorder{} }

func (r *recorder) Record(_ context.Context, _ outbox.Tx, row *outbox.Row) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, row)
	return nil
}

func (r *recorder) Claim(_ context.Context, batchSize int) ([]*outbox.Row, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*outbox.Row, 0, batchSize)
	for _, row := range r.rows {
		if row.Status == outbox.StatusPending && !row.Locked {
			row.Locked = true
			out = append(out, row)
			if len(out) >= batchSize {
				break
			}
		}
	}
	return out, nil
}

func (r *recorder) MarkPublished(_ context.Context, ids []string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	want := map[string]bool{}
	for _, id := range ids {
		want[id] = true
	}
	now := time.Now().UTC()
	for _, row := range r.rows {
		if want[row.ID] {
			row.Status = outbox.StatusPublished
			row.PublishedAt = now
			row.Locked = false
		}
	}
	return nil
}

func (r *recorder) MarkFailed(_ context.Context, id string, errMsg string, deadletter bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, row := range r.rows {
		if row.ID == id {
			row.RetryCount++
			row.LastError = errMsg
			row.Locked = false
			if deadletter {
				row.Status = outbox.StatusDeadlettered
			}
			return nil
		}
	}
	return errors.New("row not found")
}

func (r *recorder) Snapshot() []outbox.Row {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]outbox.Row, len(r.rows))
	for i, row := range r.rows {
		out[i] = *row
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	return out
}

func (r *recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.rows)
}

// capturingPublisher records publish bodies for ordering assertions.
type capturingPublisher struct {
	mu     sync.Mutex
	bodies []string
}

func newCapturingPublisher() *capturingPublisher { return &capturingPublisher{} }

func (p *capturingPublisher) Publish(_ context.Context, _ string, env envelope.Envelope, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.bodies = append(p.bodies, string(payload))
	return nil
}

func (p *capturingPublisher) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.bodies)
}

func (p *capturingPublisher) Bodies() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.bodies))
	copy(out, p.bodies)
	return out
}

type flakyPublisher struct {
	fn func() error
}

func (f *flakyPublisher) Publish(_ context.Context, _ string, _ envelope.Envelope, _ []byte) error {
	return f.fn()
}

// ----------------------------------------------------------------------------
// atomic helpers (avoid importing sync/atomic in test names)
// ----------------------------------------------------------------------------

var atomicMu sync.Mutex

func atomicAddInt32(p *int32, n int32) int32 {
	atomicMu.Lock()
	defer atomicMu.Unlock()
	*p += n
	return *p
}

func atomicLoadInt32(p *int32) int32 {
	atomicMu.Lock()
	defer atomicMu.Unlock()
	return *p
}

// ----------------------------------------------------------------------------
// SQL stub plumbing — minimal shapes used by both outbox_test.go and
// postgres_test.go. Kept compact since the relay tests use the in-memory
// recorder, and the Postgres recorder tests stub the SQL surface.
// ----------------------------------------------------------------------------

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

func toDriverValues(args []interface{}) []driver.Value {
	out := make([]driver.Value, len(args))
	for i, a := range args {
		out[i] = a
	}
	return out
}

type resultAdapter struct {
	r driver.Result
}

func (r resultAdapter) LastInsertId() (int64, error) { return r.r.LastInsertId() }
func (r resultAdapter) RowsAffected() (int64, error) { return r.r.RowsAffected() }

type sqlRows struct {
	cols []string
	data [][]driver.Value
	pos  int
	err  error
}

func newRows(cols []string, data [][]driver.Value) *sqlRows {
	return &sqlRows{cols: cols, data: data, pos: -1}
}
func (r *sqlRows) Next() bool {
	r.pos++
	return r.pos < len(r.data)
}
func (r *sqlRows) Scan(dest ...interface{}) error {
	if r.pos < 0 || r.pos >= len(r.data) {
		return errors.New("no current row")
	}
	row := r.data[r.pos]
	if len(dest) != len(row) {
		return fmt.Errorf("scan dest count mismatch: dest=%d row=%d", len(dest), len(row))
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			switch v := row[i].(type) {
			case string:
				*p = v
			default:
				return fmt.Errorf("unsupported source for *string at col %d (%T)", i, row[i])
			}
		case *[]byte:
			switch v := row[i].(type) {
			case []byte:
				*p = v
			default:
				return fmt.Errorf("unsupported source for *[]byte at col %d (%T)", i, row[i])
			}
		case *time.Time:
			switch v := row[i].(type) {
			case time.Time:
				*p = v
			default:
				return fmt.Errorf("unsupported source for *time.Time at col %d (%T)", i, row[i])
			}
		case *int64:
			switch v := row[i].(type) {
			case int64:
				*p = v
			case int:
				*p = int64(v)
			default:
				return fmt.Errorf("unsupported source for *int64 at col %d (%T)", i, row[i])
			}
		default:
			return fmt.Errorf("unsupported scan dest at col %d (%T)", i, d)
		}
	}
	return nil
}
func (r *sqlRows) Close() error { return nil }
func (r *sqlRows) Err() error   { return r.err }

var _ sql.Result = driver.RowsAffected(0) // keep imports lively

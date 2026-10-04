package idempotent_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
)

// TestPostgresStore exercises the Postgres adapter against an in-memory
// stub of the database/sql Querier interface. We do NOT spin up a real
// Postgres in unit tests — testcontainers integration is in a separate
// integration test target. Here we verify that the store issues the right
// SQL with the right args.

func TestPostgresStore_MarkUsesUpsert(t *testing.T) {
	stub := &sqlStub{
		execHandler: func(query string, args []driver.Value) (driver.Result, error) {
			if !contains(query, "INSERT INTO idempotency_keys") {
				t.Errorf("Mark query missing INSERT, got %q", query)
			}
			if !contains(query, "ON CONFLICT") {
				t.Errorf("Mark query missing ON CONFLICT (upsert), got %q", query)
			}
			if len(args) != 3 {
				t.Errorf("args=%d want 3 (key, processed_at, ttl_at)", len(args))
			}
			return driver.RowsAffected(1), nil
		},
	}
	store := idempotent.NewPostgresStore(stub)
	if err := store.Mark(context.Background(), "k", time.Hour); err != nil {
		t.Fatalf("Mark: %v", err)
	}
}

func TestPostgresStore_SeenIssuesSelect(t *testing.T) {
	stub := &sqlStub{
		queryHandler: func(query string, args []driver.Value) (*sqlRows, error) {
			if !contains(query, "SELECT") || !contains(query, "idempotency_keys") {
				t.Errorf("Seen query missing SELECT, got %q", query)
			}
			if !contains(query, "ttl_at") || !contains(query, "now()") {
				t.Errorf("Seen query should compare ttl_at to now(), got %q", query)
			}
			if len(args) != 1 {
				t.Errorf("args=%d want 1", len(args))
			}
			return newRows([]string{"key"}, [][]driver.Value{{"k"}}), nil
		},
	}
	store := idempotent.NewPostgresStore(stub)
	seen, err := store.Seen(context.Background(), "k")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if !seen {
		t.Error("Seen=false want true")
	}
}

func TestPostgresStore_SeenReturnsFalseOnNoRows(t *testing.T) {
	stub := &sqlStub{
		queryHandler: func(query string, args []driver.Value) (*sqlRows, error) {
			return newRows([]string{"key"}, nil), nil
		},
	}
	store := idempotent.NewPostgresStore(stub)
	seen, err := store.Seen(context.Background(), "k-missing")
	if err != nil {
		t.Fatalf("Seen: %v", err)
	}
	if seen {
		t.Error("Seen=true want false")
	}
}

func TestPostgresStore_CleanupExpiredCallsStoredProcedure(t *testing.T) {
	stub := &sqlStub{
		queryHandler: func(query string, args []driver.Value) (*sqlRows, error) {
			if !contains(query, "cleanup_idempotency_keys") {
				t.Errorf("CleanupExpired query missing stored proc call, got %q", query)
			}
			if len(args) != 1 {
				t.Errorf("args=%d want 1 (table name)", len(args))
			}
			rows := newRows([]string{"removed"}, [][]driver.Value{{int64(7)}})
			return rows, nil
		},
	}
	store := idempotent.NewPostgresStore(stub)
	removed, err := store.CleanupExpired(context.Background())
	if err != nil {
		t.Fatalf("CleanupExpired: %v", err)
	}
	if removed != 7 {
		t.Errorf("removed=%d want 7", removed)
	}
}

func TestPostgresStore_ProcessDelegatesToSeenAndMark(t *testing.T) {
	// First call: Seen=false (no rows), then fn runs, then Mark inserts.
	// Second call: Seen=true (row exists), fn skipped.
	var gotMark int
	var seenSeq []bool
	seenSeq = []bool{false, true}
	idx := 0
	stub := &sqlStub{
		execHandler: func(query string, args []driver.Value) (driver.Result, error) {
			if contains(query, "INSERT INTO idempotency_keys") {
				gotMark++
				return driver.RowsAffected(1), nil
			}
			t.Errorf("unexpected exec %q", query)
			return driver.RowsAffected(0), nil
		},
		queryHandler: func(query string, args []driver.Value) (*sqlRows, error) {
			defer func() { idx++ }()
			if seenSeq[idx] {
				return newRows([]string{"key"}, [][]driver.Value{{"k"}}), nil
			}
			return newRows([]string{"key"}, nil), nil
		},
	}
	store := idempotent.NewPostgresStore(stub)
	var ran int
	if err := store.Process(context.Background(), "k", time.Hour, func() error {
		ran++
		return nil
	}); err != nil {
		t.Fatalf("first Process: %v", err)
	}
	if err := store.Process(context.Background(), "k", time.Hour, func() error {
		ran++
		return nil
	}); err != nil {
		t.Fatalf("second Process: %v", err)
	}
	if ran != 1 {
		t.Errorf("ran=%d want 1", ran)
	}
	if gotMark != 1 {
		t.Errorf("Mark count=%d want 1", gotMark)
	}
}

// ----------------------------------------------------------------------------
// SQL stub helpers
// ----------------------------------------------------------------------------

type sqlStub struct {
	mu           sync.Mutex
	execHandler  func(query string, args []driver.Value) (driver.Result, error)
	queryHandler func(query string, args []driver.Value) (*sqlRows, error)
}

func (s *sqlStub) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
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

func (s *sqlStub) QueryContext(ctx context.Context, query string, args ...interface{}) (idempotent.SQLRows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queryHandler == nil {
		return nil, errors.New("no query handler")
	}
	dvals := toDriverValues(args)
	return s.queryHandler(query, dvals)
}

func (s *sqlStub) QueryRowContext(ctx context.Context, query string, args ...interface{}) idempotent.SQLRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queryHandler == nil {
		return rowErr{err: errors.New("no query handler")}
	}
	dvals := toDriverValues(args)
	rows, err := s.queryHandler(query, dvals)
	if err != nil {
		return rowErr{err: err}
	}
	return rows
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
	if r.pos < 0 {
		// Auto-advance for QueryRow-style single-row scans.
		if !r.Next() {
			return errors.New("no current row")
		}
	} else if r.pos >= len(r.data) {
		return errors.New("no current row")
	}
	row := r.data[r.pos]
	if len(dest) != len(row) {
		return errors.New("scan dest count mismatch")
	}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			*p = row[i].(string)
		case *int64:
			switch v := row[i].(type) {
			case int64:
				*p = v
			case int:
				*p = int64(v)
			default:
				return errors.New("unsupported int64 source")
			}
		case *int:
			switch v := row[i].(type) {
			case int:
				*p = v
			case int64:
				*p = int(v)
			default:
				return errors.New("unsupported int source")
			}
		default:
			return errors.New("unsupported scan dest")
		}
	}
	return nil
}
func (r *sqlRows) Close() error { return nil }
func (r *sqlRows) Err() error   { return r.err }

type rowErr struct{ err error }

func (r rowErr) Scan(_ ...interface{}) error { return r.err }

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// Sanity-check helper used elsewhere; avoids a pkg import cycle.
func sortedCopy(s []string) []string {
	out := append([]string{}, s...)
	sort.Strings(out)
	return out
}

// Unused but suppresses "declared and not used" gripes if reflect ever
// becomes useful.
var _ = reflect.TypeOf

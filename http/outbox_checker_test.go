// Package choraserver — SQLOutboxChecker tests.
//
// The checker runs `SELECT 1 FROM <table> LIMIT 1`. We can't spin up a
// real Postgres in unit tests; we use a sql/driver stub to verify the
// shape of the call.
package choraserver_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	choraserver "github.com/apollo-chora/chora-common/http"
)

func TestSQLOutboxChecker_IsReachable_HappyPath(t *testing.T) {
	drv := &fakeDriver{
		queryHandler: func(query string, _ []driver.NamedValue) (driver.Rows, error) {
			if !strings.Contains(query, "SELECT 1 FROM outbox_events") {
				return nil, errors.New("unexpected query: " + query)
			}
			return &fakeRows{rows: [][]driver.Value{{int64(1)}}, cols: []string{"?column?"}}, nil
		},
	}
	sql.Register("fake-outbox-happy", drv)
	db, err := sql.Open("fake-outbox-happy", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	c := choraserver.NewSQLOutboxChecker(db, "outbox_events")
	if err := c.IsReachable(context.Background()); err != nil {
		t.Fatalf("expected nil; got %v", err)
	}
}

func TestSQLOutboxChecker_IsReachable_EmptyTableOK(t *testing.T) {
	// An outbox table with no rows is OK — only missing-table errors.
	drv := &fakeDriver{
		queryHandler: func(query string, _ []driver.NamedValue) (driver.Rows, error) {
			return &fakeRows{rows: nil, cols: []string{"?column?"}}, nil // no rows
		},
	}
	sql.Register("fake-outbox-empty", drv)
	db, err := sql.Open("fake-outbox-empty", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	c := choraserver.NewSQLOutboxChecker(db, "")
	if err := c.IsReachable(context.Background()); err != nil {
		t.Fatalf("expected nil on empty table; got %v", err)
	}
}

func TestSQLOutboxChecker_IsReachable_TableMissing(t *testing.T) {
	drv := &fakeDriver{
		queryHandler: func(_ string, _ []driver.NamedValue) (driver.Rows, error) {
			return nil, errors.New(`pq: relation "outbox_events" does not exist`)
		},
	}
	sql.Register("fake-outbox-missing", drv)
	db, err := sql.Open("fake-outbox-missing", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	c := choraserver.NewSQLOutboxChecker(db, "outbox_events")
	err = c.IsReachable(context.Background())
	if err == nil {
		t.Fatalf("expected error on missing table; got nil")
	}
	if !strings.Contains(err.Error(), "outbox_events") {
		t.Errorf("error should mention table name; got %q", err.Error())
	}
}

func TestSQLOutboxChecker_NilDB(t *testing.T) {
	c := choraserver.NewSQLOutboxChecker(nil, "")
	err := c.IsReachable(context.Background())
	if err == nil {
		t.Fatalf("expected error on nil db; got nil")
	}
}

func TestSQLOutboxChecker_CustomTableName(t *testing.T) {
	drv := &fakeDriver{
		queryHandler: func(query string, _ []driver.NamedValue) (driver.Rows, error) {
			if !strings.Contains(query, "FROM custom_outbox") {
				return nil, errors.New("expected custom_outbox in query, got: " + query)
			}
			return &fakeRows{rows: [][]driver.Value{{int64(1)}}, cols: []string{"?column?"}}, nil
		},
	}
	sql.Register("fake-outbox-custom", drv)
	db, err := sql.Open("fake-outbox-custom", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	c := choraserver.NewSQLOutboxChecker(db, "custom_outbox")
	if err := c.IsReachable(context.Background()); err != nil {
		t.Fatalf("expected nil; got %v", err)
	}
}

// -----------------------------------------------------------------------------
// fake sql driver — minimal shim for unit tests.
// -----------------------------------------------------------------------------

type fakeDriver struct {
	mu           sync.Mutex
	queryHandler func(query string, args []driver.NamedValue) (driver.Rows, error)
}

func (d *fakeDriver) Open(_ string) (driver.Conn, error) {
	return &fakeConn{d: d}, nil
}

type fakeConn struct {
	d *fakeDriver
}

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{c: c, query: query}, nil
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

// QueryContext lets database/sql skip Prepare → Query for one-shot SELECTs.
func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.d.mu.Lock()
	h := c.d.queryHandler
	c.d.mu.Unlock()
	if h == nil {
		return nil, errors.New("no queryHandler")
	}
	return h(query, args)
}

type fakeStmt struct {
	c     *fakeConn
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 }
func (s *fakeStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, errors.New("not implemented")
}
func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	named := make([]driver.NamedValue, len(args))
	for i, v := range args {
		named[i] = driver.NamedValue{Ordinal: i + 1, Value: v}
	}
	s.c.d.mu.Lock()
	h := s.c.d.queryHandler
	s.c.d.mu.Unlock()
	if h == nil {
		return nil, errors.New("no queryHandler")
	}
	return h(s.query, named)
}

type fakeRows struct {
	rows [][]driver.Value
	cols []string
	idx  int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

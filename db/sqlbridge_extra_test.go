// Package db — PgxSQLBridge unit tests.
//
// The bridge proxies a *pgxpool.Pool behind the database/sql-shaped
// idempotent.SQLDB surface. No live Postgres is required: the row/result
// wrappers are exercised against fake pgx.Row / pgx.Rows implementations,
// and the pool-backed error paths run against a pool that was closed
// immediately after construction (pgxpool fails fast with
// pgxpool.ErrClosedPool instead of dialing).
package db

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// pgxRowSingle — ErrNoRows → io.EOF mapping.
// ---------------------------------------------------------------------------

type stubPGXRow struct {
	err error
}

func (r stubPGXRow) Scan(_ ...interface{}) error { return r.err }

func TestPgxRowSingleScan_ErrNoRowsMapsToEOF(t *testing.T) {
	t.Parallel()
	row := &pgxRowSingle{r: stubPGXRow{err: pgx.ErrNoRows}}
	err := row.Scan(&struct{}{})
	if err != io.EOF {
		t.Fatalf("Scan() = %v, want io.EOF", err)
	}
}

func TestPgxRowSingleScan_OtherErrorPassesThrough(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	row := &pgxRowSingle{r: stubPGXRow{err: sentinel}}
	err := row.Scan(&struct{}{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Scan() = %v, want original sentinel", err)
	}
}

func TestPgxRowSingleScan_Success(t *testing.T) {
	t.Parallel()
	row := &pgxRowSingle{r: stubPGXRow{}}
	if err := row.Scan(&struct{}{}); err != nil {
		t.Fatalf("Scan() = %v, want nil", err)
	}
}

// ---------------------------------------------------------------------------
// pgxResult — database/sql Result surface.
// ---------------------------------------------------------------------------

func TestPgxResult_LastInsertIdUnsupported(t *testing.T) {
	t.Parallel()
	r := &pgxResult{rowsAffected: 3}
	if _, err := r.LastInsertId(); err == nil {
		t.Fatal("expected error: Postgres has no LastInsertId")
	}
	id, err := r.LastInsertId()
	if err == nil || id != 0 {
		t.Errorf("LastInsertId() = (%d, %v), want (0, error)", id, err)
	}
}

func TestPgxResult_RowsAffected(t *testing.T) {
	t.Parallel()
	r := &pgxResult{rowsAffected: 42}
	n, err := r.RowsAffected()
	if err != nil {
		t.Fatalf("RowsAffected: %v", err)
	}
	if n != 42 {
		t.Errorf("RowsAffected = %d, want 42", n)
	}
}

// ---------------------------------------------------------------------------
// pgxRowsBridge — proxies to a pgx.Rows implementation.
// ---------------------------------------------------------------------------

type stubPGXRows struct {
	next    bool
	scanErr error
	rowsErr error
	closed  bool
}

func (r *stubPGXRows) Close()                                       { r.closed = true }
func (r *stubPGXRows) Err() error                                   { return r.rowsErr }
func (r *stubPGXRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *stubPGXRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *stubPGXRows) Next() bool                                   { return r.next }
func (r *stubPGXRows) Scan(dest ...any) error                       { return r.scanErr }
func (r *stubPGXRows) Values() ([]any, error)                       { return nil, nil }
func (r *stubPGXRows) RawValues() [][]byte                          { return nil }
func (r *stubPGXRows) Conn() *pgx.Conn                              { return nil }

func TestPgxRowsBridge_ProxiesToUnderlyingRows(t *testing.T) {
	t.Parallel()
	stub := &stubPGXRows{
		next:    true,
		scanErr: errors.New("scan failed"),
		rowsErr: errors.New("rows err"),
	}
	br := &pgxRowsBridge{r: stub}

	if got := br.Next(); !got {
		t.Error("Next() = false, want true (proxy)")
	}
	if err := br.Scan(&struct{}{}); !errors.Is(err, stub.scanErr) {
		t.Errorf("Scan() = %v, want scan sentinel", err)
	}
	if !errors.Is(br.Err(), stub.rowsErr) {
		t.Errorf("Err() = %v, want rows sentinel", br.Err())
	}
	if err := br.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
	if !stub.closed {
		t.Error("Close() did not proxy to underlying rows")
	}
}

// ---------------------------------------------------------------------------
// PgxSQLBridge pool-backed error paths (closed pool = fail fast, no dial).
// ---------------------------------------------------------------------------

// closedTestPool builds a pool from a parsed config and immediately closes
// it, so every subsequent operation fails with pgxpool.ErrClosedPool.
func closedTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	pool.Close()
	return pool
}

func TestPgxSQLBridge_ExecContextClosedPoolFails(t *testing.T) {
	t.Parallel()
	b := NewPgxSQLBridge(closedTestPool(t))
	_, err := b.ExecContext(context.Background(), "INSERT INTO t (a) VALUES ($1)", 1)
	if err == nil {
		t.Fatal("expected error on closed pool")
	}
	t.Logf("closed-pool error: %v", err)
}

func TestPgxSQLBridge_QueryContextClosedPoolFails(t *testing.T) {
	t.Parallel()
	b := PgxSQLBridge{pool: closedTestPool(t)}
	_, err := b.QueryContext(context.Background(), "SELECT 1")
	if err == nil {
		t.Fatal("expected error on closed pool")
	}
	t.Logf("closed-pool error: %v", err)
}

func TestPgxSQLBridge_QueryRowContextClosedPoolFails(t *testing.T) {
	t.Parallel()
	b := PgxSQLBridge{pool: closedTestPool(t)}
	row := b.QueryRowContext(context.Background(), "SELECT 1")
	var n int
	if err := row.Scan(&n); err == nil {
		t.Fatal("expected error on closed pool")
	}
}

// sqlbridge.go — pgx → database/sql shim for adapters that take an
// `idempotent.SQLDB` (or any other database/sql-shaped surface) but want
// to live on a single pgxpool.Pool.
//
// The chora-go-common/idempotent.PostgresStore takes a database/sql-
// compatible interface so it stays adapter-agnostic. Services that have
// already standardised on pgx would otherwise need a second pool just
// for idempotency, which doubles the connection footprint. This bridge
// avoids that — pgx executes the query, but the surface looks like
// database/sql to the caller.
//
// Resilience-priority directive: the bridge proxies every call straight
// through pgx; no in-memory state, no caching. Dead-pod recovery is
// handled by pgxpool's internal health-check loop.
package db

import (
	"context"
	"database/sql"
	"errors"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/5007-Capstone/chora/libs/chora-go-common/idempotent"
)

// PgxSQLBridge wraps a *pgxpool.Pool so it satisfies the
// idempotent.SQLDB interface. It is intentionally minimal — only the
// 3 calls idempotent.PostgresStore actually issues are implemented.
type PgxSQLBridge struct {
	pool *pgxpool.Pool
}

// NewPgxSQLBridge returns a bridge wrapping the supplied pool. Panics
// when pool is nil — services should fail-fast if they wire a nil pool.
func NewPgxSQLBridge(pool *pgxpool.Pool) *PgxSQLBridge {
	if pool == nil {
		panic("db: NewPgxSQLBridge: pool must not be nil")
	}
	return &PgxSQLBridge{pool: pool}
}

// ExecContext satisfies idempotent.SQLDB.ExecContext.
func (b *PgxSQLBridge) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	tag, err := b.pool.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &pgxResult{rowsAffected: tag.RowsAffected()}, nil
}

// QueryContext satisfies idempotent.SQLDB.QueryContext.
func (b *PgxSQLBridge) QueryContext(ctx context.Context, query string, args ...interface{}) (idempotent.SQLRows, error) {
	rows, err := b.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &pgxRowsBridge{r: rows}, nil
}

// QueryRowContext satisfies idempotent.SQLDB.QueryRowContext.
func (b *PgxSQLBridge) QueryRowContext(ctx context.Context, query string, args ...interface{}) idempotent.SQLRow {
	return &pgxRowSingle{r: b.pool.QueryRow(ctx, query, args...)}
}

// Compile-time check.
var _ idempotent.SQLDB = (*PgxSQLBridge)(nil)

// pgxResult satisfies sql.Result. pgx exposes RowsAffected on
// CommandTag; LastInsertId is not supported (Postgres uses RETURNING).
type pgxResult struct {
	rowsAffected int64
}

func (r *pgxResult) LastInsertId() (int64, error) {
	return 0, errors.New("pgx: LastInsertId not supported")
}
func (r *pgxResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

// pgxRows wraps pgx.Rows so it satisfies idempotent.SQLRows.
type pgxRowsBridge struct {
	r pgx.Rows
}

func (r *pgxRowsBridge) Next() bool                     { return r.r.Next() }
func (r *pgxRowsBridge) Scan(dest ...interface{}) error { return r.r.Scan(dest...) }
func (r *pgxRowsBridge) Close() error                   { r.r.Close(); return nil }
func (r *pgxRowsBridge) Err() error                     { return r.r.Err() }

// pgxRowSingle wraps a pgx.Row.
type pgxRowSingle struct {
	r pgx.Row
}

func (r *pgxRowSingle) Scan(dest ...interface{}) error {
	err := r.r.Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return io.EOF
	}
	return err
}

// Compile-time check that pgxRowSingle satisfies idempotent.SQLRow.
var _ idempotent.SQLRow = (*pgxRowSingle)(nil)

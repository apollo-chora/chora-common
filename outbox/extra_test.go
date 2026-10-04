// Package outbox — supplementary relay/recorder error-path tests.
package outbox_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/5007-Capstone/chora/libs/chora-go-common/outbox"
)

// claimErrRecorder embeds the in-memory recorder and overrides Claim to
// fail, exercising the relay's Claim-error propagation.
type claimErrRecorder struct {
	rec      *recorder
	claimErr error
}

func (c *claimErrRecorder) Record(ctx context.Context, tx outbox.Tx, row *outbox.Row) error {
	return c.rec.Record(ctx, tx, row)
}
func (c *claimErrRecorder) Claim(ctx context.Context, batchSize int) ([]*outbox.Row, error) {
	return nil, c.claimErr
}
func (c *claimErrRecorder) MarkPublished(ctx context.Context, ids []string) error {
	return c.rec.MarkPublished(ctx, ids)
}
func (c *claimErrRecorder) MarkFailed(ctx context.Context, id, errMsg string, deadletter bool) error {
	return c.rec.MarkFailed(ctx, id, errMsg, deadletter)
}

func TestRelay_Drain_PropagatesClaimError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("claim exploded")
	relay := outbox.NewRelay(&claimErrRecorder{rec: newRecorder(), claimErr: wantErr}, newCapturingPublisher(), outbox.RelayConfig{
		BatchSize:    10,
		PollInterval: time.Millisecond,
	})
	_, err := relay.Drain(context.Background())
	if err == nil || !strings.Contains(err.Error(), "outbox.Relay.Claim") {
		t.Fatalf("err = %v, want Claim wrap", err)
	}
}

func TestRelay_Start_PropagatesClaimError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("claim exploded")
	relay := outbox.NewRelay(&claimErrRecorder{rec: newRecorder(), claimErr: wantErr}, newCapturingPublisher(), outbox.RelayConfig{
		BatchSize:    10,
		PollInterval: time.Millisecond,
	})
	err := relay.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "outbox.Relay.Claim") {
		t.Fatalf("Start err = %v, want Claim wrap", err)
	}
}

// invalidTableRecorder uses a table name that fails the identifier regex,
// exercising checkTable() in Record / Claim / MarkFailed.
func invalidTableRecorder() *outbox.PostgresRecorder {
	return outbox.NewPostgresRecorder(nil, outbox.PostgresOptions{Table: "bad-name!"})
}

func TestPostgresRecorder_InvalidTableName_AllMethodsFail(t *testing.T) {
	t.Parallel()
	rec := invalidTableRecorder()
	if err := rec.Record(context.Background(), nil, &outbox.Row{}); err == nil || !strings.Contains(err.Error(), "invalid table name") {
		t.Errorf("Record err = %v, want invalid table name", err)
	}
	if _, err := rec.Claim(context.Background(), 5); err == nil || !strings.Contains(err.Error(), "invalid table name") {
		t.Errorf("Claim err = %v, want invalid table name", err)
	}
	if err := rec.MarkFailed(context.Background(), "id", "boom", true); err == nil || !strings.Contains(err.Error(), "invalid table name") {
		t.Errorf("MarkFailed err = %v, want invalid table name", err)
	}
}

func TestPostgresRecorder_Claim_ScanError(t *testing.T) {
	t.Parallel()
	env := newValidEnvelope()
	envJSON := mustEnvelopeJSON(env)
	now := time.Now().UTC()
	stub := &pgStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			// status column supplied as int → Scan into *string fails.
			return newRows(
				[]string{"id", "aggregate_type", "aggregate_id", "event_type", "topic",
					"payload", "envelope", "occurred_at", "status", "retry_count", "last_error"},
				[][]driver.Value{{"row-1", "atom", "a", "evt", "topic", []byte("p"), envJSON, now, int64(1), int64(0), ""}},
			), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if _, err := rec.Claim(context.Background(), 5); err == nil || !strings.Contains(err.Error(), "outbox.Claim scan") {
		t.Errorf("Claim err = %v, want scan wrap", err)
	}
}

func TestPostgresRecorder_Claim_EnvelopeParseError(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	stub := &pgStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			return newRows(
				[]string{"id", "aggregate_type", "aggregate_id", "event_type", "topic",
					"payload", "envelope", "occurred_at", "status", "retry_count", "last_error"},
				[][]driver.Value{{"row-1", "atom", "a", "evt", "topic", []byte("p"), []byte("{not json"), now, "pending", int64(0), ""}},
			), nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if _, err := rec.Claim(context.Background(), 5); err == nil || !strings.Contains(err.Error(), "outbox.Claim envelope") {
		t.Errorf("Claim err = %v, want envelope wrap", err)
	}
}

func TestPostgresRecorder_Claim_RowsErr(t *testing.T) {
	t.Parallel()
	env := newValidEnvelope()
	envJSON := mustEnvelopeJSON(env)
	now := time.Now().UTC()
	wantErr := errors.New("iteration failed")
	stub := &pgStub{
		queryHandler: func(_ string, _ []driver.Value) (*sqlRows, error) {
			rows := newRows(
				[]string{"id", "aggregate_type", "aggregate_id", "event_type", "topic",
					"payload", "envelope", "occurred_at", "status", "retry_count", "last_error"},
				[][]driver.Value{{"row-1", "atom", "a", "evt", "topic", []byte("p"), envJSON, now, "pending", int64(0), ""}},
			)
			rows.err = wantErr
			return rows, nil
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	if _, err := rec.Claim(context.Background(), 5); err == nil || !errors.Is(err, wantErr) {
		t.Errorf("Claim err = %v, want rows.Err sentinel", err)
	}
}

func TestPostgresRecorder_MarkFailed_ExecError(t *testing.T) {
	t.Parallel()
	wantErr := errors.New("update failed")
	stub := &pgStub{
		execHandler: func(_ string, _ []driver.Value) (driver.Result, error) {
			return nil, wantErr
		},
	}
	rec := outbox.NewPostgresRecorder(stub, outbox.PostgresOptions{Table: "outbox_events"})
	err := rec.MarkFailed(context.Background(), "row-1", "boom", true)
	if err == nil || !strings.Contains(err.Error(), "outbox.MarkFailed") {
		t.Errorf("MarkFailed err = %v, want exec wrap", err)
	}
}

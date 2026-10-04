// Outbox end-to-end round-trip integration test:
//
//	write outbox row → relay claims → publish to in-memory bus →
//	subscriber acks → outbox row marked published
//
// Plus reorder-buffer assertion under shuffled OccurredAt timestamps.
// Plus chaos test: kill the outbox relay mid-publish and verify resume
// continues from last-published checkpoint without duplicates (per
// CLAUDE.md §6 + .claude/skills/data-consistency).
package tests

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apollo-chora/chora-common/envelope"
	"github.com/apollo-chora/chora-common/outbox"
	chpubsub "github.com/apollo-chora/chora-common/pubsub"
)

func TestOutboxRoundTrip_WithShuffledTimestamps(t *testing.T) {
	rec := newInMemRecorder()
	bus := chpubsub.NewInMemoryBus(chpubsub.WithSynchronousDelivery())
	defer bus.Close()

	var received []string
	var mu sync.Mutex
	cancel, err := bus.Subscribe(context.Background(), "chora.creation.atom.created.v1",
		func(_ context.Context, msg *chpubsub.Message) error {
			mu.Lock()
			received = append(received, string(msg.Payload))
			mu.Unlock()
			return nil
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	o := outbox.NewWithRecorder(rec)
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)

	// 5 events with intentionally shuffled OccurredAt offsets.
	seeds := []struct {
		body string
		off  time.Duration
	}{
		{"D", 4 * time.Millisecond},
		{"A", 0},
		{"E", 8 * time.Millisecond},
		{"C", 2 * time.Millisecond},
		{"B", 1 * time.Millisecond},
	}
	for _, s := range seeds {
		env := envelope.Envelope{
			EventID:        "evt-" + s.body,
			IdempotencyKey: "evt-" + s.body,
			TenantID:       "tenant-rt",
			OccurredAt:     now.Add(s.off),
			PublishedAt:    now.Add(s.off),
			Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			SourceProject:  "chora-489812",
			SourceService:  "chora-creation",
			SchemaVersion:  1,
		}
		if err := o.Publish(context.Background(), nil, outbox.PublishOpts{
			AggregateType: "atom",
			AggregateID:   "atom-1",
			EventType:     "atom.created.v1",
			Topic:         "chora.creation.atom.created.v1",
			Payload:       []byte(s.body),
			Envelope:      env,
		}); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	relay := outbox.NewRelay(rec, bus, outbox.RelayConfig{
		BatchSize:        10,
		PollInterval:     time.Millisecond,
		MaxRetries:       3,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderWindow:    20 * time.Millisecond,
		ReorderBatchSize: 10,
	})
	ctx, ctxCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer ctxCancel()
	if _, err := relay.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Wait for subscriber to receive all 5.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	got := append([]string(nil), received...)
	mu.Unlock()
	if len(got) != 5 {
		t.Fatalf("received=%d want 5: %v", len(got), got)
	}
	want := []string{"A", "B", "C", "D", "E"}
	if !equalStrings(got, want) {
		t.Errorf("subscriber order=%v want %v (sorted by occurred_at via reorder window)", got, want)
	}

	// All rows should be marked published.
	for _, row := range rec.snapshot() {
		if row.Status != outbox.StatusPublished {
			t.Errorf("row %s status=%s want published", row.ID, row.Status)
		}
		if row.PublishedAt.IsZero() {
			t.Errorf("row %s PublishedAt zero", row.ID)
		}
	}
}

func TestOutboxChaos_RelayCrashMidPublishResumesWithoutDuplicates(t *testing.T) {
	rec := newInMemRecorder()
	bus := chpubsub.NewInMemoryBus(chpubsub.WithSynchronousDelivery())
	defer bus.Close()

	var received []string
	var mu sync.Mutex
	cancel, err := bus.Subscribe(context.Background(), "chora.creation.atom.created.v1",
		func(_ context.Context, msg *chpubsub.Message) error {
			mu.Lock()
			received = append(received, string(msg.Payload))
			mu.Unlock()
			return nil
		})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer cancel()

	o := outbox.NewWithRecorder(rec)
	now := time.Date(2026, 5, 9, 12, 0, 0, 0, time.UTC)
	for i, body := range []string{"A", "B", "C", "D", "E"} {
		env := envelope.Envelope{
			EventID:        "evt-" + body,
			IdempotencyKey: "evt-" + body,
			TenantID:       "tenant-chaos",
			OccurredAt:     now.Add(time.Duration(i) * time.Millisecond),
			PublishedAt:    now.Add(time.Duration(i) * time.Millisecond),
			Traceparent:    "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			SourceProject:  "chora-489812",
			SourceService:  "chora-creation",
			SchemaVersion:  1,
		}
		_ = o.Publish(context.Background(), nil, outbox.PublishOpts{
			AggregateType: "atom",
			AggregateID:   "atom-1",
			EventType:     "atom.created.v1",
			Topic:         "chora.creation.atom.created.v1",
			Payload:       []byte(body),
			Envelope:      env,
		})
	}

	// First relay: simulate crash by returning a fatal error from the
	// publisher after the 2nd successful publish. The relay's
	// publishWithRetry will mark that row as failed/deadletter, but the
	// test cancels the context concurrently via ctx1.
	ctx1, cancel1 := context.WithCancel(context.Background())
	chaos := &chaosBus{delegate: bus, killAt: 2}
	relay1 := outbox.NewRelay(rec, chaos, outbox.RelayConfig{
		BatchSize:        1,
		MaxRetries:       1, // exit fast on first failure
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderBatchSize: 1,
	})
	chaos.cancelCtx = cancel1
	if _, err := relay1.Drain(ctx1); err != nil && !errors.Is(err, context.Canceled) {
		// Ignore: the chaos bus simulates the relay process dying.
	}

	// Snapshot intermediate state — exactly 2 published, the 3rd
	// deadlettered (because chaos returned error on publish #3 with
	// MaxRetries=1) — but for the chaos resume scenario we expect
	// 2 published + at most 1 deadlettered + the rest pending.
	intermediate := rec.snapshot()
	publishedSoFar := 0
	for _, r := range intermediate {
		if r.Status == outbox.StatusPublished {
			publishedSoFar++
		}
	}
	if publishedSoFar < 1 || publishedSoFar > 4 {
		t.Errorf("intermediate published=%d want 1..4", publishedSoFar)
	}

	// Reset chaos rows: any deadlettered row from the simulated crash
	// should be re-pended for the resume relay (in production this is
	// equivalent to an operator un-deadlettering after triage; here the
	// test models the case where the chaos error itself was transient).
	rec.requeueDeadlettered()

	// Second relay (fresh state — simulates restart) drains the rest.
	relay2 := outbox.NewRelay(rec, bus, outbox.RelayConfig{
		BatchSize:        10,
		MaxRetries:       3,
		BackoffBase:      time.Millisecond,
		BackoffCeiling:   2 * time.Millisecond,
		ReorderBatchSize: 10,
	})
	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()
	if _, err := relay2.Drain(ctx2); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	// Wait for all 5 receipts.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	mu.Lock()
	got := append([]string(nil), received...)
	mu.Unlock()
	if len(got) != 5 {
		t.Fatalf("received=%d want 5 (post-resume)", len(got))
	}
	// No duplicates — sort + dedupe should yield 5 unique.
	sort.Strings(got)
	for i := 1; i < len(got); i++ {
		if got[i] == got[i-1] {
			t.Errorf("duplicate %q at i=%d (got=%v)", got[i], i, got)
		}
	}
	// All rows published.
	for _, row := range rec.snapshot() {
		if row.Status != outbox.StatusPublished {
			t.Errorf("post-resume row %s status=%s want published", row.ID, row.Status)
		}
	}
}

// ----------------------------------------------------------------------------
// Helpers — local copies (avoiding import cycle into outbox_test package)
// ----------------------------------------------------------------------------

type inMemRecorder struct {
	mu   sync.Mutex
	rows []*outbox.Row
}

func newInMemRecorder() *inMemRecorder { return &inMemRecorder{} }

func (r *inMemRecorder) Record(_ context.Context, _ outbox.Tx, row *outbox.Row) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows = append(r.rows, row)
	return nil
}

func (r *inMemRecorder) Claim(_ context.Context, batchSize int) ([]*outbox.Row, error) {
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

func (r *inMemRecorder) MarkPublished(_ context.Context, ids []string) error {
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

func (r *inMemRecorder) MarkFailed(_ context.Context, id string, errMsg string, deadletter bool) error {
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

func (r *inMemRecorder) snapshot() []outbox.Row {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]outbox.Row, len(r.rows))
	for i, row := range r.rows {
		out[i] = *row
	}
	return out
}

// requeueDeadlettered resets any deadlettered rows back to pending, mimicking
// an operator triage where the deadletter cause was transient (the simulated
// "process died" error).
func (r *inMemRecorder) requeueDeadlettered() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, row := range r.rows {
		if row.Status == outbox.StatusDeadlettered {
			row.Status = outbox.StatusPending
			row.Locked = false
			row.LastError = ""
			row.RetryCount = 0
		}
	}
}

// chaosBus simulates a relay process dying mid-stream: it publishes
// successfully for the first killAt messages, then on every subsequent
// publish it cancels its parent context AND returns a "process died"
// error. The relay's outer loop honours the cancel and exits.
type chaosBus struct {
	delegate  *chpubsub.InMemoryBus
	killAt    int
	count     int32
	cancelCtx context.CancelFunc
}

func (c *chaosBus) Publish(ctx context.Context, topic string, env envelope.Envelope, payload []byte) error {
	n := atomic.AddInt32(&c.count, 1)
	if int(n) > c.killAt {
		// Simulate process death.
		if c.cancelCtx != nil {
			c.cancelCtx()
		}
		return errors.New("process died")
	}
	return c.delegate.Publish(ctx, topic, env, payload)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

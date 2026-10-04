// request_deadline_test.go: ExtendRequestDeadlines tests.
package choraserver_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	choraserver "github.com/5007-Capstone/chora/libs/chora-go-common/http"
)

// deadlineRecorder is a ResponseWriter that supports connection deadlines,
// recording what ExtendRequestDeadlines sets. http.NewResponseController finds
// SetReadDeadline / SetWriteDeadline on it by interface assertion.
type deadlineRecorder struct {
	http.ResponseWriter
	readDeadline  time.Time
	writeDeadline time.Time
}

func (d *deadlineRecorder) SetReadDeadline(t time.Time) error  { d.readDeadline = t; return nil }
func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error { d.writeDeadline = t; return nil }

func TestExtendRequestDeadlines_SetsBothDeadlines(t *testing.T) {
	rec := &deadlineRecorder{ResponseWriter: httptest.NewRecorder()}
	before := time.Now()

	if err := choraserver.ExtendRequestDeadlines(rec, 120*time.Second); err != nil {
		t.Fatalf("ExtendRequestDeadlines: %v", err)
	}

	// Both deadlines must land ~120s out, and be equal to each other.
	wantLo := before.Add(119 * time.Second)
	wantHi := before.Add(121 * time.Second)
	if rec.readDeadline.Before(wantLo) || rec.readDeadline.After(wantHi) {
		t.Errorf("read deadline %v not within [~120s] of %v", rec.readDeadline, before)
	}
	if rec.writeDeadline.Before(wantLo) || rec.writeDeadline.After(wantHi) {
		t.Errorf("write deadline %v not within [~120s] of %v", rec.writeDeadline, before)
	}
	if !rec.readDeadline.Equal(rec.writeDeadline) {
		t.Errorf("read %v and write %v deadlines should match", rec.readDeadline, rec.writeDeadline)
	}
}

func TestExtendRequestDeadlines_UnsupportedWriterDegrades(t *testing.T) {
	// A plain recorder does not support connection deadlines. The helper must
	// return the not-supported error (so the caller can log it) WITHOUT
	// panicking, so the route falls back to the server-wide timeout.
	rec := httptest.NewRecorder()

	err := choraserver.ExtendRequestDeadlines(rec, 120*time.Second)
	if err == nil {
		t.Fatal("expected a not-supported error from a plain recorder")
	}
	if !errors.Is(err, http.ErrNotSupported) {
		t.Fatalf("want http.ErrNotSupported, got %v", err)
	}
}

func TestExtendRequestDeadlines_RejectsNonPositive(t *testing.T) {
	rec := &deadlineRecorder{ResponseWriter: httptest.NewRecorder()}
	if err := choraserver.ExtendRequestDeadlines(rec, 0); err == nil {
		t.Fatal("expected an error for a non-positive duration")
	}
	if !rec.readDeadline.IsZero() {
		t.Fatal("no deadline should be set when the duration is rejected")
	}
}

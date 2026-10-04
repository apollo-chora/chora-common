package tracing

// traceparent_internal_test.go — internal-package tests for the
// unexported branches of tracing/traceparent.go: isAllZero's all-zeros
// return and statusRecorder.Hijack's non-hijacker rejection (plus the
// wrong-typed user-roles context read). Test-only; does not weaken
// existing assertions in tracing_test.go / hijack_test.go.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// deadlineCapableRecorder is a ResponseWriter that supports connection
// deadlines, so http.NewResponseController can reach them THROUGH a
// statusRecorder wrapper only if statusRecorder unwraps.
type deadlineCapableRecorder struct {
	*httptest.ResponseRecorder
	readDeadline  time.Time
	writeDeadline time.Time
}

func (d *deadlineCapableRecorder) SetReadDeadline(t time.Time) error { d.readDeadline = t; return nil }
func (d *deadlineCapableRecorder) SetWriteDeadline(t time.Time) error {
	d.writeDeadline = t
	return nil
}

// TestStatusRecorder_UnwrapExposesDeadlineControl guards the wiring the
// AI-Assist upload deadline fix depends on: the tracing middleware wraps the
// ResponseWriter in a statusRecorder, and http.NewResponseController must be
// able to reach the underlying connection's SetReadDeadline through it. Without
// an Unwrap method the controller returns ErrNotSupported and every per-route
// deadline extension is a silent no-op behind the middleware.
func TestStatusRecorder_UnwrapExposesDeadlineControl(t *testing.T) {
	underlying := &deadlineCapableRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: underlying}

	rc := http.NewResponseController(sr)
	deadline := time.Now().Add(90 * time.Second)
	if err := rc.SetReadDeadline(deadline); err != nil {
		t.Fatalf("SetReadDeadline through statusRecorder: %v (statusRecorder must Unwrap to the real writer)", err)
	}
	if err := rc.SetWriteDeadline(deadline); err != nil {
		t.Fatalf("SetWriteDeadline through statusRecorder: %v", err)
	}
	if !underlying.readDeadline.Equal(deadline) {
		t.Fatalf("read deadline did not reach the underlying writer: got %v want %v", underlying.readDeadline, deadline)
	}
	if !underlying.writeDeadline.Equal(deadline) {
		t.Fatalf("write deadline did not reach the underlying writer: got %v want %v", underlying.writeDeadline, deadline)
	}
}

func TestIsAllZero(t *testing.T) {
	if !isAllZero([]byte{0, 0, 0}) {
		t.Error("isAllZero([0 0 0]) = false, want true")
	}
	if isAllZero([]byte{0, 1, 0}) {
		t.Error("isAllZero([0 1 0]) = true, want false")
	}
	if !isAllZero(nil) {
		t.Error("isAllZero(nil) = false, want true (empty loop)")
	}
}

func TestStatusRecorder_Hijack_RejectsNonHijacker(t *testing.T) {
	// httptest.ResponseRecorder is NOT an http.Hijacker — Hijack must
	// surface a precise error instead of silently masking the interface.
	sr := &statusRecorder{ResponseWriter: httptest.NewRecorder()}
	conn, rw, err := sr.Hijack()
	if conn != nil || rw != nil {
		t.Errorf("Hijack returned non-nil (conn=%v rw=%v) on a non-hijacker writer", conn, rw)
	}
	if err == nil {
		t.Fatal("expected Hijack error on a non-hijacker underlying writer")
	}
}

// TestUserRolesFromContext_WrongTypedValue ensures the string type
// assertion is the only accepted shape under the user-roles key.
func TestUserRolesFromContext_WrongTypedValue(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxKeyUserRoles, 42)
	if got := UserRolesFromContext(ctx); got != "" {
		t.Errorf("UserRolesFromContext(non-string) = %q, want empty", got)
	}
}

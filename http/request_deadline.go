// request_deadline.go: per-route connection deadline extension.
//
// The Chora HTTP servers set a deliberately tight server-wide
// ReadTimeout/WriteTimeout (15s) as slowloris protection. A few routes take a
// legitimately long single request, notably the AI-Assist batch upload, which
// streams a multi-megabyte multipart body and can exceed 15s on the wire alone.
// Those requests were severed mid-upload (HTTP 504) before the job was ever
// created. ExtendRequestDeadlines lifts the deadline for that ONE request via
// http.NewResponseController, leaving every other route on the tight default.
package choraserver

import (
	"fmt"
	"net/http"
	"time"
)

// ExtendRequestDeadlines pushes the read AND write deadlines of the underlying
// connection out to now+d for the current request, overriding the server-wide
// ReadTimeout/WriteTimeout for this route only. It is the sanctioned way to let
// a single slow upload complete without loosening the server default.
//
// A ResponseWriter that does not support connection deadlines (a test recorder,
// or a wrapper that drops the capability) yields http.ErrNotSupported, which is
// returned rather than panicked so the caller can log it and continue: the route
// then falls back to the server-wide timeout instead of failing outright.
func ExtendRequestDeadlines(w http.ResponseWriter, d time.Duration) error {
	if d <= 0 {
		return fmt.Errorf("choraserver: deadline duration must be positive, got %s", d)
	}
	rc := http.NewResponseController(w)
	deadline := time.Now().Add(d)
	if err := rc.SetReadDeadline(deadline); err != nil {
		return fmt.Errorf("choraserver: set read deadline: %w", err)
	}
	if err := rc.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("choraserver: set write deadline: %w", err)
	}
	return nil
}

package tracing_test

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/apollo-chora/chora-common/tracing"
)

// hijackableRW is an httptest.ResponseRecorder that also implements
// http.Hijacker + http.Flusher — emulating the real net/http HTTP/1.1
// ResponseWriter so we can assert the tracing middleware's statusRecorder
// wrapper does not mask those interfaces.
type hijackableRW struct {
	*httptest.ResponseRecorder
	hijacked bool
	flushed  bool
}

func (h *hijackableRW) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h.hijacked = true
	return nil, nil, nil
}

func (h *hijackableRW) Flush() { h.flushed = true }

// TestMiddleware_PreservesHijacker guards the WebSocket-upgrade transport path
// (chora-gateway's rplus-delivery WS hijack). Before the fix the statusRecorder
// embedded the http.ResponseWriter *interface* and never delegated Hijack, so
// w.(http.Hijacker) failed downstream with GATEWAY_HIJACK_UNSUPPORTED even over
// HTTP/1.1.
func TestMiddleware_PreservesHijacker(t *testing.T) {
	mw := tracing.Middleware()

	var innerIsHijacker bool
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		innerIsHijacker = ok
		if ok {
			_, _, _ = hj.Hijack()
		}
	}))

	rw := &hijackableRW{ResponseRecorder: httptest.NewRecorder()}
	h.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/api/v1/live-quizzes/x/ws", nil))

	if !innerIsHijacker {
		t.Fatal("statusRecorder must expose http.Hijacker so WS-upgrade proxies can hijack the conn")
	}
	if !rw.hijacked {
		t.Fatal("Hijack must delegate to the underlying ResponseWriter")
	}
}

// TestMiddleware_PreservesFlusher guards SSE streaming (e.g. H+ tx-history
// /stream) — the wrapper must also forward Flush.
func TestMiddleware_PreservesFlusher(t *testing.T) {
	mw := tracing.Middleware()
	var innerIsFlusher bool
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fl, ok := w.(http.Flusher)
		innerIsFlusher = ok
		if ok {
			fl.Flush()
		}
	}))
	rw := &hijackableRW{ResponseRecorder: httptest.NewRecorder()}
	h.ServeHTTP(rw, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if !innerIsFlusher {
		t.Fatal("statusRecorder must expose http.Flusher for SSE streaming")
	}
	if !rw.flushed {
		t.Fatal("Flush must delegate to the underlying ResponseWriter")
	}
}

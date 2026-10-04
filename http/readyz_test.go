// Package choraserver — /readyz handler RED phase tests.
//
// Per E2E-INFRA-COLD-START §B (Infra option 1, accepted at commit
// 2292a9d1). Strict k8s ready vs live semantic:
//
//	/healthz — 200 once the HTTP server is listening (process alive).
//	          Does NOT check pool or outbox. Survives DB outage.
//	/readyz — 200 ONLY after pgxpool.Ping succeeds AND outbox table
//	          is reachable. 503 otherwise. Gates EndpointSlice
//	          publication so traffic only flows to pods that can
//	          actually persist + emit events.
//
// Rationale: cold-start race surfaced at INFRA-LEG3-D — a pod's HTTP
// server starts listening before pool warm-up finishes; without a
// real readiness check, GCLB / EndpointSlice flips it to Ready and
// the first inbound request fails on a not-yet-pingable pool.
package choraserver_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	choraserver "github.com/5007-Capstone/chora/libs/chora-go-common/http"
)

// stubPinger is a Pinger stub: Ping returns whatever err is programmed.
// Captures call count to verify the handler invokes it per-request.
type stubPinger struct {
	calls int
	err   error
}

func (p *stubPinger) Ping(_ context.Context) error {
	p.calls++
	return p.err
}

// stubOutboxChecker is a OutboxChecker stub.
type stubOutboxChecker struct {
	calls int
	err   error
}

func (c *stubOutboxChecker) IsReachable(_ context.Context) error {
	c.calls++
	return c.err
}

func TestReadyz_AllChecksPass_Returns200(t *testing.T) {
	pool := &stubPinger{}
	outbox := &stubOutboxChecker{}
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName: "chora-delivery",
		Pool:        pool,
		Outbox:      outbox,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200; body=%s", rec.Code, rec.Body.String())
	}
	if pool.calls != 1 {
		t.Errorf("pool.Ping calls: got %d want 1", pool.calls)
	}
	if outbox.calls != 1 {
		t.Errorf("outbox.IsReachable calls: got %d want 1", outbox.calls)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v; raw=%s", err, rec.Body.String())
	}
	if body["status"] != "ready" {
		t.Errorf("status field: got %q want %q", body["status"], "ready")
	}
	if body["service"] != "chora-delivery" {
		t.Errorf("service field: got %q want %q", body["service"], "chora-delivery")
	}
}

func TestReadyz_PoolNotPingable_Returns503(t *testing.T) {
	pool := &stubPinger{err: errors.New("connection refused")}
	outbox := &stubOutboxChecker{}
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName: "chora-delivery",
		Pool:        pool,
		Outbox:      outbox,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503; body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["status"] != "not_ready" {
		t.Errorf("status field: got %q want %q", body["status"], "not_ready")
	}
	if body["pool"] != "unavailable" {
		t.Errorf("pool field: got %q want %q", body["pool"], "unavailable")
	}
}

func TestReadyz_OutboxUnreachable_Returns503(t *testing.T) {
	pool := &stubPinger{}
	outbox := &stubOutboxChecker{err: errors.New("relation outbox_events does not exist")}
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName: "chora-delivery",
		Pool:        pool,
		Outbox:      outbox,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503; body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["status"] != "not_ready" {
		t.Errorf("status field: got %q want %q", body["status"], "not_ready")
	}
	if body["outbox"] != "unavailable" {
		t.Errorf("outbox field: got %q want %q", body["outbox"], "unavailable")
	}
}

func TestReadyz_NilPool_Returns503(t *testing.T) {
	// Defensive: if a service wires the handler before pool is ready,
	// /readyz must report not_ready instead of panicking. This is the
	// expected state during the brief window between HTTP server
	// listening + pool Bootstrap completing.
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName: "chora-delivery",
		Pool:        nil,
		Outbox:      &stubOutboxChecker{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503; body=%s", rec.Code, rec.Body.String())
	}
}

func TestReadyz_NilOutbox_StillRequiresPool(t *testing.T) {
	// Some services may not run an outbox (e.g. read-only projections).
	// When Outbox is nil, /readyz reduces to "pool pingable" — but
	// production-grade services with outbox MUST wire Outbox; the
	// nil-outbox path is a tested escape hatch, not the default.
	pool := &stubPinger{}
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName: "chora-delivery",
		Pool:        pool,
		Outbox:      nil,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (pool-only mode); body=%s", rec.Code, rec.Body.String())
	}
	if pool.calls != 1 {
		t.Errorf("pool.Ping calls: got %d want 1", pool.calls)
	}
}

func TestReadyz_NotGET_ReturnsMethodNotAllowed(t *testing.T) {
	// /readyz is a probe-only endpoint; POST/PATCH/etc. must 405 so
	// callers cannot mutate any state via the probe handler.
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName: "chora-delivery",
		Pool:        &stubPinger{},
		Outbox:      &stubOutboxChecker{},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/readyz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d want 405; body=%s", rec.Code, rec.Body.String())
	}
}

func TestReadyz_RespectsCheckTimeout(t *testing.T) {
	// The handler must bound each check by ReadyzDeps.CheckTimeout —
	// a slow Ping that hangs longer than the timeout MUST return 503
	// in approximately that timeout (not hang forever).
	pool := &slowPinger{
		blockFor: 200 * time.Millisecond,
	}
	h := choraserver.ReadyzHandler(choraserver.ReadyzDeps{
		ServiceName:  "chora-delivery",
		Pool:         pool,
		Outbox:       nil,
		CheckTimeout: 50 * time.Millisecond,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	start := time.Now()
	h.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d want 503; body=%s", rec.Code, rec.Body.String())
	}
	if elapsed > 500*time.Millisecond {
		t.Errorf("CheckTimeout not honoured: handler took %s; expected ~50ms", elapsed)
	}
}

// slowPinger sleeps for blockFor before returning the programmed error.
// Used to test CheckTimeout enforcement.
type slowPinger struct {
	blockFor time.Duration
}

func (p *slowPinger) Ping(ctx context.Context) error {
	t := time.NewTimer(p.blockFor)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func TestHealthz_NotGET_ReturnsMethodNotAllowed(t *testing.T) {
	// /healthz is probe-only — POST/PATCH must 405 (parity with /readyz).
	h := choraserver.HealthzHandler(choraserver.HealthzDeps{
		ServiceName: "chora-delivery",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d want 405; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHealthz_StillReturns200_OnlyOnHTTPListening(t *testing.T) {
	// /healthz must continue to return 200 regardless of pool state —
	// liveness is about "is the process alive", NOT "can it serve
	// traffic". Disconnecting these two signals is the whole point of
	// /readyz vs /healthz. Note: HealthzDeps deliberately takes NO
	// pool/outbox — adding such a field would break liveness semantic.
	// The fact that this test compiles + passes pins the contract.
	h := choraserver.HealthzHandler(choraserver.HealthzDeps{
		ServiceName: "chora-delivery",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (liveness unchanged); body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status field: got %q want %q", body["status"], "ok")
	}
}

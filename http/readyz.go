// Package choraserver provides shared HTTP server primitives for Chora
// Go services: canonical /healthz + /readyz handlers with the strict
// k8s ready vs live semantic mandated by E2E-INFRA-COLD-START.
//
// Liveness vs readiness — the load-bearing distinction:
//
//   /healthz — "is the process alive?". 200 once the HTTP server is
//             listening. Does NOT check pool, outbox, or any
//             downstream dependency. A failing /healthz is the signal
//             for kubelet to kill the pod. Survives DB outage.
//
//   /readyz  — "can the pod actually serve traffic?". 200 ONLY when
//             pgxpool.Ping succeeds AND the outbox table is reachable
//             (when an outbox is wired). 503 otherwise. Gates k8s
//             EndpointSlice publication so GCLB / kube-proxy only
//             route to pods that can persist + emit events.
//
// Cold-start race surfaced at INFRA-LEG3-D — a pod's HTTP server
// starts listening before pool warm-up finishes; without a real
// readiness check, EndpointSlice flips the pod Ready and the first
// inbound request fails on a not-yet-pingable pool. The strict
// pool+outbox check fixes the race in conjunction with §3a
// startupProbe wired by chora-infra.
//
// Per `feedback_no_stubs_real_wiring`: NEVER fail-open or default to
// 200. When in doubt, return 503 — letting EndpointSlice withhold
// traffic until the pod is genuinely able to serve.
package choraserver

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// defaultCheckTimeout bounds each individual readiness check (pool
// ping + outbox check). Tuned for cold start: 2s gives a freshly-
// warming Cloud SQL Auth Proxy enough headroom to respond while
// still aborting before a startupProbe periodSeconds tick.
const defaultCheckTimeout = 2 * time.Second

// Pinger is the contract /readyz uses to probe the database pool.
// Satisfied by *pgxpool.Pool (which has Ping(ctx) error).
type Pinger interface {
	Ping(ctx context.Context) error
}

// OutboxChecker is the contract /readyz uses to probe the producer-
// side outbox table. Production wires an adapter that runs `SELECT 1
// FROM outbox_events LIMIT 1` (cheap; table-existence sentinel). The
// check distinguishes "outbox table missing" (which would silently
// drop event publishes) from "pool unavailable".
type OutboxChecker interface {
	IsReachable(ctx context.Context) error
}

// ReadyzDeps wires the canonical /readyz handler. Pool is required;
// Outbox is recommended for services that publish events. CheckTimeout
// defaults to 2s when zero.
type ReadyzDeps struct {
	ServiceName  string
	Pool         Pinger
	Outbox       OutboxChecker
	CheckTimeout time.Duration
}

// HealthzDeps wires the canonical /healthz handler. Intentionally
// minimal: no pool, no outbox, no downstream consultation. Adding
// such a field would break the liveness semantic.
type HealthzDeps struct {
	ServiceName string
}

// ReadyzHandler returns the canonical /readyz handler. The handler is
// strict: any failing check → 503 + a body field naming the failed
// component. All-checks-pass → 200 + {"status":"ready","service":...}.
//
// Per E2E-INFRA-COLD-START §B: pool MUST be pingable AND outbox MUST
// be reachable for the 200 response. A nil Pool always 503s
// (defensive — covers the brief boot window before Bootstrap returns).
// A nil Outbox is the documented escape hatch for read-only services
// that don't run an outbox; production services with outbox MUST wire
// it.
func ReadyzHandler(deps ReadyzDeps) http.Handler {
	timeout := deps.CheckTimeout
	if timeout == 0 {
		timeout = defaultCheckTimeout
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
				"error": "method not allowed",
			})
			return
		}

		body := map[string]string{
			"service": deps.ServiceName,
		}

		if deps.Pool == nil {
			body["status"] = "not_ready"
			body["pool"] = "unwired"
			writeJSON(w, http.StatusServiceUnavailable, body)
			return
		}

		// Pool ping — bounded by CheckTimeout.
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		if err := deps.Pool.Ping(ctx); err != nil {
			cancel()
			body["status"] = "not_ready"
			body["pool"] = "unavailable"
			body["pool_err"] = err.Error()
			writeJSON(w, http.StatusServiceUnavailable, body)
			return
		}
		cancel()

		// Outbox check — only when wired. Read-only services that
		// don't publish events legitimately leave Outbox nil.
		if deps.Outbox != nil {
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			if err := deps.Outbox.IsReachable(ctx); err != nil {
				cancel()
				body["status"] = "not_ready"
				body["pool"] = "ok"
				body["outbox"] = "unavailable"
				body["outbox_err"] = err.Error()
				writeJSON(w, http.StatusServiceUnavailable, body)
				return
			}
			cancel()
			body["outbox"] = "ok"
		}

		body["status"] = "ready"
		body["pool"] = "ok"
		writeJSON(w, http.StatusOK, body)
	})
}

// HealthzHandler returns the canonical /healthz handler. Always 200
// when the HTTP server is listening (i.e. when this handler is
// invocable). No pool / outbox / downstream consultation.
func HealthzHandler(deps HealthzDeps) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{
				"error": "method not allowed",
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"service": deps.ServiceName,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})
}

// writeJSON is the no-store JSON response helper for probe endpoints.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

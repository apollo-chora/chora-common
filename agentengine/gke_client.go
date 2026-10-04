package agentengine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// GKEReasoningEngineClient is a Client implementation that talks to an ADK
// agent's `web` mode REST surface running as a plain GKE Service (ADR-169 /
// CHO-1631 — Vertex AI Agent Engine decommissioned 2026-06-01). It differs
// from AgentEngineClient (the Vertex AI control-plane impl):
//
//   - Endpoints: POST {baseURL}/api/reasoning_engine (create/delete) +
//     /api/stream_reasoning_engine?alt=sse (stream) — the ADK launcher web
//     mode, NOT {resource}:query.
//   - Transport: cluster-local CLEARTEXT HTTP, NO bearer token (the caller
//     reaches the sidecar-less agent pod plaintext per the ADR-169 mesh
//     posture).
//   - Stream message: a Content object {role,parts}, mirroring the Python
//     ReasoningEngineExecutor (the proven gke:// caller). The ADK web server
//     accepts string|object; the object form is the canonical shape.
//   - Response: newline-delimited JSON events (one object per line, NO `data:`
//     prefix) — parsed by the shared ParseSSE (which tolerates both).
//
// EngineResource on the request structs is interpreted as the base URL
// (e.g. http://chora-familiar.ai-kernel.svc.cluster.local:8080). A `gke://`
// scheme is normalised to `http://`.
type GKEReasoningEngineClient struct {
	httpc    HTTPDoer
	crewKind string
	tracer   trace.Tracer
}

// GKEOption mutates a *GKEReasoningEngineClient during NewGKEClient.
type GKEOption func(*GKEReasoningEngineClient)

// WithGKEHTTPDoer injects the HTTP client (required).
func WithGKEHTTPDoer(d HTTPDoer) GKEOption {
	return func(c *GKEReasoningEngineClient) { c.httpc = d }
}

// WithGKECrewKind sets the chora.crew_kind span attr (D6 P4).
func WithGKECrewKind(k string) GKEOption {
	return func(c *GKEReasoningEngineClient) { c.crewKind = k }
}

// WithGKETracer overrides the default tracer.
func WithGKETracer(t trace.Tracer) GKEOption {
	return func(c *GKEReasoningEngineClient) { c.tracer = t }
}

// NewGKEClient constructs a GKE web-mode Client. HTTPDoer is required.
func NewGKEClient(opts ...GKEOption) (*GKEReasoningEngineClient, error) {
	c := &GKEReasoningEngineClient{tracer: otel.Tracer("chora.agentengine.gke")}
	for _, o := range opts {
		o(c)
	}
	if c.httpc == nil {
		return nil, errors.New("agentengine: GKE HTTPDoer required (WithGKEHTTPDoer)")
	}
	if c.crewKind == "" {
		c.crewKind = "unknown"
	}
	return c, nil
}

// normaliseGKEBase turns a gke:// scheme or bare host:port into an http://
// base URL, trimming any trailing slash. Empty input is ErrEngineNotConfigured.
func normaliseGKEBase(resource string) (string, error) {
	r := strings.TrimSpace(resource)
	if r == "" {
		return "", ErrEngineNotConfigured
	}
	r = strings.TrimPrefix(r, "gke://")
	if !strings.HasPrefix(r, "http://") && !strings.HasPrefix(r, "https://") {
		r = "http://" + r
	}
	return strings.TrimRight(r, "/"), nil
}

// CreateSession POSTs async_create_session to /api/reasoning_engine.
func (c *GKEReasoningEngineClient) CreateSession(ctx context.Context, req CreateSessionRequest) (string, error) {
	base, err := normaliseGKEBase(req.EngineResource)
	if err != nil {
		return "", err
	}
	if req.UserID == "" {
		return "", fmt.Errorf("%w: empty UserID", ErrInvalidRequest)
	}

	ctx, span := c.tracer.Start(ctx, "agentengine.gke.CreateSession")
	span.SetAttributes(SpanAttrs(SpanAttrsInput{CrewKind: c.crewKind, EngineResource: req.EngineResource})...)
	defer span.End()

	input := map[string]any{"user_id": req.UserID}
	if len(req.State) > 0 {
		input["state"] = req.State
	}
	resp, err := c.post(ctx, base+"/api/reasoning_engine",
		queryEnvelope{ClassMethod: "async_create_session", Input: input}, "application/json")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 500 {
		return "", c.wrapHTTPErr(resp, ErrEngineUnavailable)
	}
	if resp.StatusCode >= 400 {
		return "", c.wrapHTTPErr(resp, ErrSessionCreate)
	}

	var out struct {
		Output struct {
			ID string `json:"id"`
		} `json:"output"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("%w: decode session: %v", ErrSessionCreate, err)
	}
	if out.Output.ID == "" {
		return "", fmt.Errorf("%w: empty session id in response", ErrSessionCreate)
	}
	return out.Output.ID, nil
}

// StreamQuery POSTs async_stream_query to /api/stream_reasoning_engine?alt=sse
// and returns a channel of parsed events (ParseSSE handles the bare-JSON-per-
// line GKE output). The channel closes on stream end or ctx cancel.
func (c *GKEReasoningEngineClient) StreamQuery(ctx context.Context, req StreamQueryRequest) (<-chan StreamEvent, error) {
	base, err := normaliseGKEBase(req.EngineResource)
	if err != nil {
		return nil, err
	}
	if req.UserID == "" || req.SessionID == "" {
		return nil, fmt.Errorf("%w: UserID + SessionID required", ErrInvalidRequest)
	}

	ctx, span := c.tracer.Start(ctx, "agentengine.gke.StreamQuery")
	span.SetAttributes(SpanAttrs(SpanAttrsInput{CrewKind: c.crewKind, EngineResource: req.EngineResource})...)

	body := queryEnvelope{
		ClassMethod: "async_stream_query",
		Input: map[string]any{
			"user_id":    req.UserID,
			"session_id": req.SessionID,
			"message": map[string]any{
				"role":  "user",
				"parts": []map[string]any{{"text": req.Message}},
			},
		},
	}
	resp, err := c.post(ctx, base+"/api/stream_reasoning_engine?alt=sse", body, "text/event-stream")
	if err != nil {
		span.End()
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		span.End()
		if resp.StatusCode >= 500 {
			return nil, c.wrapHTTPErr(resp, ErrEngineUnavailable)
		}
		return nil, c.wrapHTTPErr(resp, ErrStreamAborted)
	}

	parsed := ParseSSE(ctx, resp.Body)
	out := make(chan StreamEvent, 16)
	go func() {
		defer close(out)
		defer span.End()
		defer resp.Body.Close()
		for ev := range parsed {
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// DeleteSession POSTs async_delete_session. Idempotent (404 → nil). Best-effort
// for the in-memory session service.
func (c *GKEReasoningEngineClient) DeleteSession(ctx context.Context, req DeleteSessionRequest) error {
	base, err := normaliseGKEBase(req.EngineResource)
	if err != nil {
		return err
	}
	if req.UserID == "" || req.SessionID == "" {
		return fmt.Errorf("%w: UserID + SessionID required", ErrInvalidRequest)
	}

	ctx, span := c.tracer.Start(ctx, "agentengine.gke.DeleteSession")
	span.SetAttributes(SpanAttrs(SpanAttrsInput{CrewKind: c.crewKind, EngineResource: req.EngineResource})...)
	defer span.End()

	resp, err := c.post(ctx, base+"/api/reasoning_engine",
		queryEnvelope{ClassMethod: "async_delete_session", Input: map[string]any{"user_id": req.UserID, "session_id": req.SessionID}},
		"application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 500 {
		return c.wrapHTTPErr(resp, ErrEngineUnavailable)
	}
	if resp.StatusCode >= 400 {
		return c.wrapHTTPErr(resp, ErrSessionCreate)
	}
	return nil
}

// post sends a cleartext JSON POST — NO bearer token (cluster-local plaintext).
func (c *GKEReasoningEngineClient) post(ctx context.Context, url string, body any, accept string) (*http.Response, error) {
	bs, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal body: %v", ErrInvalidRequest, err)
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bs))
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %v", ErrInvalidRequest, err)
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Accept", accept)
	resp, err := c.httpc.Do(r)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEngineUnavailable, err)
	}
	return resp, nil
}

func (c *GKEReasoningEngineClient) wrapHTTPErr(resp *http.Response, sentinel error) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return fmt.Errorf("%w: HTTP %d: %s", sentinel, resp.StatusCode, strings.TrimSpace(string(b)))
}

// Compile-time guarantee *GKEReasoningEngineClient satisfies the port.
var _ Client = (*GKEReasoningEngineClient)(nil)

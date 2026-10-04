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

// AgentEngineClient is the default Client implementation. Construct via New.
type AgentEngineClient struct {
	httpc            HTTPDoer
	tokens           TokenSource
	crewKind         string
	endpointOverride string // empty in prod; populated in tests pointing at httptest.Server
	tracer           trace.Tracer
}

// Option mutates a *AgentEngineClient during New construction.
type Option func(*AgentEngineClient)

// WithHTTPDoer injects the HTTP client used for outbound calls.
func WithHTTPDoer(d HTTPDoer) Option {
	return func(c *AgentEngineClient) { c.httpc = d }
}

// WithTokenSource injects the bearer-token producer (ADC in prod, Static in tests).
func WithTokenSource(t TokenSource) Option {
	return func(c *AgentEngineClient) { c.tokens = t }
}

// WithCrewKind sets the chora.crew_kind span attr stamped on every call.
// Required for D6 P4 trace emission.
func WithCrewKind(k string) Option {
	return func(c *AgentEngineClient) { c.crewKind = k }
}

// WithTracer overrides the default tracer (otel.Tracer("chora.agentengine")).
func WithTracer(t trace.Tracer) Option {
	return func(c *AgentEngineClient) { c.tracer = t }
}

// WithEndpointOverride redirects all REST calls to the given base URL. Tests
// use this to point at httptest.Server. Production callers MUST NOT set
// this; the canonical Vertex AI URL is derived from the engine resource.
func WithEndpointOverride(u string) Option {
	return func(c *AgentEngineClient) { c.endpointOverride = strings.TrimRight(u, "/") }
}

// New constructs an AgentEngineClient. HTTPDoer + TokenSource are required;
// CrewKind defaults to "unknown" if not provided.
func New(opts ...Option) (*AgentEngineClient, error) {
	c := &AgentEngineClient{
		tracer: otel.Tracer("chora.agentengine"),
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpc == nil {
		return nil, errors.New("agentengine: HTTPDoer required (WithHTTPDoer)")
	}
	if c.tokens == nil {
		return nil, errors.New("agentengine: TokenSource required (WithTokenSource)")
	}
	if c.crewKind == "" {
		c.crewKind = "unknown"
	}
	return c, nil
}

// endpointFor builds the v1beta1 REST endpoint for a resource_name, parsing
// the region from `locations/{region}/`.
func endpointFor(resource, suffix string) (string, error) {
	resource = strings.TrimSpace(resource)
	if resource == "" {
		return "", fmt.Errorf("%w: empty resource_name", ErrInvalidRequest)
	}
	const locPrefix = "locations/"
	idx := strings.Index(resource, locPrefix)
	if idx < 0 {
		return "", fmt.Errorf("%w: missing 'locations/' in %q", ErrInvalidRequest, resource)
	}
	rest := resource[idx+len(locPrefix):]
	slash := strings.Index(rest, "/")
	if slash <= 0 {
		return "", fmt.Errorf("%w: empty or malformed region in %q", ErrInvalidRequest, resource)
	}
	region := rest[:slash]
	return fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1beta1/%s%s", region, resource, suffix), nil
}

// queryEnvelope is the body wrapper for `:query` and `:streamQuery` calls.
type queryEnvelope struct {
	ClassMethod string         `json:"class_method"`
	Input       map[string]any `json:"input"`
}

// CreateSession POSTs to {engine}:query with async_create_session.
func (c *AgentEngineClient) CreateSession(ctx context.Context, req CreateSessionRequest) (string, error) {
	if req.EngineResource == "" {
		return "", ErrEngineNotConfigured
	}
	if req.UserID == "" {
		return "", fmt.Errorf("%w: empty UserID", ErrInvalidRequest)
	}

	ctx, span := c.tracer.Start(ctx, "agentengine.CreateSession")
	span.SetAttributes(SpanAttrs(SpanAttrsInput{
		CrewKind:       c.crewKind,
		EngineResource: req.EngineResource,
	})...)
	defer span.End()

	input := map[string]any{
		"user_id": req.UserID,
	}
	if len(req.State) > 0 {
		input["state"] = req.State
	}
	body := queryEnvelope{ClassMethod: "async_create_session", Input: input}

	resp, err := c.postJSON(ctx, req.EngineResource, ":query", body)
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

// StreamQuery POSTs to {engine}:streamQuery?alt=sse with async_stream_query
// and returns a channel of parsed events. The channel closes when the engine
// emits turn_complete=true or the context cancels.
func (c *AgentEngineClient) StreamQuery(ctx context.Context, req StreamQueryRequest) (<-chan StreamEvent, error) {
	if req.EngineResource == "" {
		return nil, ErrEngineNotConfigured
	}
	if req.UserID == "" || req.SessionID == "" {
		return nil, fmt.Errorf("%w: UserID + SessionID required", ErrInvalidRequest)
	}

	ctx, span := c.tracer.Start(ctx, "agentengine.StreamQuery")
	span.SetAttributes(SpanAttrs(SpanAttrsInput{
		CrewKind:       c.crewKind,
		EngineResource: req.EngineResource,
	})...)
	// span is intentionally NOT ended here — caller signals end via channel
	// close. We attach a closer goroutine that ends the span once the
	// channel terminates.

	body := queryEnvelope{
		ClassMethod: "async_stream_query",
		Input: map[string]any{
			"user_id":    req.UserID,
			"session_id": req.SessionID,
			"message":    req.Message,
		},
	}

	resp, err := c.postJSONStream(ctx, req.EngineResource, ":streamQuery?alt=sse", body)
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

// DeleteSession POSTs async_delete_session. Idempotent: 404 from the engine
// returns nil (already gone is a success state).
func (c *AgentEngineClient) DeleteSession(ctx context.Context, req DeleteSessionRequest) error {
	if req.EngineResource == "" {
		return ErrEngineNotConfigured
	}
	if req.UserID == "" || req.SessionID == "" {
		return fmt.Errorf("%w: UserID + SessionID required", ErrInvalidRequest)
	}

	ctx, span := c.tracer.Start(ctx, "agentengine.DeleteSession")
	span.SetAttributes(SpanAttrs(SpanAttrsInput{
		CrewKind:       c.crewKind,
		EngineResource: req.EngineResource,
	})...)
	defer span.End()

	body := queryEnvelope{
		ClassMethod: "async_delete_session",
		Input: map[string]any{
			"user_id":    req.UserID,
			"session_id": req.SessionID,
		},
	}
	resp, err := c.postJSON(ctx, req.EngineResource, ":query", body)
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

// postJSON sends a JSON body to {engine}{suffix} and returns the response.
func (c *AgentEngineClient) postJSON(ctx context.Context, resource, suffix string, body any) (*http.Response, error) {
	url, err := c.resolveURL(resource, suffix)
	if err != nil {
		return nil, err
	}
	return c.do(ctx, url, body, "application/json")
}

// postJSONStream is identical to postJSON but signals SSE in the Accept header.
func (c *AgentEngineClient) postJSONStream(ctx context.Context, resource, suffix string, body any) (*http.Response, error) {
	url, err := c.resolveURL(resource, suffix)
	if err != nil {
		return nil, err
	}
	return c.do(ctx, url, body, "text/event-stream")
}

// resolveURL builds the final URL, honoring endpointOverride for tests.
func (c *AgentEngineClient) resolveURL(resource, suffix string) (string, error) {
	if c.endpointOverride != "" {
		// In tests we don't need to parse the region — the override is the
		// test server's base URL.
		return c.endpointOverride + "/" + resource + suffix, nil
	}
	return endpointFor(resource, suffix)
}

func (c *AgentEngineClient) do(ctx context.Context, url string, body any, accept string) (*http.Response, error) {
	bs, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal body: %v", ErrInvalidRequest, err)
	}
	tok, err := c.tokens.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: token: %v", ErrEngineUnavailable, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bs))
	if err != nil {
		return nil, fmt.Errorf("%w: build request: %v", ErrInvalidRequest, err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	resp, err := c.httpc.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: %v", ErrEngineTimeout, err)
		}
		return nil, fmt.Errorf("%w: %v", ErrEngineUnavailable, err)
	}
	return resp, nil
}

func (c *AgentEngineClient) wrapHTTPErr(resp *http.Response, sentinel error) error {
	excerpt := readExcerpt(resp.Body, 256)
	return &EngineError{Sentinel: sentinel, HTTPStatus: resp.StatusCode, BodyExcerpt: excerpt}
}

func readExcerpt(r io.Reader, max int) string {
	if r == nil {
		return ""
	}
	buf := make([]byte, max)
	n, _ := r.Read(buf)
	return strings.TrimSpace(string(buf[:n]))
}

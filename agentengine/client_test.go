package agentengine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// otelTestTracer is the no-op tracer used to verify WithTracer plumbing.
var _ trace.Tracer = (*otelTestTracerImpl)(nil)

type otelTestTracerImpl struct{ noop.Tracer }

type otelTestTracer = otelTestTracerImpl

const fixtureEngine = "projects/381315455325/locations/us-central1/reasoningEngines/6115726116004036608"

func TestEndpointForResource_ParsesRegionAndAppendsSuffix(t *testing.T) {
	got, err := endpointFor(fixtureEngine, ":query")
	if err != nil {
		t.Fatalf("endpointFor: %v", err)
	}
	want := "https://us-central1-aiplatform.googleapis.com/v1beta1/" + fixtureEngine + ":query"
	if got != want {
		t.Errorf("endpoint = %q\nwant %q", got, want)
	}
}

func TestEndpointForResource_RejectsBadResourceName(t *testing.T) {
	cases := []string{
		"",
		"projects/123",
		"projects/123/locations/", // empty region
		"completely-wrong",
	}
	for _, c := range cases {
		if _, err := endpointFor(c, ":query"); err == nil {
			t.Errorf("expected error for resource %q; got nil", c)
		}
	}
}

func TestNew_RequiresHTTPDoerAndTokens(t *testing.T) {
	if _, err := New(); err == nil {
		t.Error("expected error: missing both HTTPDoer and TokenSource")
	}
	if _, err := New(WithHTTPDoer(http.DefaultClient)); err == nil {
		t.Error("expected error: missing TokenSource")
	}
}

func TestNew_SucceedsWithMinimalConfig(t *testing.T) {
	c, err := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("x")),
		WithCrewKind("test_crew"),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c == nil {
		t.Fatal("New returned nil client")
	}
}

func TestCreateSession_PostsCorrectJSONAndReturnsSessionID(t *testing.T) {
	var capturedReq map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, ":query") {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Errorf("Authorization = %q; want %q", got, "Bearer test-token")
		}
		body, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(body, &capturedReq); err != nil {
			t.Fatalf("body unmarshal: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"output":{"id":"sess-abc-123","userId":"gcid-x","state":{}}}`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("test-token")),
		WithCrewKind("test_crew"),
		WithEndpointOverride(srv.URL),
	)

	sid, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine,
		UserID:         "gcid-x",
		State: map[string]any{
			"tenant_id":  "tenant-y",
			"user_gcid":  "gcid-x",
			"mana_tier":  "premium",
			"familiar_id": "newton",
		},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if sid != "sess-abc-123" {
		t.Errorf("session id = %q; want %q", sid, "sess-abc-123")
	}

	if capturedReq["class_method"] != "async_create_session" {
		t.Errorf("class_method = %v; want %q", capturedReq["class_method"], "async_create_session")
	}
	input, ok := capturedReq["input"].(map[string]any)
	if !ok {
		t.Fatalf("input missing/wrong type: %T", capturedReq["input"])
	}
	if input["user_id"] != "gcid-x" {
		t.Errorf("input.user_id = %v; want gcid-x", input["user_id"])
	}
	if state, ok := input["state"].(map[string]any); !ok || state["mana_tier"] != "premium" {
		t.Errorf("state.mana_tier missing; got %v", input["state"])
	}
}

func TestCreateSession_FailsLoudOnMissingEngineResource(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("x")),
		WithCrewKind("t"),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{UserID: "g"})
	if !errors.Is(err, ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
}

func TestCreateSession_Returns503OnEngineFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"engine cold"}`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine,
		UserID:         "g",
	})
	if err == nil {
		t.Fatal("expected error on 503")
	}
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestStreamQuery_StreamsSSEEventsAsStreamEvents(t *testing.T) {
	fixture := `{"author":"familiar_companion","content":{"parts":[{"text":"hello"}],"role":"model"},"partial":true,"finish_reason":"","usage_metadata":{"total_token_count":0}}
{"author":"familiar_companion","content":{"parts":[{"text":"world"}],"role":"model"},"partial":false,"turn_complete":true,"finish_reason":"STOP","model_version":"gemini-2.5-flash-lite","usage_metadata":{"total_token_count":42,"candidates_token_count":2}}
`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, ":streamQuery") {
			t.Errorf("path = %s; want suffix :streamQuery", r.URL.Path)
		}
		if got := r.URL.Query().Get("alt"); got != "sse" {
			t.Errorf("alt query = %q; want sse", got)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(fixture))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("familiar_companion"),
		WithEndpointOverride(srv.URL),
	)
	ch, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: fixtureEngine,
		UserID:         "g",
		SessionID:      "s",
		Message:        "hi",
	})
	if err != nil {
		t.Fatalf("StreamQuery: %v", err)
	}

	var events []StreamEvent
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				goto done
			}
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("StreamQuery timed out (%d events)", len(events))
		}
	}
done:
	if len(events) != 2 {
		t.Fatalf("got %d events; want 2", len(events))
	}
	if events[0].Text != "hello" || events[1].Text != "world" {
		t.Errorf("text sequence = %q,%q; want hello,world", events[0].Text, events[1].Text)
	}
	if !events[1].TurnComplete {
		t.Error("last event TurnComplete=false; want true")
	}
}

func TestStreamQuery_FailsLoudOn4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad session"}`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: fixtureEngine,
		UserID:         "g",
		SessionID:      "s",
		Message:        "x",
	})
	if err == nil {
		t.Fatal("expected error on 400")
	}
}

func TestDeleteSession_IsIdempotent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	if err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: fixtureEngine,
		UserID:         "g",
		SessionID:      "s",
	}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
}

func TestCreateSession_EmptyUserIDFailsValidation(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{EngineResource: fixtureEngine})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestStreamQuery_EmptySessionIDFailsValidation(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestStreamQuery_FailsLoudOn5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: fixtureEngine, UserID: "g", SessionID: "s", Message: "x",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestDeleteSession_EmptyEngineFailsValidation(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{UserID: "g", SessionID: "s"})
	if !errors.Is(err, ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
	err = c.DeleteSession(context.Background(), DeleteSessionRequest{EngineResource: fixtureEngine})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestDeleteSession_FailsOn5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: fixtureEngine, UserID: "g", SessionID: "s",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestDeleteSession_FailsOn4xxNon404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: fixtureEngine, UserID: "g", SessionID: "s",
	})
	if err == nil {
		t.Fatal("expected error on 400")
	}
}

func TestStreamQuery_FailsLoudWhenEngineNotConfigured(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{UserID: "g", SessionID: "s"})
	if !errors.Is(err, ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
}

// TestNew_WithTracerOption verifies the WithTracer option path is exercised.
func TestNew_WithTracerOption(t *testing.T) {
	tr := otelTestTracer{}
	c, err := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithTracer(tr),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.tracer != tr {
		t.Error("tracer not propagated through option")
	}
}

// errTokenSource always errors — exercises the token-failure path in do().
type errTokenSource struct{}

func (errTokenSource) Token(_ context.Context) (string, error) {
	return "", errors.New("synthetic token error")
}

func TestDo_PropagatesTokenSourceError(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(errTokenSource{}),
		WithCrewKind("t"),
		WithEndpointOverride("http://example.invalid"),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

// failingDoer makes Do() fail at the transport layer.
type failingDoer struct{}

func (failingDoer) Do(_ *http.Request) (*http.Response, error) {
	return nil, errors.New("synthetic transport failure")
}

func TestDo_PropagatesTransportError(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(failingDoer{}),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride("http://x.invalid"),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

// TestResolveURL_ProdPathBuildsCanonicalEndpoint verifies the no-override
// branch uses endpointFor (canonical Vertex AI URL).
func TestResolveURL_ProdPathBuildsCanonicalEndpoint(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	url, err := c.resolveURL(fixtureEngine, ":query")
	if err != nil {
		t.Fatalf("resolveURL: %v", err)
	}
	wantPrefix := "https://us-central1-aiplatform.googleapis.com/v1beta1/"
	if !strings.HasPrefix(url, wantPrefix) {
		t.Errorf("URL = %q; want prefix %q", url, wantPrefix)
	}
}

func TestResolveURL_BadResourceErrors(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	if _, err := c.resolveURL("bad-resource", ":query"); err == nil {
		t.Error("expected error on bad resource")
	}
}

func TestDeleteSession_TolerantOf404(t *testing.T) {
	// Already-deleted session must NOT error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	if err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: fixtureEngine,
		UserID:         "g",
		SessionID:      "s",
	}); err != nil {
		t.Errorf("DeleteSession 404 returned err = %v; want nil (idempotent)", err)
	}
}

func TestNew_DefaultsCrewKindToUnknown(t *testing.T) {
	c, err := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.crewKind != "unknown" {
		t.Errorf("crewKind = %q; want %q", c.crewKind, "unknown")
	}
}

func TestCreateSession_FailsOn4xxNon5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad state"}`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
}

func TestCreateSession_FailsOnMalformedJSONBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{not-json`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
}

func TestCreateSession_FailsOnEmptySessionIDResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
}

func TestStreamQuery_PostErrorPropagates(t *testing.T) {
	// No endpoint override + malformed resource → resolveURL fails before any
	// HTTP request is made; the returned error must match ErrInvalidRequest.
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: "bad-resource", UserID: "g", SessionID: "s", Message: "x",
	})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestStreamQuery_ContextCancelMidStreamClosesForwarder(t *testing.T) {
	// 32 events exceed the 16-slot out buffer: the forwarding goroutine must
	// block on a full-buffer send, after which cancelling ctx trips the
	// select's ctx.Done arm and closes the channel.
	var sb strings.Builder
	for i := 0; i < 32; i++ {
		sb.WriteString(`{"author":"familiar_companion","content":{"parts":[{"text":"chunk"}],"role":"model"},"partial":true}` + "\n")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sb.String())
	}))
	defer srv.Close()

	c, _ := New(
		WithHTTPDoer(srv.Client()),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride(srv.URL),
	)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.StreamQuery(ctx, StreamQueryRequest{
		EngineResource: fixtureEngine, UserID: "g", SessionID: "s", Message: "x",
	})
	if err != nil {
		t.Fatalf("StreamQuery: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let the forwarder fill its buffer
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream channel did not close after cancel")
		}
	}
}

func TestDeleteSession_PropagatesTokenSourceError(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(errTokenSource{}),
		WithCrewKind("t"),
		WithEndpointOverride("http://example.invalid"),
	)
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: fixtureEngine, UserID: "g", SessionID: "s",
	})
	if err == nil {
		t.Fatal("expected error from token source")
	}
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestPostJSON_ResolveURLErrorBeforeDo(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	if _, err := c.postJSON(context.Background(), "bad-resource", ":query", struct{}{}); err == nil {
		t.Error("expected resolveURL error for bad resource")
	}
}

func TestPostJSONStream_ResolveURLErrorBeforeDo(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	if _, err := c.postJSONStream(context.Background(), "bad-resource", ":streamQuery?alt=sse", struct{}{}); err == nil {
		t.Error("expected resolveURL error for bad resource")
	}
}

func TestDo_MarshalErrorWrapsInvalidRequest(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	_, err := c.do(context.Background(), "http://example.invalid", func() {}, "application/json")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestDo_InvalidURLBuildsError(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	_, err := c.do(context.Background(), "http://bad url", struct{}{}, "application/json")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

// deadlineDoer fails like a context-deadline-exceeded transport, exercising
// the ErrEngineTimeout branch of do().
type deadlineDoer struct{}

func (deadlineDoer) Do(_ *http.Request) (*http.Response, error) {
	return nil, context.DeadlineExceeded
}

func TestDo_PropagatesDeadlineExceededAsTimeout(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(deadlineDoer{}),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
		WithEndpointOverride("http://x.invalid"),
	)
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: fixtureEngine, UserID: "g",
	})
	if !errors.Is(err, ErrEngineTimeout) {
		t.Errorf("err = %v; want ErrEngineTimeout", err)
	}
}

func TestReadExcerpt_NilBodyReturnsEmpty(t *testing.T) {
	c, _ := New(
		WithHTTPDoer(http.DefaultClient),
		WithTokenSource(NewStaticTokenSource("t")),
		WithCrewKind("t"),
	)
	err := c.wrapHTTPErr(&http.Response{StatusCode: http.StatusBadRequest}, ErrSessionCreate)
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
	if got := err.Error(); !strings.Contains(got, "(http 400)") {
		t.Errorf("error = %q; want status excerpt (http 400)", got)
	}
}

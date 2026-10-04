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
)

// GKE web-mode: create_session POSTs to /api/reasoning_engine and the session
// id comes back at output.id (same envelope as Vertex). No bearer token.
func TestGKEClient_CreateSession(t *testing.T) {
	var gotPath, gotAuth, gotClassMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		var env queryEnvelope
		_ = json.NewDecoder(r.Body).Decode(&env)
		gotClassMethod = env.ClassMethod
		_, _ = w.Write([]byte(`{"output":{"id":"sess-abc"}}`))
	}))
	defer srv.Close()

	c, err := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	if err != nil {
		t.Fatalf("NewGKEClient: %v", err)
	}
	sid, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: srv.URL, UserID: "gcid-1", State: map[string]any{"tenant_id": "t1"},
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if sid != "sess-abc" {
		t.Errorf("session id = %q; want sess-abc", sid)
	}
	if gotPath != "/api/reasoning_engine" {
		t.Errorf("path = %q; want /api/reasoning_engine", gotPath)
	}
	if gotClassMethod != "async_create_session" {
		t.Errorf("class_method = %q; want async_create_session", gotClassMethod)
	}
	if gotAuth != "" {
		t.Errorf("Authorization = %q; want empty (cluster-local plaintext, no token)", gotAuth)
	}
}

// StreamQuery POSTs to /api/stream_reasoning_engine, sends the message as a
// {role,parts} object, and parses the bare-JSON-per-line response into events.
func TestGKEClient_StreamQuery(t *testing.T) {
	var gotPath string
	var gotBody queryEnvelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		// GKE ADK server emits ONE JSON object per line, NO data: prefix.
		_, _ = io.WriteString(w, `{"author":"familiar_companion","content":{"parts":[{"text":"Hi"}]},"partial":true}`+"\n")
		_, _ = io.WriteString(w, `{"author":"familiar_companion","content":{"parts":[{"text":"Hi there!"}]},"partial":false,"turn_complete":true,"model_version":"gemini-2.5-flash","usage_metadata":{"candidates_token_count":3}}`+"\n")
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	ch, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: srv.URL, UserID: "gcid-1", SessionID: "sess-abc", Message: "hello",
	})
	if err != nil {
		t.Fatalf("StreamQuery: %v", err)
	}
	var texts []string
	var sawComplete bool
	for ev := range ch {
		if ev.Err != nil {
			t.Fatalf("stream event err: %v", ev.Err)
		}
		if ev.Text != "" {
			texts = append(texts, ev.Text)
		}
		if ev.TurnComplete {
			sawComplete = true
			if ev.Model != "gemini-2.5-flash" {
				t.Errorf("model = %q; want gemini-2.5-flash", ev.Model)
			}
			if ev.UsageMetadata == nil || ev.UsageMetadata.CandidatesTokenCount != 3 {
				t.Errorf("usage = %+v; want candidates=3", ev.UsageMetadata)
			}
		}
	}
	if !sawComplete {
		t.Error("never saw turn_complete event")
	}
	if len(texts) == 0 {
		t.Error("no text events parsed")
	}
	if gotPath != "/api/stream_reasoning_engine" {
		t.Errorf("path = %q; want /api/stream_reasoning_engine", gotPath)
	}
	// message must be an OBJECT {role,parts}, not a bare string (the proven
	// ReasoningEngineExecutor shape).
	msg, ok := gotBody.Input["message"].(map[string]any)
	if !ok {
		t.Fatalf("message = %T; want object {role,parts}", gotBody.Input["message"])
	}
	if msg["role"] != "user" {
		t.Errorf("message.role = %v; want user", msg["role"])
	}
}

func TestNormaliseGKEBase(t *testing.T) {
	cases := map[string]string{
		"gke://chora-familiar.ai-kernel.svc.cluster.local:8080": "http://chora-familiar.ai-kernel.svc.cluster.local:8080",
		"chora-familiar.ai-kernel.svc.cluster.local:8080":       "http://chora-familiar.ai-kernel.svc.cluster.local:8080",
		"http://x:8080/": "http://x:8080",
	}
	for in, want := range cases {
		got, err := normaliseGKEBase(in)
		if err != nil {
			t.Errorf("normaliseGKEBase(%q): %v", in, err)
		}
		if got != want {
			t.Errorf("normaliseGKEBase(%q) = %q; want %q", in, got, want)
		}
	}
	if _, err := normaliseGKEBase(""); err == nil {
		t.Error("want error on empty base")
	}
}

func TestNewGKEClient_RequiresHTTPDoer(t *testing.T) {
	if _, err := NewGKEClient(); err == nil {
		t.Error("want error when HTTPDoer missing")
	}
}

var _ = strings.TrimSpace

func TestNewGKEClient_WithGKETracer(t *testing.T) {
	tr := otelTestTracer{}
	c, err := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient), WithGKECrewKind("familiar"), WithGKETracer(tr))
	if err != nil {
		t.Fatalf("NewGKEClient: %v", err)
	}
	if c.tracer != tr {
		t.Error("tracer not propagated through WithGKETracer")
	}
}

func TestNewGKEClient_DefaultsCrewKindToUnknown(t *testing.T) {
	c, err := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	if err != nil {
		t.Fatalf("NewGKEClient: %v", err)
	}
	if c.crewKind != "unknown" {
		t.Errorf("crewKind = %q; want %q", c.crewKind, "unknown")
	}
}

func TestGKEClient_CreateSession_EmptyResourceIsNotConfigured(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{UserID: "g"})
	if !errors.Is(err, ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
}

func TestGKEClient_CreateSession_EmptyUserIDRejected(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{EngineResource: "http://x:8080"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestGKEClient_CreateSession_TransportError(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(failingDoer{}))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: "http://x:8080", UserID: "g",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestGKEClient_CreateSession_Engine5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"cold"}`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: srv.URL, UserID: "g",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestGKEClient_CreateSession_Engine4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad state"}`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: srv.URL, UserID: "g",
	})
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
}

func TestGKEClient_CreateSession_MalformedBodyErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{not-json`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: srv.URL, UserID: "g",
	})
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
}

func TestGKEClient_CreateSession_EmptySessionIDErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	_, err := c.CreateSession(context.Background(), CreateSessionRequest{
		EngineResource: srv.URL, UserID: "g",
	})
	if !errors.Is(err, ErrSessionCreate) {
		t.Errorf("err = %v; want ErrSessionCreate", err)
	}
}

func TestGKEClient_StreamQuery_EmptyResourceIsNotConfigured(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{UserID: "g", SessionID: "s"})
	if !errors.Is(err, ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
}

func TestGKEClient_StreamQuery_MissingSessionIDRejected(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{EngineResource: "http://x:8080", UserID: "g"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestGKEClient_StreamQuery_TransportError(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(failingDoer{}))
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: "http://x:8080", UserID: "g", SessionID: "s", Message: "x",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestGKEClient_StreamQuery_Engine5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s", Message: "x",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestGKEClient_StreamQuery_Engine4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad"}`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	_, err := c.StreamQuery(context.Background(), StreamQueryRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s", Message: "x",
	})
	if !errors.Is(err, ErrStreamAborted) {
		t.Errorf("err = %v; want ErrStreamAborted", err)
	}
}

func TestGKEClient_StreamQuery_ContextCancelMidStreamClosesForwarder(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < 32; i++ {
		sb.WriteString(`{"author":"familiar_companion","content":{"parts":[{"text":"c"}],"role":"model"},"partial":true}` + "\n")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, sb.String())
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := c.StreamQuery(ctx, StreamQueryRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s", Message: "x",
	})
	if err != nil {
		t.Fatalf("StreamQuery: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // let the forwarder fill its 16-slot buffer
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("GKE stream channel did not close after cancel")
		}
	}
}

func TestGKEClient_DeleteSession_EmptyResourceIsNotConfigured(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{UserID: "g", SessionID: "s"})
	if !errors.Is(err, ErrEngineNotConfigured) {
		t.Errorf("err = %v; want ErrEngineNotConfigured", err)
	}
}

func TestGKEClient_DeleteSession_MissingSessionIDRejected(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{EngineResource: "http://x:8080", UserID: "g"})
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestGKEClient_DeleteSession_SuccessIsIdempotent(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	if err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s",
	}); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}
	if gotPath != "/api/reasoning_engine" {
		t.Errorf("path = %q; want /api/reasoning_engine", gotPath)
	}
}

func TestGKEClient_DeleteSession_TolerantOf404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	if err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s",
	}); err != nil {
		t.Errorf("DeleteSession 404 err = %v; want nil (already gone)", err)
	}
}

func TestGKEClient_DeleteSession_Engine5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestGKEClient_DeleteSession_Engine4xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	c, _ := NewGKEClient(WithGKEHTTPDoer(srv.Client()), WithGKECrewKind("familiar"))
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: srv.URL, UserID: "g", SessionID: "s",
	})
	if err == nil {
		t.Fatal("expected error on 400")
	}
}

func TestGKEClient_DeleteSession_TransportError(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(failingDoer{}))
	err := c.DeleteSession(context.Background(), DeleteSessionRequest{
		EngineResource: "http://x:8080", UserID: "g", SessionID: "s",
	})
	if !errors.Is(err, ErrEngineUnavailable) {
		t.Errorf("err = %v; want ErrEngineUnavailable", err)
	}
}

func TestGKEClient_Post_MarshalError(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	_, err := c.post(context.Background(), "http://x:8080", func() {}, "application/json")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

func TestGKEClient_Post_InvalidURL(t *testing.T) {
	c, _ := NewGKEClient(WithGKEHTTPDoer(http.DefaultClient))
	_, err := c.post(context.Background(), "http://bad url", struct{}{}, "application/json")
	if !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("err = %v; want ErrInvalidRequest", err)
	}
}

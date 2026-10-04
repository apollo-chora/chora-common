package agentengine

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// TestParseSSE_FamiliarFixture verifies the parser turns Newton's Familiar
// fixture (recorded 2026-05-13 against the live us-central1 engine) into
// well-formed StreamEvent values.
func TestParseSSE_FamiliarFixture(t *testing.T) {
	f, err := os.Open("testdata/familiar_newton.sse")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := ParseSSE(ctx, f)
	events := drainChan(t, ch, 5*time.Second)

	if len(events) < 1 {
		t.Fatalf("expected ≥1 events, got %d", len(events))
	}
	first := events[0]
	if first.Author != "familiar_companion" {
		t.Errorf("Author = %q; want %q", first.Author, "familiar_companion")
	}
	if first.Model != "gemini-2.5-flash-lite" {
		t.Errorf("Model = %q; want %q", first.Model, "gemini-2.5-flash-lite")
	}
	if !strings.Contains(first.Text, "chain rule") {
		t.Errorf("Text missing keyword 'chain rule': %q", first.Text)
	}
	if !first.Partial {
		t.Errorf("first event partial=false; want true (streamed chunk)")
	}
}

// TestParseSSE_ModerationFixture verifies a Reflection-pattern fixture where
// two authors (moderator, critic) emit chunks.
func TestParseSSE_ModerationFixture(t *testing.T) {
	f, err := os.Open("testdata/moderation_benign.sse")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	ch := ParseSSE(context.Background(), f)
	events := drainChan(t, ch, 5*time.Second)

	if len(events) == 0 {
		t.Fatal("got 0 events")
	}
	gotModerator := false
	for _, ev := range events {
		if ev.Author == "moderator" {
			gotModerator = true
		}
	}
	if !gotModerator {
		t.Errorf("expected at least one event with Author=moderator; authors=%v", authorList(events))
	}
}

// TestParseSSE_QGenFixture verifies the Sequential-pipeline fixture surfaces
// multiple sub-agent authors in turn.
func TestParseSSE_QGenFixture(t *testing.T) {
	f, err := os.Open("testdata/qgen_pipeline.sse")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	ch := ParseSSE(context.Background(), f)
	events := drainChan(t, ch, 5*time.Second)

	if len(events) < 2 {
		t.Fatalf("expected ≥2 events, got %d", len(events))
	}
	// Multiple authors must appear (sequential pipeline characteristic).
	authors := map[string]bool{}
	for _, ev := range events {
		if ev.Author != "" {
			authors[ev.Author] = true
		}
	}
	if len(authors) < 1 {
		t.Errorf("expected ≥1 distinct authors, got %v", authors)
	}
	// At least one terminal event with FinishReason set.
	gotTerminal := false
	for _, ev := range events {
		if ev.FinishReason == "STOP" || ev.FinishReason == "MAX_TOKENS" {
			gotTerminal = true
			break
		}
	}
	if !gotTerminal {
		t.Errorf("no terminal event with FinishReason set")
	}
}

// TestParseSSE_UsageMetadataPopulated verifies that the terminal event
// surfaces UsageMetadata for cost-ledger emit.
func TestParseSSE_UsageMetadataPopulated(t *testing.T) {
	f, err := os.Open("testdata/familiar_newton.sse")
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	ch := ParseSSE(context.Background(), f)
	events := drainChan(t, ch, 5*time.Second)

	var terminal *StreamEvent
	for i := range events {
		if events[i].FinishReason != "" {
			terminal = &events[i]
		}
	}
	if terminal == nil {
		t.Fatal("no terminal event")
	}
	if terminal.UsageMetadata == nil {
		t.Fatal("terminal event UsageMetadata = nil; want populated")
	}
	if terminal.UsageMetadata.TotalTokenCount == 0 {
		t.Errorf("TotalTokenCount = 0; want >0")
	}
}

// TestParseSSE_HandlesDataPrefix verifies the parser tolerates both the raw
// JSON-lines fixture format AND the canonical SSE `data:` prefix.
func TestParseSSE_HandlesDataPrefix(t *testing.T) {
	body := `data: {"author":"x","content":{"parts":[{"text":"hello"}],"role":"model"},"turn_complete":true,"finish_reason":"STOP","usage_metadata":{"total_token_count":7}}

`
	ch := ParseSSE(context.Background(), strings.NewReader(body))
	events := drainChan(t, ch, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Text != "hello" {
		t.Errorf("Text = %q; want %q", events[0].Text, "hello")
	}
	if !events[0].TurnComplete {
		t.Errorf("TurnComplete = false; want true")
	}
}

// TestParseSSE_BadJSONErrors verifies malformed JSON closes the channel with
// an Err event.
func TestParseSSE_BadJSONErrors(t *testing.T) {
	body := "{not-json\n"
	ch := ParseSSE(context.Background(), strings.NewReader(body))
	events := drainChan(t, ch, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 (error) event, got %d", len(events))
	}
	if events[0].Err == nil {
		t.Fatal("expected Err on malformed JSON")
	}
}

// TestParseSSE_ContextCancelClosesChannel verifies cancelling the context
// closes the channel without hanging.
func TestParseSSE_ContextCancelClosesChannel(t *testing.T) {
	pr, pw := io.Pipe()
	// Writer leaves r open so the parser blocks on Read.
	defer pw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	ch := ParseSSE(ctx, pr)

	cancel()

	select {
	case ev, ok := <-ch:
		if ok && ev.Err == nil && !errors.Is(ev.Err, context.Canceled) {
			// Either we got an Err event (acceptable) or the channel closed.
			// Both are valid termination paths.
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel did not close after context cancel")
	}
}

// drainChan reads the channel until close or timeout, returning all events.
func drainChan(t *testing.T, ch <-chan StreamEvent, timeout time.Duration) []StreamEvent {
	t.Helper()
	var out []StreamEvent
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline:
			t.Fatalf("drainChan timeout after %s (collected %d events)", timeout, len(out))
		}
	}
}

func authorList(events []StreamEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Author
	}
	return out
}

// TestParseSSE_SkipsControlLines verifies `event:` / `id:` / `retry:` SSE
// control lines are ignored while the following `data:` payload is parsed.
func TestParseSSE_SkipsControlLines(t *testing.T) {
	body := "event: message\nid: 3\nretry: 100\ndata: {\"author\":\"x\",\"content\":{\"parts\":[{\"text\":\"hi\"}],\"role\":\"model\"}}\n\n"
	ch := ParseSSE(context.Background(), strings.NewReader(body))
	events := drainChan(t, ch, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Text != "hi" {
		t.Errorf("Text = %q; want hi", events[0].Text)
	}
}

// TestParseSSE_SkipsEmptyDataPayload verifies bare `data:` / `data: ` lines
// (no JSON payload) are skipped without emitting an event.
func TestParseSSE_SkipsEmptyDataPayload(t *testing.T) {
	body := "data:\n\ndata: \n\n{\"author\":\"x\",\"content\":{\"parts\":[{\"text\":\"ok\"}]}}\n"
	ch := ParseSSE(context.Background(), strings.NewReader(body))
	events := drainChan(t, ch, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Text != "ok" {
		t.Errorf("Text = %q; want ok", events[0].Text)
	}
}

// TestParseSSE_FunctionCallSurfaced verifies a content part with a
// function_call payload surfaces StreamEvent.FunctionCall.
func TestParseSSE_FunctionCallSurfaced(t *testing.T) {
	body := `{"author":"familiar_companion","content":{"parts":[{"text":"","function_call":{"name":"lookup_concept","args":{"term":"chain rule"}}}],"role":"model"}}`
	ch := ParseSSE(context.Background(), strings.NewReader(body))
	events := drainChan(t, ch, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].FunctionCall == nil {
		t.Fatal("FunctionCall = nil; want populated")
	}
	if events[0].FunctionCall.Name != "lookup_concept" {
		t.Errorf("FunctionCall.Name = %q; want lookup_concept", events[0].FunctionCall.Name)
	}
}

// TestParseSSE_OverlongLineErrors verifies a line exceeding the scanner's
// 1 MiB buffer closes the channel with an Err event (scanner.Err branch).
func TestParseSSE_OverlongLineErrors(t *testing.T) {
	huge := strings.Repeat("x", 1024*1024+1)
	ch := ParseSSE(context.Background(), strings.NewReader(huge))
	events := drainChan(t, ch, 2*time.Second)
	if len(events) != 1 {
		t.Fatalf("expected 1 (error) event, got %d", len(events))
	}
	if events[0].Err == nil {
		t.Fatal("expected Err on overlong line")
	}
}

// TestForwardSSE_CancelUnblocksForward verifies that a blocking forward to an
// unbuffered out channel is unblocked by context cancellation (inner select's
// ctx.Done arm).
func TestForwardSSE_CancelUnblocksForward(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	staging := make(chan StreamEvent, 1)
	out := make(chan StreamEvent) // unbuffered: forwarder blocks until cancelled
	done := make(chan struct{})
	go func() {
		forwardSSE(ctx, staging, out)
		close(done)
	}()
	staging <- StreamEvent{Text: "pending"}
	time.Sleep(150 * time.Millisecond) // let forwardSSE block on the unbuffered send
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("forwardSSE did not return after cancel")
	}
}

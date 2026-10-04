package agentengine

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// sseEnvelope mirrors the ADK Agent Engine per-event JSON shape. Vertex AI
// Agent Engine `:streamQuery?alt=sse` emits one envelope per event; some
// recorded fixtures are newline-delimited JSON without the canonical
// `data: ` SSE prefix — ParseSSE handles both.
type sseEnvelope struct {
	Author       string `json:"author"`
	Content      struct {
		Parts []struct {
			Text         string                 `json:"text,omitempty"`
			FunctionCall *FunctionCall          `json:"function_call,omitempty"`
		} `json:"parts"`
		Role string `json:"role"`
	} `json:"content"`
	FinishReason  string         `json:"finish_reason"`
	Partial       bool           `json:"partial"`
	TurnComplete  bool           `json:"turn_complete"`
	ModelVersion  string         `json:"model_version"`
	UsageMetadata *UsageMetadata `json:"usage_metadata,omitempty"`
}

// ParseSSE reads Vertex AI Agent Engine SSE output from r and emits one
// StreamEvent per JSON envelope on the returned channel. Accepts both
// canonical SSE (`data: <json>\n\n`) and newline-delimited JSON.
//
// The channel closes when r returns io.EOF, ctx is cancelled, or a fatal
// parse error occurs. Fatal parse errors are delivered as a final
// StreamEvent with Err != nil before close.
//
// ParseSSE runs the read loop in its own goroutine and does not close r.
// Callers retain ownership of r; on context cancel the read goroutine may
// leak until r is closed.
func ParseSSE(ctx context.Context, r io.Reader) <-chan StreamEvent {
	out := make(chan StreamEvent, 16)
	// staging channel between the (blocking) reader goroutine and the
	// (ctx-aware) forwarder goroutine.
	staging := make(chan StreamEvent, 16)

	go readSSE(r, staging)
	go forwardSSE(ctx, staging, out)

	return out
}

func readSSE(r io.Reader, staging chan<- StreamEvent) {
	defer close(staging)
	scanner := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// Ignore `event:` / `id:` / `retry:` SSE control lines.
		if strings.HasPrefix(line, "event:") || strings.HasPrefix(line, "id:") || strings.HasPrefix(line, "retry:") {
			continue
		}
		// Tolerate canonical SSE `data: ` prefix.
		payload := strings.TrimPrefix(line, "data:")
		payload = strings.TrimSpace(payload)
		if payload == "" {
			continue
		}

		var env sseEnvelope
		if err := json.Unmarshal([]byte(payload), &env); err != nil {
			staging <- StreamEvent{Err: fmt.Errorf("%w: %v", ErrStreamAborted, err)}
			return
		}

		ev := StreamEvent{
			Author:        env.Author,
			FinishReason:  env.FinishReason,
			Partial:       env.Partial,
			TurnComplete:  env.TurnComplete,
			Model:         env.ModelVersion,
			UsageMetadata: env.UsageMetadata,
		}
		if len(env.Content.Parts) > 0 {
			ev.Text = env.Content.Parts[0].Text
			if env.Content.Parts[0].FunctionCall != nil {
				ev.FunctionCall = env.Content.Parts[0].FunctionCall
			}
		}
		staging <- ev
	}
	if err := scanner.Err(); err != nil {
		staging <- StreamEvent{Err: fmt.Errorf("%w: %v", ErrStreamAborted, err)}
	}
}

func forwardSSE(ctx context.Context, staging <-chan StreamEvent, out chan<- StreamEvent) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-staging:
			if !ok {
				return
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}

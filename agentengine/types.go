package agentengine

import (
	"context"
	"net/http"
)

// Client invokes a Vertex AI Agent Engine (Reasoning Engine).
//
// Implementations MUST stamp the D6 P4 mandatory span attributes on every
// returned call (see tracing.go). Engine resource_names follow the pattern
// `projects/PROJECT_NUMBER/locations/REGION/reasoningEngines/NNN`.
type Client interface {
	// CreateSession creates a Reasoning Engine session bound to UserID with
	// the given initial state. Returns the session_id.
	CreateSession(ctx context.Context, req CreateSessionRequest) (string, error)

	// StreamQuery sends a user message to an existing session and returns a
	// channel of streamed events. The channel closes when the engine emits
	// turn_complete=true OR the context is cancelled OR an error occurs.
	// Callers MUST drain the channel to release resources.
	StreamQuery(ctx context.Context, req StreamQueryRequest) (<-chan StreamEvent, error)

	// DeleteSession releases the engine-side session. Idempotent — repeated
	// calls return nil even if the session is already gone.
	DeleteSession(ctx context.Context, req DeleteSessionRequest) error
}

// CreateSessionRequest is the payload for the `:query` async_create_session
// control-plane method.
type CreateSessionRequest struct {
	// EngineResource is the canonical resource_name
	// `projects/.../locations/.../reasoningEngines/NNN`. Source from env
	// (`<CREW>_ENGINE_RESOURCE`), never inline.
	EngineResource string

	// UserID is the actor GCID (or AGID for A2A). Required by Vertex AI to
	// scope the session.
	UserID string

	// State is the initial session.State seeded into the engine. Keys must
	// match the per-crew contract (manaplugin: tenant_id + user_gcid +
	// mana_tier mandatory; Familiar adds familiar_id; Recommender adds
	// learner_persona; Moderation adds author_gcid + post_text; QGen adds
	// batch_id + subject_hint + difficulty_hint + desired_atom_type).
	State map[string]any
}

// StreamQueryRequest is the payload for the `:streamQuery?alt=sse` method.
type StreamQueryRequest struct {
	EngineResource string
	UserID         string
	SessionID      string
	Message        string
}

// DeleteSessionRequest releases an engine-side session.
type DeleteSessionRequest struct {
	EngineResource string
	UserID         string
	SessionID      string
}

// StreamEvent is one ADK-shaped event emitted by `:streamQuery`. The engine
// emits multiple events per turn (Sequential pipelines emit one chunk-stream
// per sub-agent); callers aggregate by Author or wait for the terminal
// TurnComplete=true event.
type StreamEvent struct {
	// Author is the sub-agent name. For P1 single-agent crews this is the
	// crew name (e.g. `familiar_companion`); for P2 Sequential pipelines
	// it's the step (`qgen_assurance`, `qgen_fitness`, ...); for P6
	// Reflection it's `moderator` then `critic`.
	Author string

	// Text is the cumulative text emitted by this event's
	// content.parts[0].text. For partial=true events this is one chunk.
	Text string

	// FunctionCall, when non-nil, indicates the model invoked a tool.
	FunctionCall *FunctionCall

	// FinishReason captures the model's exit reason: `STOP`, `MAX_TOKENS`,
	// or empty for non-terminal chunks.
	FinishReason string

	// Partial=true marks a streamed chunk; the same Author will emit
	// further chunks until a partial=false aggregate event lands.
	Partial bool

	// TurnComplete=true marks the end of the turn. Callers ranging over the
	// stream channel can break here.
	TurnComplete bool

	// Model is the resolved model_version per-event (e.g.
	// `gemini-2.5-flash-lite`). Mirrors the tieredmodelplugin choice.
	Model string

	// UsageMetadata is populated on the terminal turn_complete=true event
	// (and sometimes partial chunks) — used for cost ledger + D6 P4 attrs.
	UsageMetadata *UsageMetadata

	// Err is non-nil if the channel is closing due to a transport error or
	// malformed JSON. The channel closes after delivering this event.
	Err error
}

// FunctionCall describes an ADK tool invocation.
type FunctionCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// UsageMetadata mirrors Vertex AI Agent Engine's per-event usage record.
type UsageMetadata struct {
	PromptTokenCount     int    `json:"prompt_token_count"`
	CandidatesTokenCount int    `json:"candidates_token_count"`
	ThoughtsTokenCount   int    `json:"thoughts_token_count"`
	ToolUseTokenCount    int    `json:"tool_use_prompt_token_count"`
	TotalTokenCount      int    `json:"total_token_count"`
	TrafficType          string `json:"traffic_type"`
}

// HTTPDoer is the minimal interface AgentEngineClient depends on. *http.Client
// satisfies it; tests substitute fakes.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// TokenSource produces bearer tokens for the Vertex AI control plane.
type TokenSource interface {
	Token(ctx context.Context) (string, error)
}

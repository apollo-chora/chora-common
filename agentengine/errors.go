package agentengine

import (
	"errors"
	"fmt"
)

// Sentinel errors. Domain handlers convert these into 503 responses with
// stable error codes so chora-web / BFF can surface "engine not configured"
// vs "engine unavailable" vs "session timed out" distinctly.
var (
	// ErrEngineNotConfigured is returned when the calling service has no
	// EngineResource configured (empty env). Domain handlers MUST 503 with
	// code `{crew}_engine_not_configured`.
	ErrEngineNotConfigured = errors.New("agentengine: engine resource not configured")

	// ErrEngineUnavailable is returned on transport-level failures
	// (network, 5xx from Vertex AI control plane).
	ErrEngineUnavailable = errors.New("agentengine: engine unavailable")

	// ErrSessionCreate is returned when `:query` async_create_session fails
	// with a 4xx response (e.g. malformed state, missing keys).
	ErrSessionCreate = errors.New("agentengine: session create failed")

	// ErrStreamAborted is returned when `:streamQuery?alt=sse` aborts mid-turn
	// (network drop, context cancelled, malformed SSE event).
	ErrStreamAborted = errors.New("agentengine: stream aborted")

	// ErrEngineTimeout is returned when the engine exceeds the per-call deadline.
	ErrEngineTimeout = errors.New("agentengine: engine timeout")

	// ErrInvalidRequest is returned when the caller-supplied request is
	// missing mandatory fields (EngineResource, UserID).
	ErrInvalidRequest = errors.New("agentengine: invalid request")
)

// EngineError wraps a sentinel error with an HTTP status code + body excerpt
// for structured logging.
type EngineError struct {
	Sentinel    error
	HTTPStatus  int
	BodyExcerpt string
}

func (e *EngineError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.BodyExcerpt == "" {
		return fmt.Sprintf("%s (http %d)", e.Sentinel.Error(), e.HTTPStatus)
	}
	return fmt.Sprintf("%s (http %d): %s", e.Sentinel.Error(), e.HTTPStatus, e.BodyExcerpt)
}

func (e *EngineError) Unwrap() error { return e.Sentinel }

// IsNotConfigured reports whether err (or any error in its chain) is
// ErrEngineNotConfigured. Domain handlers use this for 503 mapping.
func IsNotConfigured(err error) bool {
	return errors.Is(err, ErrEngineNotConfigured)
}

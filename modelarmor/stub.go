// In-process Screener stub for downstream-package unit tests. NO
// network. Mirrors the pattern used by `libs/chora-go-common/secrets`
// (`StubClient`) — keeps every test in the repo deterministic.

package modelarmor

import (
	"context"
	"errors"
	"sync"
)

// StubScreener is the in-process test double. It returns canned
// ScreenResults based on per-request hook functions so each test can
// drive the verdict + filter shape it needs.
//
// Usage in a downstream test:
//
//	stub := modelarmor.NewStubScreener()
//	stub.SetUserPromptResult(func(req modelarmor.ScreenRequest) (modelarmor.ScreenResult, error) {
//	    if strings.Contains(req.Text, "secret") {
//	        return modelarmor.ScreenResult{
//	            Verdict: modelarmor.VerdictBlock,
//	            Reason:  "stub: matched 'secret' rule",
//	            Filters: []modelarmor.FilterHit{{
//	                FilterName: modelarmor.FilterNameRAI,
//	                MatchState: modelarmor.MatchStateMatchFound,
//	                Severity:   modelarmor.SeverityHigh,
//	                Subcategory: "HARASSMENT",
//	            }},
//	            LatencyMs: 5,
//	        }, nil
//	    }
//	    return modelarmor.ScreenResult{Verdict: modelarmor.VerdictAllow, LatencyMs: 1}, nil
//	})
//
// The stub is concurrency-safe (each hook is read under the mutex).
type StubScreener struct {
	mu sync.RWMutex

	userPromptFn  func(ScreenRequest) (ScreenResult, error)
	modelRespFn   func(ScreenRequest) (ScreenResult, error)
	closeFn       func() error
	calls         []StubCall
	captureCalls  bool
}

// StubCall records a single invocation when CaptureCalls is enabled.
type StubCall struct {
	Method  string // "SanitizeUserPrompt" | "SanitizeModelResponse"
	Request ScreenRequest
}

// NewStubScreener returns a stub that defaults to VerdictAllow on both
// methods. Tests override via SetUserPromptResult / SetModelResponseResult.
func NewStubScreener() *StubScreener {
	return &StubScreener{}
}

// Compile-time check that *StubScreener implements Screener.
var _ Screener = (*StubScreener)(nil)

// SetUserPromptResult installs the hook for SanitizeUserPrompt. Pass nil
// to revert to the default (Allow). Concurrency-safe.
func (s *StubScreener) SetUserPromptResult(fn func(ScreenRequest) (ScreenResult, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userPromptFn = fn
}

// SetModelResponseResult installs the hook for SanitizeModelResponse.
// Pass nil to revert to the default (Allow). Concurrency-safe.
func (s *StubScreener) SetModelResponseResult(fn func(ScreenRequest) (ScreenResult, error)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.modelRespFn = fn
}

// SetCloseHook installs a custom Close behaviour. Defaults to no-op.
func (s *StubScreener) SetCloseHook(fn func() error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeFn = fn
}

// EnableCallCapture starts recording every call into Calls() — useful
// for tests that assert "Screener was called once with these args".
func (s *StubScreener) EnableCallCapture() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captureCalls = true
	s.calls = nil
}

// Calls returns a snapshot of captured calls (only populated after
// EnableCallCapture).
func (s *StubScreener) Calls() []StubCall {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]StubCall, len(s.calls))
	copy(out, s.calls)
	return out
}

// SanitizeUserPrompt dispatches to the configured hook, or returns the
// default Allow result.
func (s *StubScreener) SanitizeUserPrompt(_ context.Context, req ScreenRequest) (ScreenResult, error) {
	if err := validateRequest(req); err != nil {
		return ScreenResult{}, err
	}
	s.mu.RLock()
	fn := s.userPromptFn
	if s.captureCalls {
		s.mu.RUnlock()
		s.mu.Lock()
		s.calls = append(s.calls, StubCall{Method: "SanitizeUserPrompt", Request: req})
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}
	if fn == nil {
		return defaultAllowResult(), nil
	}
	return fn(req)
}

// SanitizeModelResponse dispatches to the configured hook, or returns
// the default Allow result.
func (s *StubScreener) SanitizeModelResponse(_ context.Context, req ScreenRequest) (ScreenResult, error) {
	if err := validateRequest(req); err != nil {
		return ScreenResult{}, err
	}
	s.mu.RLock()
	fn := s.modelRespFn
	if s.captureCalls {
		s.mu.RUnlock()
		s.mu.Lock()
		s.calls = append(s.calls, StubCall{Method: "SanitizeModelResponse", Request: req})
		s.mu.Unlock()
	} else {
		s.mu.RUnlock()
	}
	if fn == nil {
		return defaultAllowResult(), nil
	}
	return fn(req)
}

// Close calls the configured Close hook (default no-op).
func (s *StubScreener) Close() error {
	s.mu.RLock()
	fn := s.closeFn
	s.mu.RUnlock()
	if fn == nil {
		return nil
	}
	return fn()
}

// defaultAllowResult is what the stub returns when no hook is set.
func defaultAllowResult() ScreenResult {
	return ScreenResult{
		Verdict:     VerdictAllow,
		Reason:      "",
		Filters:     []FilterHit{},
		LatencyMs:   0,
		RawResponse: map[string]any{"stub": true},
	}
}

// ErrStubExplicit is a convenience for tests that want to assert the
// caller surfaces a Screener error. Wrap it in your hook:
//
//	stub.SetUserPromptResult(func(_ modelarmor.ScreenRequest) (modelarmor.ScreenResult, error) {
//	    return modelarmor.ScreenResult{}, modelarmor.ErrStubExplicit
//	})
var ErrStubExplicit = errors.New("modelarmor: stub error")

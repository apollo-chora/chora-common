// Unit tests for the modelarmor port. NO network — every test
// exercises either the StubScreener, the LocalScreener substitute, or
// the pure validation/construction surface.
package modelarmor

import (
	"context"
	"errors"
	"strings"
	"testing"
)

const validTemplate = "projects/chora-test/locations/us-central1/templates/chora-guardrail-strict-dev"

func validReq(text string) ScreenRequest {
	return ScreenRequest{
		TenantID:     "tenant-uuid-v7-aaa",
		AgentID:      "qgen-generator",
		GCID:         "gcid-uuid-v7-bbb",
		TemplateName: validTemplate,
		Text:         text,
	}
}

// ---------------------------------------------------------------------
// validateRequest
// ---------------------------------------------------------------------

func TestValidateRequest_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		req  ScreenRequest
	}{
		{"missing tenant", ScreenRequest{AgentID: "a", TemplateName: "t", Text: "x"}},
		{"missing agent", ScreenRequest{TenantID: "t", TemplateName: "t", Text: "x"}},
		{"missing template", ScreenRequest{TenantID: "t", AgentID: "a", Text: "x"}},
		{"missing text", ScreenRequest{TenantID: "t", AgentID: "a", TemplateName: "t"}},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequest(tc.req)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, ErrInvalidScreenRequest) {
				t.Errorf("expected ErrInvalidScreenRequest, got %v", err)
			}
		})
	}
}

func TestValidateRequest_AcceptsValidRequest(t *testing.T) {
	req := validReq("hello world")
	if err := validateRequest(req); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidateRequest_AcceptsEmptyGCID(t *testing.T) {
	// GCID is the only optional field — system-initiated calls may omit it.
	req := validReq("hello")
	req.GCID = ""
	if err := validateRequest(req); err != nil {
		t.Errorf("expected GCID empty to be valid, got %v", err)
	}
}

// ---------------------------------------------------------------------
// NewScreener — compatibility constructor
// ---------------------------------------------------------------------

func TestNewScreener_RejectsEmptyProject(t *testing.T) {
	_, err := NewScreener(context.Background(), "", "us-central1")
	if err == nil {
		t.Fatal("expected error for empty project")
	}
	if !strings.Contains(err.Error(), "project required") {
		t.Errorf("expected project-required error, got %v", err)
	}
}

func TestNewScreener_RejectsEmptyLocation(t *testing.T) {
	_, err := NewScreener(context.Background(), "chora-test", "")
	if err == nil {
		t.Fatal("expected error for empty location")
	}
	if !strings.Contains(err.Error(), "location required") {
		t.Errorf("expected location-required error, got %v", err)
	}
}

// TestNewScreener_ReturnsLocalSubstitute pins the post-GCP-exit
// construction contract: the compatibility constructor returns the
// local substitute, which satisfies Screener.
func TestNewScreener_ReturnsLocalSubstitute(t *testing.T) {
	s, err := NewScreener(context.Background(), "chora-test", "us-central1")
	if err != nil {
		t.Fatalf("NewScreener: %v", err)
	}
	if _, ok := s.(*LocalScreener); !ok {
		t.Fatalf("NewScreener should return *LocalScreener, got %T", s)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// ---------------------------------------------------------------------
// StubScreener — surface coverage
// ---------------------------------------------------------------------

func TestStubScreener_DefaultsToAllow(t *testing.T) {
	s := NewStubScreener()
	r, err := s.SanitizeUserPrompt(context.Background(), validReq("hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Verdict != VerdictAllow {
		t.Errorf("default Verdict=%s, want Allow", r.Verdict)
	}
	r2, err := s.SanitizeModelResponse(context.Background(), validReq("hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r2.Verdict != VerdictAllow {
		t.Errorf("default model-resp Verdict=%s, want Allow", r2.Verdict)
	}
}

func TestStubScreener_HookOverride(t *testing.T) {
	s := NewStubScreener()
	s.SetUserPromptResult(func(req ScreenRequest) (ScreenResult, error) {
		if strings.Contains(req.Text, "block-me") {
			return ScreenResult{Verdict: VerdictBlock, Reason: "stub: matched"}, nil
		}
		return ScreenResult{Verdict: VerdictAllow}, nil
	})
	r, err := s.SanitizeUserPrompt(context.Background(), validReq("please block-me"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Verdict != VerdictBlock {
		t.Errorf("expected Block via hook, got %s", r.Verdict)
	}
}

func TestStubScreener_ModelResponseHookOverride(t *testing.T) {
	s := NewStubScreener()
	s.SetModelResponseResult(func(req ScreenRequest) (ScreenResult, error) {
		return ScreenResult{
			Verdict: VerdictInspectOnly,
			Reason:  "stub: audit-only",
		}, nil
	})
	r, err := s.SanitizeModelResponse(context.Background(), validReq("llm output"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Verdict != VerdictInspectOnly {
		t.Errorf("expected InspectOnly via hook, got %s", r.Verdict)
	}
}

func TestStubScreener_ErrStubExplicit(t *testing.T) {
	s := NewStubScreener()
	s.SetUserPromptResult(func(_ ScreenRequest) (ScreenResult, error) {
		return ScreenResult{}, ErrStubExplicit
	})
	_, err := s.SanitizeUserPrompt(context.Background(), validReq("anything"))
	if !errors.Is(err, ErrStubExplicit) {
		t.Errorf("expected ErrStubExplicit, got %v", err)
	}
}

func TestStubScreener_CaptureCalls(t *testing.T) {
	s := NewStubScreener()
	s.EnableCallCapture()
	_, _ = s.SanitizeUserPrompt(context.Background(), validReq("one"))
	_, _ = s.SanitizeModelResponse(context.Background(), validReq("two"))
	calls := s.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected 2 captured calls, got %d", len(calls))
	}
	if calls[0].Method != "SanitizeUserPrompt" || calls[0].Request.Text != "one" {
		t.Errorf("call[0] wrong: %+v", calls[0])
	}
	if calls[1].Method != "SanitizeModelResponse" || calls[1].Request.Text != "two" {
		t.Errorf("call[1] wrong: %+v", calls[1])
	}
}

func TestStubScreener_RejectsInvalidRequest(t *testing.T) {
	s := NewStubScreener()
	_, err := s.SanitizeUserPrompt(context.Background(), ScreenRequest{})
	if err == nil || !errors.Is(err, ErrInvalidScreenRequest) {
		t.Errorf("expected ErrInvalidScreenRequest, got %v", err)
	}
}

func TestStubScreener_CloseHook(t *testing.T) {
	s := NewStubScreener()
	called := false
	s.SetCloseHook(func() error { called = true; return nil })
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("Close hook not invoked")
	}
}

package agentengine

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// cloudPlatformScope is the OAuth2 scope required to invoke Vertex AI Agent
// Engine REST endpoints.
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// ADCTokenSource produces bearer tokens via Application Default Credentials.
// In production (Cloud Run / GKE) ADC resolves via Workload Identity
// Federation against the runtime service account. In local dev (Dale's
// machine) ADC resolves via `gcloud auth application-default login` or the
// SA key in GOOGLE_APPLICATION_CREDENTIALS.
//
// Never inline credentials — ADC is the only sanctioned path
// (feedback_no_inline_config + secrets-and-env skill).
type ADCTokenSource struct {
	src oauth2.TokenSource
}

// NewADCTokenSource creates a token source backed by Google's Application
// Default Credentials. The underlying TokenSource is constructed lazily on
// first Token call; if ADC resolution fails the error surfaces to the
// caller (typically a 503 in the domain handler).
func NewADCTokenSource(ctx context.Context) (*ADCTokenSource, error) {
	src, err := google.DefaultTokenSource(ctx, cloudPlatformScope)
	if err != nil {
		return nil, fmt.Errorf("agentengine: ADC token source: %w", err)
	}
	return &ADCTokenSource{src: src}, nil
}

// Token returns a fresh access token, refreshing transparently.
func (a *ADCTokenSource) Token(ctx context.Context) (string, error) {
	if a == nil || a.src == nil {
		return "", errors.New("agentengine: ADCTokenSource is nil")
	}
	tok, err := a.src.Token()
	if err != nil {
		return "", fmt.Errorf("agentengine: ADC Token(): %w", err)
	}
	if tok == nil || tok.AccessToken == "" {
		return "", errors.New("agentengine: ADC returned empty access token")
	}
	return tok.AccessToken, nil
}

// StaticTokenSource returns a fixed token regardless of context. Useful for
// tests + scripted integration. NEVER use in production — rotate via ADC.
type StaticTokenSource struct {
	token string
}

// NewStaticTokenSource builds a fixed-token source. Pass non-empty token.
func NewStaticTokenSource(token string) *StaticTokenSource {
	return &StaticTokenSource{token: token}
}

// Token returns the configured token.
func (s *StaticTokenSource) Token(_ context.Context) (string, error) {
	if s == nil {
		return "", errors.New("agentengine: StaticTokenSource is nil")
	}
	if s.token == "" {
		return "", errors.New("agentengine: StaticTokenSource has empty token")
	}
	return s.token, nil
}

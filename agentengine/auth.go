package agentengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/jwt"
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
//
// 2026-10-05: HISTORY — the resolver no longer imports
// golang.org/x/oauth2/google (which dragged the cloud.google.com/go
// compute/metadata SDK into every importer). It mirrors the ADC
// precedence locally: GOOGLE_APPLICATION_CREDENTIALS → the gcloud
// well-known file → the GCE metadata server, using only pure
// net/http + oauth2/jwt. The external_account (Workload Identity
// Federation) JSON type is NOT supported by the local resolver —
// callers on GKE use the token-less GKE web-mode client
// (NewGKEClient) instead.
type ADCTokenSource struct {
	src oauth2.TokenSource
}

// NewADCTokenSource creates a token source backed by Application Default
// Credentials. The underlying TokenSource is constructed lazily on
// first Token call; if ADC resolution fails the error surfaces to the
// caller (typically a 503 in the domain handler).
func NewADCTokenSource(ctx context.Context) (*ADCTokenSource, error) {
	src, err := defaultTokenSource(ctx, cloudPlatformScope)
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

// StaticTokenSource returns a fixed token regardless of context. Useful
// for tests + scripted integration. NEVER use in production — rotate via ADC.
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

// ---------------------------------------------------------------------------
// Local ADC resolver (no Google Cloud SDK)
// ---------------------------------------------------------------------------

// defaultTokenSource mirrors golang.org/x/oauth2/google.FindDefaultCredentials
// for the credential types the platform actually issues, without the
// cloud.google.com/go/compute/metadata dependency (HISTORY: that SDK was
// removed platform-wide 2026-10-05):
//
//  1. GOOGLE_APPLICATION_CREDENTIALS JSON file.
//  2. The gcloud well-known file
//     ($HOME/.config/gcloud/application_default_credentials.json).
//  3. The GCE metadata server (pure net/http probe — no metadata SDK).
//
// Supported JSON types: service_account (JWT signing) + authorized_user
// (refresh token). external_account (Workload Identity Federation) is
// rejected with a migration pointer — the GKE web-mode client needs no
// token at all.
func defaultTokenSource(ctx context.Context, scope string) (oauth2.TokenSource, error) {
	if filename := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); filename != "" {
		src, err := tokenSourceFromFile(ctx, filename, scope)
		if err != nil {
			return nil, fmt.Errorf("agentengine: ADC: GOOGLE_APPLICATION_CREDENTIALS: %w", err)
		}
		return src, nil
	}

	if filename := wellKnownADCFile(); filename != "" {
		if src, err := tokenSourceFromFile(ctx, filename, scope); err == nil {
			return src, nil
		}
	}

	if onGCE(ctx) {
		return metadataTokenSource{}, nil
	}

	return nil, errors.New("agentengine: ADC: no credentials found — set GOOGLE_APPLICATION_CREDENTIALS to a service-account / authorized_user JSON key, or run where the GCE metadata server is reachable")
}

// wellKnownADCFile returns the gcloud well-known ADC path, or "" when
// the user's home directory cannot be determined.
func wellKnownADCFile() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".config", "gcloud", "application_default_credentials.json")
}

// adcCredentials is the unmarshalled shape of the two supported ADC JSON
// key types.
type adcCredentials struct {
	Type string `json:"type"`

	// service_account
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	TokenURL    string `json:"token_uri"`

	// authorized_user
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
}

// tokenSourceFromFile reads + dispatches an ADC JSON key file by type.
func tokenSourceFromFile(ctx context.Context, filename, scope string) (oauth2.TokenSource, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var cred adcCredentials
	if err := json.Unmarshal(b, &cred); err != nil {
		return nil, fmt.Errorf("parse %s: %w", filename, err)
	}
	switch cred.Type {
	case "service_account":
		if cred.ClientEmail == "" || cred.PrivateKey == "" {
			return nil, fmt.Errorf("service_account key in %s missing client_email / private_key", filename)
		}
		tokenURL := cred.TokenURL
		if tokenURL == "" {
			tokenURL = "https://oauth2.googleapis.com/token"
		}
		return (&jwt.Config{
			Email:      cred.ClientEmail,
			PrivateKey: []byte(cred.PrivateKey),
			Scopes:     []string{scope},
			TokenURL:   tokenURL,
		}).TokenSource(ctx), nil
	case "authorized_user":
		if cred.ClientID == "" || cred.RefreshToken == "" {
			return nil, fmt.Errorf("authorized_user key in %s missing client_id / refresh_token", filename)
		}
		cfg := &oauth2.Config{
			ClientID:     cred.ClientID,
			ClientSecret: cred.ClientSecret,
			Endpoint:     oauth2.Endpoint{TokenURL: "https://oauth2.googleapis.com/token"},
		}
		return cfg.TokenSource(ctx, &oauth2.Token{RefreshToken: cred.RefreshToken}), nil
	case "external_account", "external_account_authorized_user", "impersonated_service_account", "gdch_service_account":
		return nil, fmt.Errorf("agentengine: ADC: %s credentials are not supported by the local resolver — use the token-less GKE web-mode client (NewGKEClient) or a service-account key", cred.Type)
	default:
		return nil, fmt.Errorf("agentengine: ADC: unsupported credentials type %q in %s", cred.Type, filename)
	}
}

// metadataHost is the GCE metadata server host. Overridable via
// GCE_METADATA_HOST for tests (mirrors the metadata SDK's env knob).
const metadataHost = "metadata.google.internal"

// onGCE probes the GCE metadata server with a bounded request. Returns
// true only when the server answers with the Google flavor header.
func onGCE(ctx context.Context) bool {
	host := os.Getenv("GCE_METADATA_HOST")
	if host == "" {
		host = metadataHost
	}
	probeCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, "http://"+host+"/", nil)
	if err != nil {
		return false
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// metadataTokenSource fetches access tokens from the GCE metadata
// server's default service-account token endpoint.
type metadataTokenSource struct{}

// metadataTokenResponse is the metadata server's token JSON shape.
type metadataTokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

func (metadataTokenSource) Token() (*oauth2.Token, error) {
	host := os.Getenv("GCE_METADATA_HOST")
	if host == "" {
		host = metadataHost
	}
	req, err := http.NewRequest(http.MethodGet,
		"http://"+host+"/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("agentengine: metadata token: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agentengine: metadata token: HTTP %d", resp.StatusCode)
	}
	var out metadataTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("agentengine: metadata token: decode: %w", err)
	}
	if out.AccessToken == "" {
		return nil, errors.New("agentengine: metadata token: empty access_token")
	}
	tok := &oauth2.Token{AccessToken: out.AccessToken, TokenType: out.TokenType}
	if out.ExpiresIn > 0 {
		tok.Expiry = time.Now().Add(time.Duration(out.ExpiresIn) * time.Second)
	}
	return tok, nil
}

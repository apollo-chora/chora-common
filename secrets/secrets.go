// Package secrets is the shared GCP Secret Manager helper for every
// Chora Go service.
//
// Usage:
//
//	cl, err := secrets.NewClient(ctx, "chora-489812")
//	if err != nil { ... }
//	defer cl.Close()
//
//	dsn, err := cl.GetSecret(ctx, "chora-dev-cloudsql-chora_identity-app_rw-dsn")
//
// Per CLAUDE.md §6 (no inline config) + .claude/skills/secrets-and-env:
// services NEVER bake URLs/secrets into source. They source DSNs, IdP
// secrets, and OIDC client config from Secret Manager via this client.
//
// Authentication uses Application Default Credentials (Workload Identity
// Federation in production; gcloud SA key in dev). No SA keys travel
// cross-project.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrSecretNotFound is returned by GetSecret when Secret Manager
// reports NotFound. Wrapped + errors.Is-detectable so callers can
// fall back to env-var defaults during local dev.
var ErrSecretNotFound = errors.New("secrets: not found")

// Client is the production resolver. Wrap a single instance for the
// service lifetime; the underlying secretmanager.Client maintains a
// gRPC connection.
type Client struct {
	project string
	cli     *secretmanager.Client
}

// NewClient constructs a Client. Caller MUST defer Close().
func NewClient(ctx context.Context, project string) (*Client, error) {
	if project == "" {
		return nil, errors.New("secrets: project required")
	}
	cli, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("secrets: new client: %w", err)
	}
	return &Client{project: project, cli: cli}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c.cli == nil {
		return nil
	}
	return c.cli.Close()
}

// GetSecret resolves the named secret to its latest-version value.
// `name` may be either a short ID (the secret's name within the
// project) or a fully-qualified resource path.
func (c *Client) GetSecret(ctx context.Context, name string) (string, error) {
	resource, err := ResourceName(c.project, name)
	if err != nil {
		return "", err
	}
	resp, err := c.cli.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: resource,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return "", fmt.Errorf("%w: %s", ErrSecretNotFound, name)
		}
		return "", fmt.Errorf("secrets: access %q: %w", name, err)
	}
	if resp == nil || resp.Payload == nil {
		return "", fmt.Errorf("secrets: %q empty payload", name)
	}
	return string(resp.Payload.Data), nil
}

// ResourceName returns a fully-qualified Secret Manager resource path,
// pass-through when the input already looks fully-qualified.
//
// Validation:
//   - rejects empty input
//   - rejects path-traversal patterns ("..", "/")
func ResourceName(project, name string) (string, error) {
	if project == "" {
		return "", errors.New("secrets: project required")
	}
	if name == "" {
		return "", errors.New("secrets: name required")
	}
	if strings.HasPrefix(name, "projects/") {
		return name, nil
	}
	if strings.Contains(name, "..") {
		return "", fmt.Errorf("secrets: invalid name %q", name)
	}
	if strings.HasPrefix(name, "/") || strings.Contains(name, "/") {
		// Reject embedded slashes — the "version" portion is appended below.
		return "", fmt.Errorf("secrets: invalid name %q", name)
	}
	return fmt.Sprintf("projects/%s/secrets/%s/versions/latest", project, name), nil
}

// StubClient is the test-only resolver. Satisfies the same surface as
// *Client + db.SecretFetcher.
type StubClient struct {
	values map[string]string
}

// NewStubClient seeds a stub with the supplied secret values.
func NewStubClient(values map[string]string) *StubClient {
	if values == nil {
		values = map[string]string{}
	}
	return &StubClient{values: values}
}

// GetSecret returns a stubbed value or ErrSecretNotFound.
func (s *StubClient) GetSecret(_ context.Context, name string) (string, error) {
	v, ok := s.values[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrSecretNotFound, name)
	}
	return v, nil
}

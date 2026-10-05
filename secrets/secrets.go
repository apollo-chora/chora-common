// Package secrets is the shared environment-backed secret resolver for
// every Chora Go service.
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
// secrets, and OIDC client config from the environment via this client.
//
// Resolution: GetSecret maps the secret name to an environment variable.
// The first match wins, in order:
//
//  1. SECRET_<NAME>      (e.g. secret "db-dsn" → SECRET_DB_DSN)
//  2. <NAME>             (the raw name, uppercased + normalised)
//  3. <raw name>         (exactly as supplied)
//
// Normalisation uppercases the name and replaces every non-alphanumeric
// character with '_' so names like "chora-dev-cloudsql-chora_identity-
// app_rw-dsn" resolve to SECRET_CHORA_DEV_CLOUDSQL_CHORA_IDENTITY_APP_RW_DSN.
// A missing OR empty variable yields an explicit ErrSecretNotFound — the
// resolver NEVER returns an empty string for an unresolved secret.
//
// The managed cloud secret-manager implementation was removed (2026-10-05): the
// platform is broker-neutral and the cloud SDK tainted every importing
// service with Google Cloud dependencies. The exported API shape is
// unchanged so callers (db.Bootstrap via SecretFetcher, service mains)
// compile and behave identically.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrSecretNotFound is returned by GetSecret when no environment
// variable resolves the requested secret (or the resolved value is
// empty). Wrapped + errors.Is-detectable so callers can fall back to
// env-var defaults during local dev.
var ErrSecretNotFound = errors.New("secrets: not found")

// Client is the resolver. Wrap a single instance for the service
// lifetime; resolution reads the process environment on every call so
// secret rotation only needs a pod restart (or an env reload sidecar).
type Client struct {
	project string
}

// NewClient constructs a Client. The project argument is retained for
// API compatibility (it is used by ResourceName for audit/logging
// paths); resolution itself is environment-backed and needs no cloud
// credentials. Caller MUST defer Close().
func NewClient(_ context.Context, project string) (*Client, error) {
	if project == "" {
		return nil, errors.New("secrets: project required")
	}
	return &Client{project: project}, nil
}

// Close releases the client. No-op: the env-backed resolver holds no
// connection. Retained so callers keep the defer-Close lifecycle.
func (c *Client) Close() error {
	return nil
}

// GetSecret resolves the named secret to its environment value. `name`
// is the same short ID the Secret Manager path used; it is mapped to an
// environment variable per the package-doc resolution order. Returns an
// ErrSecretNotFound-wrapped error when nothing resolves — never an
// empty string.
func (c *Client) GetSecret(ctx context.Context, name string) (string, error) {
	if _, err := ResourceName(c.project, name); err != nil {
		return "", err
	}
	for _, candidate := range envCandidates(name) {
		if v := strings.TrimSpace(os.Getenv(candidate)); v != "" {
			return v, nil
		}
	}
	return "", fmt.Errorf("%w: %s", ErrSecretNotFound, name)
}

// envCandidates returns the environment-variable names GetSecret tries,
// in precedence order: SECRET_<normalised>, <normalised>, then the raw
// name. The normalised form upper-cases and replaces non-alphanumerics
// with '_' (e.g. "db-rw-dsn" → "DB_RW_DSN").
func envCandidates(name string) []string {
	normalised := normaliseEnvName(name)
	return []string{
		"SECRET_" + normalised,
		normalised,
		name,
	}
}

// normaliseEnvName upper-cases s and replaces every character outside
// [A-Z0-9] with '_'. Empty input yields "".
func normaliseEnvName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range strings.ToUpper(s) {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// ResourceName validates name and returns the canonical
// projects/{project}/secrets/{name}/versions/latest resource path.
//
// The env-backed Client no longer uses this path for resolution, but the
// helper is retained for exported-API stability: it is still the single
// name-validation gate for GetSecret, and the canonical path remains
// useful in audit logs + error messages.
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

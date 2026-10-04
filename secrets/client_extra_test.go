// Package secrets — supplementary client-surface unit tests.
//
// The production *Client wraps the official Secret Manager SDK, which
// needs Application Default Credentials to construct. These tests cover
// every branch that does NOT require a live GCP session: input
// validation, early-error paths, nil-client handling, and the
// credential-lookup failure of NewClient. Live AccessSecretVersion calls
// are exercised by service-level integration tests.
package secrets

import (
	"context"
	"strings"
	"testing"
)

func TestNewClient_ProjectRequired(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(context.Background(), ""); err == nil {
		t.Fatal("expected error on empty project")
	}
}

// TestNewClient_CredentialLookup — NewClient either fails fast because
// no Application Default Credentials are present (error path) or -- on a
// machine with valid ADC -- constructs a working client that must Close
// cleanly. Asserting on both outcomes keeps the test green in CI and on
// developer laptops alike.
func TestNewClient_CredentialLookup(t *testing.T) {
	t.Parallel()
	cl, err := NewClient(context.Background(), "chora-unit-test")
	if err != nil {
		if !strings.Contains(err.Error(), "secrets: new client") {
			t.Fatalf("unexpected NewClient error: %v", err)
		}
		return
	}
	defer func() {
		if cerr := cl.Close(); cerr != nil {
			t.Errorf("Close: %v", cerr)
		}
	}()
	if cl.project != "chora-unit-test" {
		t.Errorf("project = %q, want chora-unit-test", cl.project)
	}
}

func TestClient_CloseNilClientIsNoOp(t *testing.T) {
	t.Parallel()
	c := &Client{}
	if err := c.Close(); err != nil {
		t.Fatalf("Close on empty client: %v", err)
	}
}

// TestClient_GetSecret_InvalidNameFailsBeforeRPC — the resource-name
// validation rejects bad names without touching the (nil) SDK handle,
// proving the guard runs before any network call.
func TestClient_GetSecret_InvalidNameFailsBeforeRPC(t *testing.T) {
	t.Parallel()
	c := &Client{project: "chora-unit-test"}
	if _, err := c.GetSecret(context.Background(), "bad/name"); err == nil {
		t.Fatal("expected error on name containing a slash")
	}
	if _, err := c.GetSecret(context.Background(), ""); err == nil {
		t.Fatal("expected error on empty name")
	}
	if _, err := c.GetSecret(context.Background(), "../etc"); err == nil {
		t.Fatal("expected error on traversal name")
	}
}

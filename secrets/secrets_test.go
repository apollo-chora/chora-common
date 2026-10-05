// Package secrets unit tests — RED phase first.
//
// The lazy client wraps the official Secret Manager SDK. We only test
// the input-validation surface here; live calls require a real cloud
// project and are exercised by service-level integration tests.
package secrets

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestResourceName_FromShortID(t *testing.T) {
	t.Parallel()
	got, err := ResourceName("chora-489812", "chora-dev-cloudsql-chora_identity-app_rw-dsn")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	want := "projects/chora-489812/secrets/chora-dev-cloudsql-chora_identity-app_rw-dsn/versions/latest"
	if got != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
}

func TestResourceName_FullPathPassThrough(t *testing.T) {
	t.Parallel()
	in := "projects/chora-489812/secrets/foo/versions/3"
	got, err := ResourceName("chora-489812", in)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != in {
		t.Fatalf("expected pass-through, got %q", got)
	}
}

func TestResourceName_RejectsEmpty(t *testing.T) {
	t.Parallel()
	if _, err := ResourceName("chora-489812", ""); err == nil {
		t.Fatalf("expected error on empty name")
	}
	if _, err := ResourceName("", "secret"); err == nil {
		t.Fatalf("expected error on empty project")
	}
}

func TestResourceName_RejectsTraversal(t *testing.T) {
	t.Parallel()
	if _, err := ResourceName("chora-489812", "../etc"); err == nil {
		t.Fatalf("expected error on path traversal")
	}
}

// StubClient is a test-only resolver that satisfies db.SecretFetcher and
// is bundled here so multiple service tests can reuse the same shape.
func TestStubClient_GetSecret(t *testing.T) {
	t.Parallel()
	stub := NewStubClient(map[string]string{
		"my-secret": "postgres://u:p@host:5432/db",
	})
	got, err := stub.GetSecret(context.Background(), "my-secret")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if got != "postgres://u:p@host:5432/db" {
		t.Fatalf("unexpected value: %q", got)
	}
}

func TestStubClient_NotFound(t *testing.T) {
	t.Parallel()
	stub := NewStubClient(nil)
	_, err := stub.GetSecret(context.Background(), "missing")
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, ErrSecretNotFound) {
		t.Fatalf("expected ErrSecretNotFound, got %v", err)
	}
	if !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error should name the missing key, got %q", err.Error())
	}
}

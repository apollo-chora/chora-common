// Package secrets — supplementary client-surface unit tests.
//
// The production *Client is environment-backed: no credentials, no
// network. These tests cover every branch: input validation, the
// env-var resolution order, the missing-secret sentinel, and the
// no-op Close lifecycle.
package secrets

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNewClient_ProjectRequired(t *testing.T) {
	t.Parallel()
	if _, err := NewClient(context.Background(), ""); err == nil {
		t.Fatal("expected error on empty project")
	}
}

func TestNewClient_ConstructsEnvBackedClient(t *testing.T) {
	t.Parallel()
	cl, err := NewClient(context.Background(), "chora-unit-test")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
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

// TestClient_GetSecret_InvalidNameFailsBeforeLookup — the resource-name
// validation rejects bad names before any environment lookup, proving
// the guard runs first.
func TestClient_GetSecret_InvalidNameFailsBeforeLookup(t *testing.T) {
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

// TestClient_GetSecret_EnvResolutionOrder — table-driven coverage of the
// SECRET_<NAME> → <NAME> → <raw name> precedence, including the
// normalisation of dashes/underscores and the empty-value-is-missing
// rule. Sequential (not parallel): t.Setenv mutates process env.
func TestClient_GetSecret_EnvResolutionOrder(t *testing.T) {
	c := &Client{project: "chora-unit-test"}

	cases := []struct {
		name    string
		secret  string
		env     map[string]string
		want    string
		wantErr bool
	}{
		{
			name:   "SECRET_ prefix wins",
			secret: "db-dsn",
			env:    map[string]string{"SECRET_DB_DSN": "from-prefix", "DB_DSN": "from-normalised"},
			want:   "from-prefix",
		},
		{
			name:   "normalised name used when prefix absent",
			secret: "db-dsn",
			env:    map[string]string{"DB_DSN": "from-normalised"},
			want:   "from-normalised",
		},
		{
			name:   "raw name used when normalised absent",
			secret: "db-dsn",
			env:    map[string]string{"db-dsn": "from-raw"},
			want:   "from-raw",
		},
		{
			name:   "dashes and underscores normalise to the same var",
			secret: "chora-dev-dsn",
			env:    map[string]string{"SECRET_CHORA_DEV_DSN": "canonical"},
			want:   "canonical",
		},
		{
			name:    "missing secret yields ErrSecretNotFound",
			secret:  "db-dsn",
			env:     map[string]string{},
			wantErr: true,
		},
		{
			name:    "empty value yields ErrSecretNotFound",
			secret:  "db-dsn",
			env:     map[string]string{"SECRET_DB_DSN": "   "},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got, err := c.GetSecret(context.Background(), tc.secret)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got value %q", got)
				}
				if !errors.Is(err, ErrSecretNotFound) {
					t.Errorf("err = %v, want ErrSecretNotFound", err)
				}
				if !strings.Contains(err.Error(), "db-dsn") {
					t.Errorf("error should name the missing key, got %q", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("GetSecret = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestEnvCandidates_Order pins the resolution precedence for coverage.
func TestEnvCandidates_Order(t *testing.T) {
	t.Parallel()
	got := envCandidates("db-rw-dsn")
	want := []string{"SECRET_DB_RW_DSN", "DB_RW_DSN", "db-rw-dsn"}
	if len(got) != len(want) {
		t.Fatalf("envCandidates(%q) = %v, want %v", "db-rw-dsn", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("candidate[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestNormaliseEnvName covers the upper-case + non-alnum→'_' mapping.
func TestNormaliseEnvName(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":              "",
		"db-dsn":        "DB_DSN",
		"chora_dev-dsn": "CHORA_DEV_DSN",
		"MixedCase123":  "MIXEDCASE123",
		"a.b/c":         "A_B_C",
	}
	for in, want := range cases {
		if got := normaliseEnvName(in); got != want {
			t.Errorf("normaliseEnvName(%q) = %q, want %q", in, got, want)
		}
	}
}

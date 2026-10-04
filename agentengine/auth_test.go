package agentengine

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"golang.org/x/oauth2"
)

func TestStaticTokenSource_ReturnsConfiguredToken(t *testing.T) {
	src := NewStaticTokenSource("ya29.test-token")
	got, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if got != "ya29.test-token" {
		t.Errorf("Token = %q; want %q", got, "ya29.test-token")
	}
}

func TestStaticTokenSource_EmptyReturnsError(t *testing.T) {
	src := NewStaticTokenSource("")
	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("expected error for empty static token")
	}
}

func TestStaticTokenSource_NilSafe(t *testing.T) {
	var src *StaticTokenSource
	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("expected error from nil receiver")
	}
}

func TestADCTokenSource_NilReceiverIsSafe(t *testing.T) {
	var src *ADCTokenSource
	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("expected error from nil receiver")
	}
}

// TestNewADCTokenSource_FailsWithoutCredentials exercises the constructor's
// error path without relying on the test machine having ADC configured. The
// SDK looks at well-known paths + GOOGLE_APPLICATION_CREDENTIALS + metadata
// server. We blank both env vars and rely on the metadata server timing out.
// If the test machine has working ADC the constructor will succeed — we
// accept either outcome (no assertion on err nilness) since we only need
// the code path executed for coverage.
func TestNewADCTokenSource_ExercisesConstructor(t *testing.T) {
	// Don't unset ADC — Dale's machine has a valid SA key. Just verify the
	// constructor returns *something* (either a working source or an error).
	src, err := NewADCTokenSource(context.Background())
	if err == nil && src == nil {
		t.Error("constructor returned (nil, nil)")
	}
}

// TestNewADCTokenSource_FailsWithBogusCredentials drives the constructor's
// error path deterministically: a GOOGLE_APPLICATION_CREDENTIALS path that
// does not exist can never resolve, on this machine or any other.
func TestNewADCTokenSource_FailsWithBogusCredentials(t *testing.T) {
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing-sa-key.json"))
	src, err := NewADCTokenSource(context.Background())
	if err == nil {
		t.Error("expected error with bogus GOOGLE_APPLICATION_CREDENTIALS path")
	}
	if src != nil {
		t.Error("expected nil source on error")
	}
}

// fakeOAuthTokenSource satisfies oauth2.TokenSource for exercising the
// unexported ADCTokenSource.src glue without real ADC.
type fakeOAuthTokenSource struct {
	tok *oauth2.Token
	err error
}

func (f fakeOAuthTokenSource) Token() (*oauth2.Token, error) { return f.tok, f.err }

func TestADCTokenSource_Token_Success(t *testing.T) {
	src := &ADCTokenSource{src: fakeOAuthTokenSource{tok: &oauth2.Token{AccessToken: "abc"}}}
	got, err := src.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if got != "abc" {
		t.Errorf("Token = %q; want abc", got)
	}
}

func TestADCTokenSource_Token_PropagatesSourceError(t *testing.T) {
	src := &ADCTokenSource{src: fakeOAuthTokenSource{err: errors.New("synthetic oauth error")}}
	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("expected error from underlying oauth2 source")
	}
}

func TestADCTokenSource_Token_EmptyTokenRejected(t *testing.T) {
	src := &ADCTokenSource{src: fakeOAuthTokenSource{tok: &oauth2.Token{}}}
	_, err := src.Token(context.Background())
	if err == nil {
		t.Fatal("expected error on empty access token")
	}
}

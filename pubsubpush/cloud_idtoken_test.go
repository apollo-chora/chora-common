// cloud_idtoken_test.go — minimal smoke coverage for the production
// google.golang.org/api/idtoken adapter. Real signature validation is
// integration-tested at the per-service deployment level (M14 chaos suite
// touches it); here we exercise the wiring + the malformed-token error
// path.
package pubsubpush_test

import (
	"context"
	"strings"
	"testing"

	"github.com/5007-Capstone/chora/libs/chora-go-common/pubsubpush"
)

func TestNewGoogleValidateToken_Wires(t *testing.T) {
	fn := pubsubpush.NewGoogleValidateToken()
	if fn == nil {
		t.Fatalf("NewGoogleValidateToken returned nil")
	}
	// Malformed token — google idtoken.Validate must reject without
	// reaching the network. We assert err != nil; the exact message is
	// owned by the upstream library and may shift across versions.
	_, err := fn(context.Background(), "not-a-jwt", "https://example.com/x")
	if err == nil {
		t.Fatalf("expected idtoken.Validate to reject a malformed token")
	}
	// Sanity-check error is descriptive (covers wrap path).
	if got := strings.TrimSpace(err.Error()); got == "" {
		t.Fatalf("error is empty")
	}
}

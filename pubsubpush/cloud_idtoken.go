// cloud_idtoken.go — production wiring for the OIDC token validator.
//
// Uses google.golang.org/api/idtoken which (a) fetches + caches the Google
// JWKS at https://www.googleapis.com/oauth2/v3/certs, (b) validates the JWT
// signature against the matching public key, and (c) enforces the audience
// claim. We then re-extract the standard claims onto TokenClaims so the
// rest of pubsubpush.Verifier is decoupled from the idtoken types.
package pubsubpush

import (
	"context"
	"fmt"

	"google.golang.org/api/idtoken"
)

// NewGoogleValidateToken returns a ValidateTokenFunc that uses
// google.golang.org/api/idtoken.Validate to verify the token signature +
// audience against the Google JWKS at oauth2/v3/certs. The returned func is
// safe for concurrent use.
//
// Per the always-loaded secrets-and-env rule + the iter G.8 / #11 brief,
// the audience MUST come from env (the per-subscription public push URL)
// at service-boot time. This factory is wiring; the policy lives at the
// call site.
func NewGoogleValidateToken() ValidateTokenFunc {
	return func(ctx context.Context, token, audience string) (TokenClaims, error) {
		payload, err := idtoken.Validate(ctx, token, audience)
		if err != nil {
			return TokenClaims{}, err
		}
		if payload == nil {
			return TokenClaims{}, fmt.Errorf("idtoken validate returned nil payload")
		}
		c := TokenClaims{
			Subject:  payload.Subject,
			Issuer:   payload.Issuer,
			Audience: audience,
		}
		if v, ok := payload.Claims["email"].(string); ok {
			c.Email = v
		}
		switch v := payload.Claims["email_verified"].(type) {
		case bool:
			c.EmailVerified = v
		case string:
			c.EmailVerified = v == "true"
		}
		return c, nil
	}
}

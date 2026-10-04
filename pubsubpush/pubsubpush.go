// Package pubsubpush is the canonical Pub/Sub push-subscription helper for
// Chora HTTP services.
//
// Cloud Pub/Sub delivers each message to a subscriber's push endpoint as a
// JSON POST body of the shape:
//
//	{
//	  "message": {
//	    "data":        "<base64 protobuf-or-json payload>",
//	    "messageId":   "<broker-assigned>",
//	    "publishTime": "<RFC3339>",
//	    "attributes":  {"<key>": "<value>", ...}
//	  },
//	  "subscription": "projects/<project>/subscriptions/<name>"
//	}
//
// AND stamps an OIDC ID token in the Authorization header (when the
// subscription is configured with `pushConfig.oidcToken`). This package
// handles:
//
//  1. Decoding the envelope (Decode).
//  2. Verifying the OIDC token authenticity + audience + service-account
//     allowlist (Verifier).
//  3. Composing both above + a typed dispatch callback into a stdlib
//     http.Handler (NewHandler).
//
// All Chora services that receive Pub/Sub push deliveries MUST use this
// package so the security envelope is consistent. Per the iter G.8 / #11
// brief and the always-loaded secrets-and-env rule, all configuration
// (audience, trusted SA emails, validator hook) is supplied via env at
// service boot — never inlined.
//
// Hexagonal placement: this is a generic transport adapter. Domain
// subscriber structs live in services/{svc}/internal/adapter/subscribers
// and are invoked from the per-service HTTP handler's Dispatch callback.
package pubsubpush

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ErrBadRequest is returned by Decode when the inbound HTTP request does not
// conform to the Cloud Pub/Sub push envelope contract. Handler maps it to a
// 400 response.
var ErrBadRequest = errors.New("pubsubpush: bad request")

// ErrUnauthorized is returned by Verifier.Verify when the OIDC token is
// missing, malformed, fails JWKS validation, fails audience match, or fails
// the trusted-service-account allowlist. Handler maps it to a 401 response.
var ErrUnauthorized = errors.New("pubsubpush: unauthorized")

// allowedIssuers is the canonical set of issuers Google's Pub/Sub push OIDC
// tokens carry. (Google rotates between these two; both are valid.)
var allowedIssuers = map[string]struct{}{
	"https://accounts.google.com": {},
	"accounts.google.com":         {},
}

// PushMessage is the decoded form of a Cloud Pub/Sub push delivery.
type PushMessage struct {
	// Data is the base64-decoded payload body — typically the Protobuf
	// wire bytes for the topic's registered schema, or a JSON envelope in
	// dev / cross-domain JSON paths.
	Data []byte

	// MessageID is the broker-assigned identifier (NOT the Chora
	// envelope's event_id — that lives in Attributes).
	MessageID string

	// PublishTime is the server-side timestamp Pub/Sub stamps when the
	// publisher PUBSUBLISH call lands.
	PublishTime time.Time

	// Attributes are the publisher-supplied metadata + the projected
	// Chora envelope fields (event_id, tenant_id, gcid, traceparent ...).
	Attributes map[string]string

	// Subscription is the full subscription resource name —
	// "projects/<proj>/subscriptions/<name>". Useful for per-topic
	// dispatch when one push handler fronts multiple subscriptions.
	Subscription string
}

// pushEnvelope is the wire shape Pub/Sub sends. Internal.
type pushEnvelope struct {
	Message *struct {
		Data        string            `json:"data"`
		MessageID   string            `json:"messageId"`
		PublishTime string            `json:"publishTime"`
		Attributes  map[string]string `json:"attributes,omitempty"`
	} `json:"message"`
	Subscription string `json:"subscription"`
}

// Decode reads the Pub/Sub push envelope from r and returns a PushMessage.
// Returns ErrBadRequest (wrapped via %w) for any wire-shape violation.
func Decode(r *http.Request) (PushMessage, error) {
	if r.Method != http.MethodPost {
		return PushMessage{}, fmt.Errorf("%w: method %s (want POST)", ErrBadRequest, r.Method)
	}
	if r.Body == nil {
		return PushMessage{}, fmt.Errorf("%w: empty body", ErrBadRequest)
	}
	// Pub/Sub messages can be up to 10MiB. Cap defensively at 12 MiB so
	// callers can't OOM us with crafted requests.
	body, err := io.ReadAll(io.LimitReader(r.Body, 12*1024*1024))
	if err != nil {
		return PushMessage{}, fmt.Errorf("%w: read body: %v", ErrBadRequest, err)
	}
	var env pushEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return PushMessage{}, fmt.Errorf("%w: unmarshal: %v", ErrBadRequest, err)
	}
	if env.Message == nil {
		return PushMessage{}, fmt.Errorf("%w: missing message", ErrBadRequest)
	}
	var data []byte
	if env.Message.Data != "" {
		data, err = base64.StdEncoding.DecodeString(env.Message.Data)
		if err != nil {
			return PushMessage{}, fmt.Errorf("%w: decode base64 data: %v", ErrBadRequest, err)
		}
	}
	pt := time.Time{}
	if env.Message.PublishTime != "" {
		if parsed, perr := time.Parse(time.RFC3339Nano, env.Message.PublishTime); perr == nil {
			pt = parsed
		}
	}
	return PushMessage{
		Data:         data,
		MessageID:    env.Message.MessageID,
		PublishTime:  pt,
		Attributes:   env.Message.Attributes,
		Subscription: env.Subscription,
	}, nil
}

// ---------------------------------------------------------------------------
// OIDC verifier
// ---------------------------------------------------------------------------

// TokenClaims is the minimal Google OIDC claim set Verifier inspects. The
// concrete JWKS-validating implementation lives in cloud_idtoken.go (it
// imports google.golang.org/api/idtoken which carries the JWKS cache + RSA
// public-key validation). Tests inject a stub via VerifierConfig.ValidateToken.
type TokenClaims struct {
	// Subject is the Google `sub` claim (the service-account's GAIA ID).
	Subject string
	// Email is the service-account email Pub/Sub used to mint the token.
	Email string
	// EmailVerified mirrors `email_verified`; Google sets this to true on
	// SA-minted tokens.
	EmailVerified bool
	// Audience is the `aud` claim — equal to the push endpoint URL the
	// subscription is configured for.
	Audience string
	// Issuer is the `iss` claim — Google rotates between "accounts.google.com"
	// and "https://accounts.google.com".
	Issuer string
}

// ValidateTokenFunc is the seam Verifier uses to validate raw OIDC bearer
// tokens. The default production wiring (NewGoogleValidateToken in
// cloud_idtoken.go) uses google.golang.org/api/idtoken. Tests inject a stub.
type ValidateTokenFunc func(ctx context.Context, token, audience string) (TokenClaims, error)

// VerifierConfig wires a Verifier.
type VerifierConfig struct {
	// Audience is the expected `aud` claim — the public push endpoint URL
	// (e.g. "https://chora-consumption-xyz.run.app/internal/pubsub/familiar-growth").
	// When empty, OIDC verification is DISABLED (dev / tests only). Production
	// MUST set this to the Cloud Run URL each subscription is configured
	// with.
	Audience string

	// TrustedServiceAccs is the optional allowlist of acceptable `email`
	// claims. When non-empty, only tokens whose email is in this list pass.
	// When empty, any verified google-issued SA token passes (still
	// audience-checked). Keep this set to the per-environment subscriber SA
	// email (typically `pubsub-invoker@<project>.iam.gserviceaccount.com`).
	TrustedServiceAccs []string

	// ValidateToken is the JWKS / signature-validation seam. Default
	// production wiring is NewGoogleValidateToken; tests inject a stub.
	// When Audience is empty AND ValidateToken is nil, Verify becomes a
	// no-op (dev / in-process tests).
	ValidateToken ValidateTokenFunc
}

// Verifier is an OIDC token verifier for Cloud Pub/Sub push deliveries. It
// is safe for concurrent use after construction.
type Verifier struct {
	cfg     VerifierConfig
	trusted map[string]struct{}
}

// NewVerifier constructs a Verifier from cfg.
func NewVerifier(cfg VerifierConfig) *Verifier {
	v := &Verifier{cfg: cfg, trusted: make(map[string]struct{}, len(cfg.TrustedServiceAccs))}
	for _, s := range cfg.TrustedServiceAccs {
		if s = strings.TrimSpace(s); s != "" {
			v.trusted[strings.ToLower(s)] = struct{}{}
		}
	}
	return v
}

// Enabled reports whether the verifier will perform any check. When false
// (cfg.Audience == "" AND cfg.ValidateToken == nil), Verify is a no-op.
func (v *Verifier) Enabled() bool {
	return v.cfg.Audience != "" || v.cfg.ValidateToken != nil
}

// Verify checks the Authorization header for a valid Pub/Sub OIDC bearer
// token. Returns nil on success, ErrUnauthorized-wrapped error on failure.
//
// When the verifier is Disabled (no audience + no validator), Verify
// returns nil unconditionally — caller is expected to enforce auth via
// service-mesh mTLS or a network ACL in that mode.
func (v *Verifier) Verify(ctx context.Context, r *http.Request) error {
	if !v.Enabled() {
		return nil
	}
	authz := r.Header.Get("Authorization")
	if authz == "" {
		return fmt.Errorf("%w: missing Authorization header", ErrUnauthorized)
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(authz, prefix) {
		return fmt.Errorf("%w: Authorization scheme is not Bearer", ErrUnauthorized)
	}
	token := strings.TrimSpace(authz[len(prefix):])
	if token == "" {
		return fmt.Errorf("%w: empty bearer token", ErrUnauthorized)
	}
	if v.cfg.ValidateToken == nil {
		return fmt.Errorf("%w: no token validator configured", ErrUnauthorized)
	}
	claims, err := v.cfg.ValidateToken(ctx, token, v.cfg.Audience)
	if err != nil {
		return fmt.Errorf("%w: token invalid: %v", ErrUnauthorized, err)
	}
	if _, ok := allowedIssuers[claims.Issuer]; !ok {
		return fmt.Errorf("%w: issuer %q not allowed", ErrUnauthorized, claims.Issuer)
	}
	if !claims.EmailVerified {
		return fmt.Errorf("%w: email_verified=false (email=%s)", ErrUnauthorized, claims.Email)
	}
	if len(v.trusted) > 0 {
		if _, ok := v.trusted[strings.ToLower(claims.Email)]; !ok {
			return fmt.Errorf("%w: service account %q not in allowlist", ErrUnauthorized, claims.Email)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// HTTP handler
// ---------------------------------------------------------------------------

// DispatchFunc is the per-handler callback the Pub/Sub push handler invokes
// after a successful decode + verify. It MUST be idempotent — Cloud Pub/Sub
// guarantees at-least-once delivery.
//
// Return nil to ack the message (200); return any error to nack (500 → retry
// per Pub/Sub's exponential-backoff + DLQ topology).
type DispatchFunc func(ctx context.Context, msg PushMessage) error

// HandlerConfig wires a push handler.
type HandlerConfig struct {
	// Verifier is mandatory. Use NewVerifier(VerifierConfig{}) for the
	// no-op verifier in dev.
	Verifier *Verifier
	// Dispatch is mandatory. Panics on nil at NewHandler time.
	Dispatch DispatchFunc
	// Logger receives structured one-liners on auth / decode / dispatch
	// failure. Optional — falls back to the stdlib log package.
	Logger interface {
		Printf(format string, args ...interface{})
	}
}

// handler is the http.Handler returned by NewHandler.
type handler struct {
	verifier *Verifier
	dispatch DispatchFunc
	logger   interface {
		Printf(format string, args ...interface{})
	}
}

// NewHandler builds the http.Handler for a Pub/Sub push subscription.
// Panics if cfg.Dispatch is nil.
func NewHandler(cfg HandlerConfig) http.Handler {
	if cfg.Dispatch == nil {
		panic("pubsubpush: Dispatch is required")
	}
	if cfg.Verifier == nil {
		cfg.Verifier = NewVerifier(VerifierConfig{})
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	return &handler{
		verifier: cfg.Verifier,
		dispatch: cfg.Dispatch,
		logger:   cfg.Logger,
	}
}

// ServeHTTP runs verify → decode → dispatch in order. On verifier failure
// returns 401; on decode failure 400; on dispatch failure 500 (Pub/Sub
// retries); on success 204.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. OIDC verification first — fail closed before we even read the body
	// in case of a forged Authorization header.
	if err := h.verifier.Verify(r.Context(), r); err != nil {
		h.logger.Printf("pubsubpush: verify failed: %v", err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	// 2. Decode envelope.
	msg, err := Decode(r)
	if err != nil {
		h.logger.Printf("pubsubpush: decode failed: %v", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	// 3. Dispatch — subscriber struct is responsible for at-least-once
	// idempotency (composite-key UNIQUE constraints + idempotency_key
	// trackers). Duplicate events are NOT errors here — the subscriber
	// returns nil for "already processed" so we 200. Pub/Sub treats any
	// 2xx as ack.
	if err := h.dispatch(r.Context(), msg); err != nil {
		h.logger.Printf("pubsubpush: dispatch failed (subscription=%s message_id=%s): %v",
			msg.Subscription, msg.MessageID, err)
		http.Error(w, "dispatch failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

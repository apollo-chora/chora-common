// Package pubsub is the schema-validating publisher contract for the
// chora platform's centralised Pub/Sub broker (chora-489812).
//
// Source-of-truth:
//   - .claude/skills/pub-sub-topology/SKILL.md (always-loaded conventions)
//   - chora-contracts/proto/chora/common/v1/envelope.proto
//   - docs/architecture-review-inputs-2026-05-07.md Tier 2 D8
//
// Two responsibilities:
//
//  1. Validate the topic name + envelope BEFORE any wire bytes are sent.
//     The Pub/Sub Schema Registry will reject malformed Protobuf payloads
//     at the broker side, but client-side validation surfaces the failure
//     with a useful error (broker errors come back as opaque grpc.Code).
//  2. Project the envelope onto Pub/Sub message attributes so subscribers
//     and OTLP observers can read tenant_id / traceparent / IMDA tags
//     without decoding the payload.
//
// The actual transport (Pub/Sub Go client) lives in service-specific
// adapters under services/{service}/internal/adapter/pubsub/. This
// package is a thin contract that those adapters implement, so the
// schema-validation logic is shared and tested in one place.
package pubsub

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
)

// PublishCall is the wire-shaped value an adapter sends to the broker.
type PublishCall struct {
	Topic      string
	Data       []byte
	Attributes map[string]string
}

// RawPublisher is the minimal contract this package needs from a transport
// adapter. Service adapters implement this with the gcp/pubsub Go client
// (or, in tests, an in-memory recorder).
type RawPublisher interface {
	Publish(ctx context.Context, call PublishCall) error
}

// Publisher is the Chora-canonical envelope-validating wrapper.
type Publisher struct {
	raw RawPublisher
}

// NewPublisher wraps a RawPublisher with envelope + topic validation.
func NewPublisher(raw RawPublisher) *Publisher {
	return &Publisher{raw: raw}
}

// PublishEnvelope validates the envelope + topic name then delegates to
// the wrapped RawPublisher with attributes projected from the envelope.
//
// Envelope fields projected onto attributes:
//   - event_id, idempotency_key, tenant_id, gcid (always)
//   - occurred_at, published_at (RFC3339Nano, always)
//   - traceparent, tracestate (always — `traceparent` mandatory by Build)
//   - source_project, source_service, schema_version (always)
//   - correlation_id, causation_id (only when non-empty)
//   - chora_imda_dimension, imda_lifecycle_stage (only when non-empty)
//
// `data` is the marshaled Protobuf payload (the broker's Pub/Sub Schema
// Registry validates this against the topic's registered .proto). This
// function does NOT marshal — callers do that in their service adapter.
func (p *Publisher) PublishEnvelope(ctx context.Context, topic string, env envelope.Envelope, data []byte) error {
	if err := envelope.Validate(env); err != nil {
		return fmt.Errorf("pubsub: envelope validation failed: %w", err)
	}
	if err := ValidateTopicName(topic); err != nil {
		return fmt.Errorf("pubsub: topic validation failed: %w", err)
	}

	attrs := envelopeAttributes(env)
	return p.raw.Publish(ctx, PublishCall{
		Topic:      topic,
		Data:       data,
		Attributes: attrs,
	})
}

// envelopeAttributes builds the canonical attribute map.
func envelopeAttributes(env envelope.Envelope) map[string]string {
	attrs := map[string]string{
		"event_id":        env.EventID,
		"idempotency_key": env.IdempotencyKey,
		"tenant_id":       env.TenantID,
		"gcid":            env.GCID,
		"occurred_at":     env.OccurredAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
		"published_at":    env.PublishedAt.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
		"traceparent":     env.Traceparent,
		"source_project":  env.SourceProject,
		"source_service":  env.SourceService,
		"schema_version":  strconv.FormatInt(int64(env.SchemaVersion), 10),
	}
	if env.Tracestate != "" {
		attrs["tracestate"] = env.Tracestate
	}
	if env.CorrelationID != "" {
		attrs["correlation_id"] = env.CorrelationID
	}
	if env.CausationID != "" {
		attrs["causation_id"] = env.CausationID
	}
	if env.ChoraImdaDimension != "" {
		attrs["chora_imda_dimension"] = env.ChoraImdaDimension
	}
	if env.ImdaLifecycleStage != "" {
		attrs["imda_lifecycle_stage"] = env.ImdaLifecycleStage
	}
	// gcid is allowed empty per envelope.Validate — strip from attrs to keep
	// the wire representation clean.
	if env.GCID == "" {
		delete(attrs, "gcid")
	}
	return attrs
}

// ValidateTopicName enforces the chora.{domain}.{aggregate}.{event_type}.v{N}
// taxonomy locked in pub-sub-topology/SKILL.md. Returns nil on success.
//
// The 11 known domains are the closed vocabulary; aggregate + event_type
// are validated as snake_case. Major version v{N} ≥ 1 (no leading zero).
func ValidateTopicName(name string) error {
	if name == "" {
		return fmt.Errorf("topic name empty")
	}
	parts := strings.Split(name, ".")
	if len(parts) < 5 {
		return fmt.Errorf("topic %q: expected chora.{domain}.{aggregate}.{event_type}.v{N}", name)
	}
	if parts[0] != "chora" {
		return fmt.Errorf("topic %q: must start with chora", name)
	}
	domain := parts[1]
	if _, ok := knownDomains[domain]; !ok {
		return fmt.Errorf("topic %q: unknown domain %q", name, domain)
	}

	// Last part is v{N}.
	versionPart := parts[len(parts)-1]
	if !versionRe.MatchString(versionPart) {
		return fmt.Errorf("topic %q: version segment %q must match v[1-9][0-9]*", name, versionPart)
	}

	// Parts 2..(len-2) are aggregate + event_type segments — each MUST be
	// snake_case (lowercase, digits, underscores). We treat each as a
	// segment; collectively at least one aggregate + one event_type.
	if len(parts) < 5 {
		return fmt.Errorf("topic %q: missing aggregate or event_type segment", name)
	}
	for _, seg := range parts[2 : len(parts)-1] {
		if !snakeRe.MatchString(seg) {
			return fmt.Errorf("topic %q: segment %q must be snake_case (lowercase + digits + underscore)", name, seg)
		}
	}
	return nil
}

var (
	versionRe = regexp.MustCompile(`^v[1-9][0-9]*$`)
	snakeRe   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// knownDomains is the closed 11-domain vocabulary per Tier 1 D1 + Tier 2 D7
// (5 core Content {verb} + 6 supporting/platform), plus the cross-cutting
// `closure` saga namespace per chora-contracts S0.2 reconciliation
// (docs/m13/contracts-reconciliation-2026-05-09.md §1.1).
var knownDomains = map[string]struct{}{
	// 5 core
	"creation":    {},
	"consumption": {},
	"sharing":     {},
	"delivery":    {},
	"a2a":         {},
	// 6 supporting
	"identity":      {},
	"tenancy":       {},
	"governance":    {},
	"observability": {},
	"notifications": {},
	"ai_kernel":     {},
	// Cross-cutting saga namespace (no owning DB; orchestrator-emitted).
	"closure": {},
}

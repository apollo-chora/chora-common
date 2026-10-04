// ClosureAckPublisher — shared production implementation of the
// per-domain ClosurePublisher port used by every service's
// closure_subscriber (CHO-1719 gap 4).
//
// Each domain's events package declares a structurally identical port:
//
//	Publish(topic, tenantID, gcid, traceparent string,
//	        payload map[string]interface{}) error
//
// This adapter satisfies all of them, projecting the call onto the
// canonical envelope (envelope.Build) + an OutboxCompatible publisher
// (CloudPublisher in production, InMemoryBus in dev/tests).
//
// Delivery note: acks publish DIRECTLY (not via the per-domain outbox).
// The publish happens inside the subscriber's idempotent inbox.Process
// closure — a failed publish errors Handle, the pull loop NACKs, and
// Pub/Sub redelivers; the orchestrator's ack recording is idempotent on
// the deterministic idempotency key below, so at-least-once is safe.
package pubsub

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/5007-Capstone/chora/libs/chora-go-common/envelope"
)

// ClosureAckPublisher publishes closure-saga acks on
// chora.{domain}.account.pseudonymised.v1 (and the failed variant).
type ClosureAckPublisher struct {
	inner         OutboxCompatible
	sourceProject string
	sourceService string
}

// NewClosureAckPublisher wraps an OutboxCompatible publisher.
func NewClosureAckPublisher(
	inner OutboxCompatible, sourceProject, sourceService string,
) *ClosureAckPublisher {
	return &ClosureAckPublisher{
		inner:         inner,
		sourceProject: sourceProject,
		sourceService: sourceService,
	}
}

// Publish satisfies the per-domain ClosurePublisher port.
func (p *ClosureAckPublisher) Publish(
	topic, tenantID, gcid, traceparent string,
	payload map[string]interface{},
) error {
	if p == nil || p.inner == nil {
		return errors.New("pubsub: ClosureAckPublisher not initialised")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("pubsub: closure ack payload encode: %w", err)
	}

	env := envelope.Build(context.Background(), envelope.BuildOpts{
		SourceProject:      p.sourceProject,
		SourceService:      p.sourceService,
		SchemaVersion:      1,
		ChoraImdaDimension: "accountability",
		ImdaLifecycleStage: "runtime",
	})
	env.TenantID = tenantID
	env.GCID = gcid
	if traceparent != "" {
		env.Traceparent = traceparent
	}
	// Deterministic idempotency key — a redelivered request re-acks with
	// the same key; the orchestrator collapses duplicates.
	if sid, ok := payload["saga_id"].(string); ok && sid != "" && gcid != "" {
		env.IdempotencyKey = "pseudonymise_ack:" + sid + ":" + gcid
	}

	return p.inner.Publish(context.Background(), topic, env, body)
}

package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/apollo-chora/chora-common/envelope"
)

// ClosureAckPublisher publishes closure-saga acks on
// chora.{domain}.account.pseudonymised.v1 (and the failed variant). It
// satisfies the per-domain ClosurePublisher port used by every service's
// closure_subscriber.
//
// Delivery note: acks publish DIRECTLY (not via the per-domain outbox). The
// publish happens inside the subscriber's idempotent inbox.Process closure —
// a failed publish errors Handle, the consumer NAKs, and the broker redelivers;
// the orchestrator's ack recording is idempotent on the deterministic
// idempotency key below, so at-least-once is safe.
type ClosureAckPublisher struct {
	inner         Publisher
	sourceProject string
	sourceService string
}

// NewClosureAckPublisher wraps a Publisher.
func NewClosureAckPublisher(
	inner Publisher, sourceProject, sourceService string,
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
		return errors.New("eventbus: ClosureAckPublisher not initialised")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("eventbus: closure ack payload encode: %w", err)
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

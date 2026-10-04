// ensure.go — self-healing subscription provisioning (CHO-1811).
//
// Topics are centralised infra (Terraform, chora-489812 platform host), but
// per .claude/skills/pub-sub-topology subscriptions are CONSUMER-owned. A
// consuming service that assumes its pull subscriptions already exist + fail-
// exits on NotFound leaves permanently-dead subscribers when a stack is
// rebuilt without re-running the (out-of-band) provisioning step — the root
// cause of the ADR-164 marketplace-saga outage (CHO-1811): paid add-on
// subscribes never activated because chora-tenancy-payments-* subs were never
// provisioned.
//
// EnsureSubscription makes the consumer self-provision its own subscription at
// boot (GetOrCreate), so a fresh/kicked stack self-heals. The runtime SA needs
// roles/pubsub.editor (chora-platform-svc has it). Forward-only: a freshly
// created subscription only receives messages published after creation (no
// historical replay — matched to the CHO-1811 owner decision).
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"time"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// EnsureSubscriptionConfig is the desired state for a pull subscription.
// Names are canonical SHORT names (e.g. "chora-tenancy-payments-...",
// "chora.payments.x.y.v1") — EnsureSubscription resolves them to fully
// qualified resource paths under the client's project.
type EnsureSubscriptionConfig struct {
	Subscription        string        // short subscription id
	Topic               string        // canonical short topic name
	DeadLetterTopic     string        // canonical short DLQ topic; "" = no dead-letter policy
	AckDeadline         time.Duration // → AckDeadlineSeconds (rounded)
	MaxDeliveryAttempts int32         // dead-letter max attempts (ignored if DeadLetterTopic == "")
	MinBackoff          time.Duration // retry policy min; 0 = no retry policy
	MaxBackoff          time.Duration // retry policy max
	RetentionDuration   time.Duration // message retention; 0 = leave default
	NeverExpire         bool          // true → expiration_policy{} (never expire)
}

// SubscriptionEnsurer is the narrow capability for self-healing subscription
// provisioning. Implemented by *GCPClient. Kept OUT of CloudPubSubClient so the
// many in-memory/stub implementations don't have to grow an admin method —
// callers type-assert the client to this interface (prod GCPClient satisfies it;
// stubs simply skip ensuring).
type SubscriptionEnsurer interface {
	EnsureSubscription(ctx context.Context, cfg EnsureSubscriptionConfig) (created bool, err error)
}

// ErrEnsurePermissionDenied tags an EnsureSubscription failure caused by the
// runtime SA lacking pubsub.subscriptions.get/create — the EXPECTED posture
// where subscriptions are provisioned out-of-band and workloads only hold
// subscriber rights (CHO-2128 F3). Callers should log one line and bind
// directly; the subscription usually already exists.
var ErrEnsurePermissionDenied = errors.New("pubsub: ensure subscription permission denied")

// classifyEnsureError wraps an admin-API failure for return. PermissionDenied
// collapses to a SHORT single-line error tagged ErrEnsurePermissionDenied —
// the multi-line IAM wall (ErrorInfo metadata + troubleshooter URL) carries no
// action beyond the posture the sentinel already names, and it buries boot
// logs. Every other code keeps the full wrapped detail (fail-loud unchanged).
func classifyEnsureError(op, subscription string, err error) error {
	if status.Code(err) == codes.PermissionDenied {
		return fmt.Errorf("%w: %s %s — runtime SA lacks pubsub.subscriptions.%s (subscriptions are provisioned OOB); bind directly",
			ErrEnsurePermissionDenied, op, subscription, op)
	}
	return fmt.Errorf("pubsub: EnsureSubscription %s %s: %w", op, subscription, err)
}

var _ SubscriptionEnsurer = (*GCPClient)(nil)

// EnsureSubscription creates the subscription if it does not already exist.
// Returns (false, nil) when it already exists (the common steady-state path)
// and (true, nil) when it was created. Idempotent + safe under concurrent boot
// (an AlreadyExists race is treated as "exists"). A non-NotFound lookup error
// or a real create error is returned (fail loud — never silently skip).
func (c *GCPClient) EnsureSubscription(ctx context.Context, cfg EnsureSubscriptionConfig) (bool, error) {
	if cfg.Subscription == "" || cfg.Topic == "" {
		return false, errors.New("pubsub: EnsureSubscription: subscription and topic required")
	}
	subPath := fmt.Sprintf("projects/%s/subscriptions/%s", c.projectID, cfg.Subscription)

	_, err := c.client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subPath})
	if err == nil {
		return false, nil // already exists
	}
	if status.Code(err) != codes.NotFound {
		return false, classifyEnsureError("get", cfg.Subscription, err)
	}

	want := buildSubscriptionProto(c.projectID, cfg)
	if _, err := c.client.SubscriptionAdminClient.CreateSubscription(ctx, want); err != nil {
		if status.Code(err) == codes.AlreadyExists {
			return false, nil // concurrent-boot race — another replica created it
		}
		return false, classifyEnsureError("create", cfg.Subscription, err)
	}
	return true, nil
}

// buildSubscriptionProto maps an EnsureSubscriptionConfig to the pubsubpb
// Subscription create payload. Pure (no I/O) so it is unit-testable without a
// live admin client.
func buildSubscriptionProto(projectID string, cfg EnsureSubscriptionConfig) *pubsubpb.Subscription {
	sub := &pubsubpb.Subscription{
		Name:  fmt.Sprintf("projects/%s/subscriptions/%s", projectID, cfg.Subscription),
		Topic: fmt.Sprintf("projects/%s/topics/%s", projectID, cfg.Topic),
	}
	if cfg.AckDeadline > 0 {
		sub.AckDeadlineSeconds = int32(cfg.AckDeadline.Round(time.Second) / time.Second)
	}
	if cfg.RetentionDuration > 0 {
		sub.MessageRetentionDuration = durationpb.New(cfg.RetentionDuration)
	}
	if cfg.DeadLetterTopic != "" {
		sub.DeadLetterPolicy = &pubsubpb.DeadLetterPolicy{
			DeadLetterTopic:     fmt.Sprintf("projects/%s/topics/%s", projectID, cfg.DeadLetterTopic),
			MaxDeliveryAttempts: cfg.MaxDeliveryAttempts,
		}
	}
	if cfg.MinBackoff > 0 || cfg.MaxBackoff > 0 {
		sub.RetryPolicy = &pubsubpb.RetryPolicy{
			MinimumBackoff: durationpb.New(cfg.MinBackoff),
			MaximumBackoff: durationpb.New(cfg.MaxBackoff),
		}
	}
	if cfg.NeverExpire {
		// expiration_policy with an unset TTL = never expire (matches
		// `gcloud pubsub subscriptions create --expiration-period=never`).
		sub.ExpirationPolicy = &pubsubpb.ExpirationPolicy{}
	}
	return sub
}

// gcp_client.go — production wrapper around cloud.google.com/go/pubsub/v2
// that satisfies the CloudPubSubClient interface declared in cloud_pubsub.go.
//
// Per CLAUDE.md §6 + .claude/skills/pub-sub-topology, every Chora Go
// service that publishes events uses this wrapper as its CloudPubSubClient.
// Topic names are passed in as canonical
// `chora.{domain}.{aggregate}.{event_type}.v{N}` strings; the wrapper
// resolves them to fully-qualified resource paths
// (`projects/{project}/topics/{topic}`).
//
// Resilience-priority directive (`feedback_resilience_priority`):
//
//   - Publishes are async by default; result.Get(ctx) waits for
//     server-side acknowledgement so caller backpressure works.
//   - Topic publishers are cached (one *pubsub.Publisher per topic); a
//     dropped TCP connection is recovered transparently by the gRPC client
//     between publishes.
//   - Receive uses Pull semantics with the SDK's flow control; ctx
//     cancellation drains in-flight messages cleanly so a SIGTERM-ed pod
//     completes mid-message work before exiting.
//
// Test seam: pass a stubCloudClient (in cloud_pubsub_test.go) instead.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	gcppubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GCPClient is the production Cloud Pub/Sub adapter. Wrap a single
// instance for the service lifetime; caller MUST defer Close() in main().
type GCPClient struct {
	client    *gcppubsub.Client
	projectID string

	mu         sync.RWMutex
	publishers map[string]*gcppubsub.Publisher
}

// NewGCPClient builds a GCPClient for the supplied project. Authentication
// uses Application Default Credentials (Workload Identity Federation in
// production).
func NewGCPClient(ctx context.Context, projectID string) (*GCPClient, error) {
	if projectID == "" {
		return nil, errors.New("pubsub: NewGCPClient: projectID required")
	}
	cli, err := gcppubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: NewGCPClient: %w", err)
	}
	return &GCPClient{
		client:     cli,
		projectID:  projectID,
		publishers: map[string]*gcppubsub.Publisher{},
	}, nil
}

// Close stops all publishers and closes the underlying gRPC client.
func (c *GCPClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range c.publishers {
		p.Stop()
	}
	c.publishers = nil
	return c.client.Close()
}

// PublishMessage delivers data + attrs to the named topic. Topic name is
// the canonical short name (e.g. "chora.identity.user.created.v1") — the
// wrapper resolves it to a fully-qualified resource.
func (c *GCPClient) PublishMessage(ctx context.Context, topic string, data []byte, attrs map[string]string) (string, error) {
	if topic == "" {
		return "", errors.New("pubsub: PublishMessage: topic required")
	}
	pub := c.getOrCachePublisher(topic)
	res := pub.Publish(ctx, &gcppubsub.Message{Data: data, Attributes: attrs})
	id, err := res.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("pubsub: publish %s: %w", topic, err)
	}
	return id, nil
}

// SubscriptionReceive runs a Pull-style receive loop on the named
// subscription. The handler MUST be idempotent — Pub/Sub is at-least-once.
//
// Returns when ctx is canceled or a permanent error from the underlying
// client surfaces.
func (c *GCPClient) SubscriptionReceive(ctx context.Context, subscription string, handler func(ctx context.Context, msg CloudMessage) error) error {
	if subscription == "" {
		return errors.New("pubsub: SubscriptionReceive: subscription required")
	}
	sub := c.client.Subscriber(subscription)
	return sub.Receive(ctx, func(ctx context.Context, m *gcppubsub.Message) {
		cm := CloudMessage{
			ID:          m.ID,
			Data:        m.Data,
			Attributes:  m.Attributes,
			PublishTime: m.PublishTime,
			Ack:         m.Ack,
			Nack:        m.Nack,
		}
		dispatchReceivedMessage(ctx, subscription, cm, handler)
	})
}

// dispatchReceivedMessage runs handler on a received message. On a handler
// error it LOGS at ERROR (D5 — these nacked failures were previously silent,
// the `feedback_d6_resilience_first_class` "never silent" contract) then
// defensively Nacks. The handler should already have Nacked, but the
// per-skill contract is "handler error → Nack"; the Pub/Sub SDK prevents a
// double Ack/Nack so this is safe. On success the handler has already Acked,
// so this returns without further action.
//
// This is the SOLE subscriber-side failure log: every received message funnels
// through here, and CloudSubscriber.Subscribe no longer logs at its own layer
// (it Nacks + returns the error, which surfaces here). It therefore covers BOTH
// handler errors AND the malformed-envelope errors CloudSubscriber returns — no
// coverage gap, no double-logging the same failure at two layers.
//
// slog.ErrorContext routes through the observability traceHandler, so the
// emitted record carries the W3C traceparent when the receive ctx has it.
func dispatchReceivedMessage(ctx context.Context, subscription string, cm CloudMessage, handler func(ctx context.Context, msg CloudMessage) error) {
	if err := handler(ctx, cm); err != nil {
		slog.ErrorContext(ctx, "pubsub: subscription handler error → Nack",
			"subscription", subscription,
			"topic", cm.Attributes["topic"],
			"message_id", cm.ID,
			"tenant_id", cm.Attributes["tenant_id"],
			"error", err,
		)
		cm.Nack()
	}
}

// getOrCachePublisher returns a cached Publisher for the topic, lazily
// creating one on first use. Uses double-checked locking so the hot path
// is read-only.
func (c *GCPClient) getOrCachePublisher(topic string) *gcppubsub.Publisher {
	c.mu.RLock()
	if p, ok := c.publishers[topic]; ok {
		c.mu.RUnlock()
		return p
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	// Defensive: if Close() was called concurrently OR the cache was lost
	// (e.g., post-shutdown publish attempt), re-init the map under the Lock
	// so the publish can complete. Stops a nil-map-assignment panic from
	// crashing the entire process during shutdown ordering races.
	if c.publishers == nil {
		c.publishers = make(map[string]*gcppubsub.Publisher)
	}
	if p, ok := c.publishers[topic]; ok {
		return p
	}
	p := c.client.Publisher(topic)
	// Explicit PublishSettings for predictable behaviour on Cloud Run /
	// constrained-CPU runtimes per POC W3 Finding 3 investigation. The
	// default v2 SDK settings use `NumGoroutines = multiple of GOMAXPROCS`
	// and `DelayThreshold = 10ms` for batching — observed to hang at the
	// 60s default Timeout when running inside Cloud Run with limited CPU
	// + concurrent HTTP-handler load (publish batch-flush goroutine
	// starves). Explicit minimal settings make the failure mode
	// predictable + fast.
	p.PublishSettings.NumGoroutines = 1
	p.PublishSettings.DelayThreshold = 5 * time.Millisecond
	p.PublishSettings.CountThreshold = 1
	p.PublishSettings.Timeout = 30 * time.Second
	c.publishers[topic] = p
	return p
}

// EnsureTopic creates the topic if it does not exist. Idempotent: a
// concurrent creator is tolerated via GetTopic-then-CreateTopic-on-NotFound.
// This is REQUIRED on the Pub/Sub emulator (which starts empty) and a no-op
// on production where topics are Terraform-provisioned — the NotFound path
// simply never fires. Mirrors the pattern in chora-support's PubSubSubscriber.
func (c *GCPClient) EnsureTopic(ctx context.Context, topic string) error {
	fullName := "projects/" + c.projectID + "/topics/" + topic
	_, err := c.client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: fullName})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("pubsub: EnsureTopic GetTopic %s: %w", topic, err)
	}
	if _, err := c.client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: fullName}); err != nil {
		return fmt.Errorf("pubsub: EnsureTopic CreateTopic %s: %w", topic, err)
	}
	slog.InfoContext(ctx, "pubsub: EnsureTopic created", "topic", topic)
	return nil
}

// EnsureSubscription is now in ensure.go (richer config-based signature).
// Compile-time check.
var _ CloudPubSubClient = (*GCPClient)(nil)

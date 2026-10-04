// Internal tests for the GCPClient error surfaces that are reachable WITHOUT
// a live Cloud Pub/Sub connection or ADC credentials (none exist on
// dev/CI machines — GOOGLE_APPLICATION_CREDENTIALS is unset, so the SDK
// constructor fails deterministically with "could not find default
// credentials"). The live publish/receive/admin paths need a real
// *pubsub.Client and stay uncovered (noted in the coverage report).
package pubsub

import (
	"context"
	"strings"
	"testing"

	gcppubsub "cloud.google.com/go/pubsub/v2"
)

// TestNewGCPClient_RequiresProject covers the projectID-required guard —
// it fires before any SDK call.
func TestNewGCPClient_RequiresProject(t *testing.T) {
	c, err := NewGCPClient(context.Background(), "")
	if err == nil {
		t.Fatal("expected projectID-required error")
	}
	if !strings.Contains(err.Error(), "projectID required") {
		t.Errorf("err=%v want 'projectID required'", err)
	}
	if c != nil {
		t.Errorf("client=%v want nil", c)
	}
}

// TestNewGCPClient_NoADCWrapsError covers the SDK-constructor failure path.
// Written defensively (mirrors the secrets package): on machines WITHOUT
// Application Default Credentials (this machine) the constructor fails and
// the error must be wrapped in "pubsub: NewGCPClient"; on machines WITH
// credentials the client is constructed and closed so the test stays green
// everywhere. Only the bare success return (gcp_client.go:60.2,64.8) is
// machine-conditional and remains uncovered here.
func TestNewGCPClient_NoADCWrapsError(t *testing.T) {
	c, err := NewGCPClient(context.Background(), "some-project")
	if err != nil {
		if !strings.Contains(err.Error(), "pubsub: NewGCPClient") {
			t.Errorf("err=%v; want wrapped in 'pubsub: NewGCPClient'", err)
		}
		return
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestGCPClient_PublishMessageRequiresTopic covers the topic-required guard
// of PublishMessage — it fires before the client is touched.
func TestGCPClient_PublishMessageRequiresTopic(t *testing.T) {
	c := &GCPClient{}
	_, err := c.PublishMessage(context.Background(), "", []byte("p"), nil)
	if err == nil {
		t.Fatal("expected topic-required error")
	}
	if !strings.Contains(err.Error(), "topic required") {
		t.Errorf("err=%v want 'topic required'", err)
	}
}

// TestGCPClient_SubscriptionReceiveRequiresSubscription covers the
// subscription-required guard of SubscriptionReceive — it fires before the
// client is touched.
func TestGCPClient_SubscriptionReceiveRequiresSubscription(t *testing.T) {
	c := &GCPClient{}
	err := c.SubscriptionReceive(context.Background(), "", func(context.Context, CloudMessage) error { return nil })
	if err == nil {
		t.Fatal("expected subscription-required error")
	}
	if !strings.Contains(err.Error(), "subscription required") {
		t.Errorf("err=%v want 'subscription required'", err)
	}
}

// TestGCPClient_GetOrCachePublisher_HitPath covers the cache-HIT branch of
// getOrCachePublisher: a cached publisher is returned without touching
// c.client (deliberately nil here — any client access would nil-panic). The
// cache-MISS create path needs a live *pubsub.Client and stays uncovered.
func TestGCPClient_GetOrCachePublisher_HitPath(t *testing.T) {
	wanted := &gcppubsub.Publisher{}
	c := &GCPClient{publishers: map[string]*gcppubsub.Publisher{"t": wanted}}

	if got := c.getOrCachePublisher("t"); got != wanted {
		t.Errorf("got publisher %v; want the cached %v", got, wanted)
	}
	if _, ok := c.publishers["t"]; !ok {
		t.Error("cache entry must be preserved on hit")
	}
}

// TestEnsureSubscription_RequiresSubscriptionAndTopic covers the input
// validation of EnsureSubscription. It fires against a zero-value
// &GCPClient{} before any admin-client call, so it is unit-testable without
// a live connection. The lookup/create paths below it need a real
// SubscriptionAdminClient and stay uncovered.
func TestEnsureSubscription_RequiresSubscriptionAndTopic(t *testing.T) {
	c := &GCPClient{}
	for _, cfg := range []EnsureSubscriptionConfig{
		{},
		{Subscription: "chora-tenancy-payments-x"},
		{Topic: "chora.payments.x.y.v1"},
	} {
		created, err := c.EnsureSubscription(context.Background(), cfg)
		if err == nil {
			t.Errorf("cfg=%+v: expected validation error", cfg)
		}
		if created {
			t.Errorf("cfg=%+v: created=true on validation failure", cfg)
		}
	}
}

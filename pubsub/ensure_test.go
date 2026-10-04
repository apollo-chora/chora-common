package pubsub

import (
	"testing"
	"time"
)

func TestBuildSubscriptionProto_FullConfig(t *testing.T) {
	got := buildSubscriptionProto("chora-489812", EnsureSubscriptionConfig{
		Subscription:        "chora-tenancy-payments-tenant_addon_purchase-payment_captured",
		Topic:               "chora.payments.tenant_addon_purchase.payment_captured.v1",
		DeadLetterTopic:     "chora.dlq.payments.tenant_addon_purchase.payment_captured.v1",
		AckDeadline:         60 * time.Second,
		MaxDeliveryAttempts: 5,
		MinBackoff:          10 * time.Second,
		MaxBackoff:          600 * time.Second,
		RetentionDuration:   7 * 24 * time.Hour,
		NeverExpire:         true,
	})

	wantName := "projects/chora-489812/subscriptions/chora-tenancy-payments-tenant_addon_purchase-payment_captured"
	if got.GetName() != wantName {
		t.Errorf("Name = %q; want %q", got.GetName(), wantName)
	}
	wantTopic := "projects/chora-489812/topics/chora.payments.tenant_addon_purchase.payment_captured.v1"
	if got.GetTopic() != wantTopic {
		t.Errorf("Topic = %q; want %q", got.GetTopic(), wantTopic)
	}
	if got.GetAckDeadlineSeconds() != 60 {
		t.Errorf("AckDeadlineSeconds = %d; want 60", got.GetAckDeadlineSeconds())
	}
	if d := got.GetMessageRetentionDuration().AsDuration(); d != 7*24*time.Hour {
		t.Errorf("MessageRetentionDuration = %s; want 168h", d)
	}
	dlp := got.GetDeadLetterPolicy()
	if dlp == nil {
		t.Fatal("DeadLetterPolicy = nil; want set")
	}
	wantDLQ := "projects/chora-489812/topics/chora.dlq.payments.tenant_addon_purchase.payment_captured.v1"
	if dlp.GetDeadLetterTopic() != wantDLQ {
		t.Errorf("DeadLetterTopic = %q; want %q", dlp.GetDeadLetterTopic(), wantDLQ)
	}
	if dlp.GetMaxDeliveryAttempts() != 5 {
		t.Errorf("MaxDeliveryAttempts = %d; want 5", dlp.GetMaxDeliveryAttempts())
	}
	rp := got.GetRetryPolicy()
	if rp == nil {
		t.Fatal("RetryPolicy = nil; want set")
	}
	if rp.GetMinimumBackoff().AsDuration() != 10*time.Second {
		t.Errorf("MinimumBackoff = %s; want 10s", rp.GetMinimumBackoff().AsDuration())
	}
	if rp.GetMaximumBackoff().AsDuration() != 600*time.Second {
		t.Errorf("MaximumBackoff = %s; want 600s", rp.GetMaximumBackoff().AsDuration())
	}
	if got.GetExpirationPolicy() == nil {
		t.Error("ExpirationPolicy = nil; want non-nil (never-expire = empty policy)")
	} else if got.GetExpirationPolicy().GetTtl() != nil {
		t.Error("ExpirationPolicy.Ttl != nil; want nil (never expire)")
	}
}

func TestBuildSubscriptionProto_NoDeadLetterNoRetryNoExpiry(t *testing.T) {
	got := buildSubscriptionProto("p", EnsureSubscriptionConfig{
		Subscription: "s",
		Topic:        "t",
		AckDeadline:  60 * time.Second,
	})
	if got.GetDeadLetterPolicy() != nil {
		t.Error("DeadLetterPolicy set; want nil when DeadLetterTopic empty")
	}
	if got.GetRetryPolicy() != nil {
		t.Error("RetryPolicy set; want nil when backoffs zero")
	}
	if got.GetExpirationPolicy() != nil {
		t.Error("ExpirationPolicy set; want nil when NeverExpire false")
	}
	if got.GetMessageRetentionDuration() != nil {
		t.Error("MessageRetentionDuration set; want nil when RetentionDuration zero")
	}
}

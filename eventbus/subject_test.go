package eventbus

import "testing"

// canonicalDomains is the closed event-domain vocabulary as published by
// chora-contracts (asyncapi/<domain>/) and used across the services. It is
// duplicated here deliberately: if a new domain is added to the platform and
// this validator is not updated, Subscribe/Publish fails at RUNTIME with
// `unknown domain`, which is exactly how `payments` was silently rejected.
// This test turns that drift into a build failure.
var canonicalDomains = []string{
	// 5 core content domains
	"creation", "consumption", "sharing", "delivery", "a2a",
	// 7 supporting/platform domains
	"identity", "tenancy", "governance", "observability", "notifications", "ai_kernel", "payments",
	// cross-cutting saga namespace
	"closure",
}

func TestKnownDomainsMatchCanonicalSet(t *testing.T) {
	if len(knownDomains) != len(canonicalDomains) {
		t.Fatalf("knownDomains has %d entries, canonical set has %d — update knownDomains in subject.go", len(knownDomains), len(canonicalDomains))
	}
	for _, d := range canonicalDomains {
		if _, ok := knownDomains[d]; !ok {
			t.Errorf("domain %q is missing from knownDomains", d)
		}
	}
	for d := range knownDomains {
		found := false
		for _, c := range canonicalDomains {
			if d == c {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("knownDomains contains %q which is not in the canonical set", d)
		}
	}
}

func TestValidateSubjectAcceptsEveryCanonicalDomain(t *testing.T) {
	for _, d := range canonicalDomains {
		subject := "chora." + d + ".thing.happened.v1"
		if err := ValidateSubject(subject); err != nil {
			t.Errorf("ValidateSubject(%q) = %v, want nil", subject, err)
		}
	}
}

func TestValidateSubjectRejectsUnknownDomain(t *testing.T) {
	if err := ValidateSubject("chora.billing.thing.happened.v1"); err == nil {
		t.Error("expected an unknown domain to be rejected")
	}
}

package eventbus

import (
	"fmt"
	"regexp"
	"strings"
)

// ValidateSubject enforces the chora.{domain}.{aggregate}.{event_type}.v{N}
// taxonomy. The same strings are valid NATS subjects, so the event taxonomy is
// unchanged from the Pub/Sub era. Returns nil on success.
//
// The 11 known domains are the closed vocabulary; aggregate + event_type are
// validated as snake_case. Major version v{N} >= 1 (no leading zero).
func ValidateSubject(name string) error {
	if name == "" {
		return fmt.Errorf("subject name empty")
	}
	parts := strings.Split(name, ".")
	if len(parts) < 5 {
		return fmt.Errorf("subject %q: expected chora.{domain}.{aggregate}.{event_type}.v{N}", name)
	}
	if parts[0] != "chora" {
		return fmt.Errorf("subject %q: must start with chora", name)
	}
	domain := parts[1]
	if _, ok := knownDomains[domain]; !ok {
		return fmt.Errorf("subject %q: unknown domain %q", name, domain)
	}

	// Last part is v{N}.
	versionPart := parts[len(parts)-1]
	if !versionRe.MatchString(versionPart) {
		return fmt.Errorf("subject %q: version segment %q must match v[1-9][0-9]*", name, versionPart)
	}

	// Parts 2..(len-2) are aggregate + event_type segments — each MUST be
	// snake_case (lowercase, digits, underscores).
	for _, seg := range parts[2 : len(parts)-1] {
		if !snakeRe.MatchString(seg) {
			return fmt.Errorf("subject %q: segment %q must be snake_case (lowercase + digits + underscore)", name, seg)
		}
	}
	return nil
}

var (
	versionRe = regexp.MustCompile(`^v[1-9][0-9]*$`)
	snakeRe   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// knownDomains is the closed 11-domain vocabulary (5 core Content + 6
// supporting/platform), plus the cross-cutting `closure` saga namespace.
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

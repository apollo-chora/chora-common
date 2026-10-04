// Package pubsubinbox is the CANONICAL registry mapping every
// `/api/internal/pubsub/{inbox}` name to the service that owns it.
//
// # Why this package exists
//
// chora-gateway fronts every Pub/Sub push subscription. Pub/Sub posts to
// `https://api.chora.site/api/internal/pubsub/{inbox}`; the gateway picks a
// downstream base URL by inbox name and forwards the request VERBATIM (it does
// not rewrite the path), so the downstream must mount the identical path.
//
// That made the routing table a HAND-MAINTAINED DUPLICATE: the gateway kept its
// own allowlist literals, and each service separately kept its own `mux.Handle`
// calls. Two copies of one fact, with nothing forcing them to agree. They
// drifted, silently, and on 2026-07-15 the drift was found to be TOTAL for
// chora-sharing:
//
//   - the gateway routed `weakness-grown` + `live-quiz-scores` to chora-sharing,
//     which mounted NEITHER (the greenfield rewrite 470ec0ef9 deleted the push
//     wiring) ⇒ 404 → 5 Pub/Sub retries → dead-letter;
//   - chora-sharing mounted `course-published`, which the gateway did NOT route
//     ⇒ unreachable.
//
// Perfectly inverted: every inbox broken, in both directions, for over two
// weeks. Nothing failed. `NewWeaknessGrownPushHandler` existed and its unit test
// was GREEN — because the test called the handler directly, bypassing the mux. A
// constructed-but-unmounted handler is indistinguishable from no handler.
//
// # The invariant
//
// One descriptor, derived twice — never two hand-written copies:
//
//   - chora-gateway builds its routing allowlists FROM this registry
//     (see cmd/server/main.go), so the gateway cannot route an inbox nobody owns.
//   - each service asserts, THROUGH ITS OWN MUX, that it mounts exactly the
//     inboxes InboxesFor() assigns it (see each service's
//     pubsub_inbox_contract_test.go), so a service cannot fail to mount an inbox
//     the gateway sends it.
//
// Adding an inbox therefore means adding it HERE first; the owning service's
// contract test then goes RED until the route is actually mounted. That is the
// point.
//
// An inbox absent from this registry is UNOWNED: the gateway refuses it with a
// 404 and a loud log, rather than silently forwarding it to chora-delivery to
// 404 there. Fail loud, never silently fall back.
package pubsubinbox

import "sort"

// PathPrefix is the canonical route prefix. Pub/Sub push endpoints are
// `https://api.chora.site` + PathPrefix + inbox, and the gateway forwards the
// full path unchanged, so downstreams mount exactly Path(inbox).
const PathPrefix = "/api/internal/pubsub/"

// Service is the owning Chora service for an inbox. The value is the k8s
// Deployment / Artifact Registry name, so it reads correctly in logs.
type Service string

const (
	Delivery    Service = "chora-delivery"
	Consumption Service = "chora-consumption"
	Sharing     Service = "chora-sharing"
	Identity    Service = "chora-identity"
)

// Inbox names referenced from service code. Declared as constants so the map
// key below and the `mux.Handle` call in the owning service are literally the
// same symbol — a typo in either becomes a compile error rather than a route
// that silently 404s into a dead-letter topic.
const (
	InboxWeaknessGrown   = "weakness-grown"
	InboxLiveQuizScores  = "live-quiz-scores"
	InboxCoursePublished = "course-published"
	// ASYNC-mode analytics progress projection (chora-delivery, CHO-1827).
	InboxCourseProgressAdvanced  = "course-progress-advanced-inbox"
	InboxCourseProgressCompleted = "course-progress-completed-inbox"
)

// registry is the single source of truth. Every entry is verified against BOTH
// the owning service's `mux.Handle` call AND the live Pub/Sub push
// subscription's endpoint (`gcloud pubsub subscriptions list`, 2026-07-15).
//
// Two entries have no subscription yet and are listed deliberately, so the
// route exists BEFORE the subscription is created — a subscription created
// ahead of its route 404s every message straight into the dead-letter topic,
// which is strictly worse than having no subscription at all:
//
//   - course-published      (CHO-2194 — sub created once the route is live)
//   - weakness-review-pending (mounted in chora-consumption, no publisher yet)
//   - course-progress-advanced-inbox  (CHO-1827: the two learning_path topics
//   - course-progress-completed-inbox  already exist and are PUBLISHED, but had
//     zero subscriptions as of 2026-07-14; the subs are created once these routes
//     are live, per the rule above)
var registry = map[string]Service{
	// ---- chora-delivery (10) ----
	"payments-inbox":               Delivery, // chora.payments.{application_payment,course_purchase}.{payment_captured,refunded}.v1
	"grading-inbox":                Delivery, // chora.delivery.grading.submission_completed.v1
	"batch-testset-inbox":          Delivery, // chora.creation.question_batch.accepted.v1
	"completion-released-inbox":    Delivery, // chora.delivery.submission.released.v1
	"exam-result-released-inbox":   Delivery, // chora.delivery.exam_result.released.v1 (EXAM-mode auto-cert, R+ Four-Mode DoD section 10.4)
	"identity-profile-inbox":       Delivery, // chora.identity.user.profile_updated.v1 — NB owned by DELIVERY despite the name
	"module-progress-atom-inbox":   Delivery, // chora.consumption.atom_session.completed.v1
	"module-progress-graded-inbox": Delivery, // chora.delivery.submission.graded.v1
	// ASYNC-mode analytics (R+ Four-Mode DoD §10.3, CHO-1827). Two single-topic
	// routes, not one topic-dispatching route: chora-consumption's outbox does not
	// always stamp the `topic` attribute, so the ROUTE is the discriminator.
	InboxCourseProgressAdvanced:  Delivery, // chora.consumption.learning_path.advanced.v1
	InboxCourseProgressCompleted: Delivery, // chora.consumption.learning_path.completed.v1

	// ---- chora-identity (1) ----
	"identity-payments-inbox": Identity, // chora.payments.user_mana_topup.payment_captured.v1 → user_mana credit

	// ---- chora-sharing (3) ----
	InboxWeaknessGrown:   Sharing, // chora.consumption.weakness.grown.v1 → leaderboard XP (ADR-196 B3)
	InboxLiveQuizScores:  Sharing, // chora.delivery.live_quiz_session.score_awarded.v1 → leaderboard.Ranker (ADR-168 #8)
	InboxCoursePublished: Sharing, // chora.delivery.course.published.v1 → C+ discovery feed (CHO-2155/2194)

	// ---- chora-consumption (30) ----
	"enrollment-created":                 Consumption, // → LearningPath bootstrap
	"collection-converted-to-study-list": Consumption, // ADR-233 / spec-001 US5
	"learning-path-topics":               Consumption, // learning_path.{bootstrapped,advanced}.v1 → active_path_topics (CHO-2167)
	"atom-created":                       Consumption, // → atom_index projection
	"atom-published":                     Consumption, // → atom_index playability flip (CHO-1968) + ADR-244 D5 refresh fan-out
	"atom-updated":                       Consumption, // atom.updated.v1 → ADR-244 D5 refresh trigger (proposals only)
	"kg-retention":                       Consumption, // atom.{published,updated}.v1 → ADR-143 fog-cache invalidation (delivery fixed 2026-08-17; was mounted un-prefixed + fed by nothing)
	"course-content-composed":            Consumption, // → course-content projection (CHO-1612)
	"course-metadata":                    Consumption, // → course_directory projection (CHO-2059)
	"egg-purchase-provision":             Consumption, // → ProvisionEggSubscriber (CHO-1624)
	"companion-growth-source-events":     Consumption, // 5 EXP source topics → CompanionGrowthSubscriber (renamed in the W4 consumption cut, ADR-254 D9)
	"weakness-analyzed":                  Consumption, // → LearnerWeakness upsert (Epic-1b W8)
	"concept-suggested":                  Consumption, // ADR-212 WS-4
	"concept-deleted":                    Consumption, // CHO-2324 edge-cleanup cascade
	"goal-knowledge-synthesized":         Consumption, // → reflection cache row (CHO-2118)
	"goal-knowledge-invalidation":        Consumption, // 6 topics → goal-knowledge cache invalidation (CHO-2118)
	"weakness-grown-goals":               Consumption, // → GoalGraduationSubscriber (CHO-1962)
	"weakness-grown-exp":                 Consumption, // → Wave-1 familiar XP (CHO-2090, ADR-228 D4)
	"weakness-review-pending":            Consumption, // mounted; no publisher yet
	"module-completed-exp":               Consumption, // → Wave-1 familiar XP module leg (CHO-2124)
	"proofing-test-terminal":             Consumption, // ai_assist.{completed,refused}.v1 (CHO-2040 R8-6)
	"campaign-node-won":                  Consumption, // → free-on-win fog reveal (CHO-2083, ADR-227 D2)
	"campaign-growth-events":             Consumption, // campaign.{rung_cleared,node_won,goal_sealed}.v1 (CHO-2084, ADR-227 D10)
	"live-quiz-score-awarded":            Consumption, // derived Growth-Edge projector — distinct from sharing's live-quiz-scores
	"submission-graded-derived":          Consumption, // WS-6 second-evidence derived Growth Edge (ADR-205 / CHO-1958)
	// LearnerProfile projection spine (ADR-200)
	"learner-profile-cert-issued":          Consumption,
	"learner-profile-path-completed":       Consumption,
	"learner-profile-enrollment-created":   Consumption,
	"learner-profile-submission-graded":    Consumption,
	"learner-profile-enrollment-completed": Consumption,
	"learner-profile-preferences-updated":  Consumption, // WS1 preferences leg (ADR-200 / CHO-2049)
}

// Owner returns the service that owns inbox, and whether any service does.
//
// The second return is the fail-loud seam: a false MUST NOT be treated as
// "route it to chora-delivery and hope". An unowned inbox is a configuration
// error, and the caller is required to refuse it visibly.
func Owner(inbox string) (Service, bool) {
	svc, ok := registry[inbox]
	return svc, ok
}

// InboxesFor returns the inbox names owned by svc, sorted for deterministic
// logs and test output. Each service's contract test asserts it mounts exactly
// these.
func InboxesFor(svc Service) []string {
	out := make([]string, 0, 8)
	for inbox, owner := range registry {
		if owner == svc {
			out = append(out, inbox)
		}
	}
	sort.Strings(out)
	return out
}

// Path returns the full mount path for inbox — the path Pub/Sub posts to and
// the gateway forwards unchanged.
func Path(inbox string) string { return PathPrefix + inbox }

// All returns a copy of the registry. For tests and for the gateway's boot log.
func All() map[string]Service {
	out := make(map[string]Service, len(registry))
	for k, v := range registry {
		out[k] = v
	}
	return out
}

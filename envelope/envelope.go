// Package envelope builds + validates the mandatory Pub/Sub event envelope
// per CLAUDE.md §6:
//
//	Event envelope mandatory fields: event_id (UUIDv7), idempotency_key,
//	tenant_id, gcid, occurred_at, published_at, traceparent, tracestate,
//	source_project, source_service, schema_version
//
// And the Protobuf definition at chora-contracts/proto/common/envelope.proto.
//
// This is a Go-native struct mirror of the Protobuf message — it is the
// developer-friendly type that domain code constructs. The transport layer
// (chora-contracts gen/go) will marshal it to the on-wire Protobuf.
package envelope

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/apollo-chora/chora-common/tracing"
)

// Envelope mirrors chora-contracts/proto/common/envelope.proto/EventEnvelope.
// All fields except optional correlation/causation/imda are mandatory.
type Envelope struct {
	EventID        string
	IdempotencyKey string
	TenantID       string
	GCID           string
	OccurredAt     time.Time
	PublishedAt    time.Time
	Traceparent    string
	Tracestate     string
	SourceProject  string
	SourceService  string
	SchemaVersion  int32
	CorrelationID  string
	CausationID    string

	// ChoraImdaDimension is the optional IMDA Model AI Governance Framework v2
	// canonical label per ADR-141 (chora-contracts/proto/common/envelope.proto
	// field 14). One of:
	//   - "accountability"
	//   - "transparency"
	//   - "safety_and_robustness"
	//   - "fairness_and_human_oversight"
	// Empty when the event is not IMDA evidence.
	// Build() canonicalises deprecated v1 aliases (risk_levels →
	// accountability, etc.) so consumers only see canonical values on the
	// wire.
	ChoraImdaDimension string

	// ImdaLifecycleStage is the optional 4-stage IMDA pipeline classifier
	// (chora-contracts/proto/common/envelope.proto field 15). One of:
	//   - "ci_pre_merge"
	//   - "pre_deploy"
	//   - "runtime"
	//   - "post_deploy"
	// Empty when not applicable.
	ImdaLifecycleStage string
}

// BuildOpts captures the call-site values the helper cannot infer from ctx.
// All fields except IdempotencyKey are required for a valid envelope.
type BuildOpts struct {
	// EventType is currently informational on the envelope (the topic name
	// captures the semantic event_type). It is accepted for symmetry with the
	// publishing call site so callers can pass it through cleanly.
	EventType string

	// SchemaVersion is the major version of the event payload (matches v{N}
	// in the topic name). Must be ≥ 1.
	SchemaVersion int32

	// SourceProject is the project the publisher runs in
	// (e.g. chora-content, chora-delivery, chora-489812, chora-golden).
	SourceProject string

	// SourceService is the publishing service name (e.g. chora-creation).
	SourceService string

	// IdempotencyKey defaults to EventID when blank. Override for replay-safe
	// idempotency (e.g. aggregate_id+version for atom revisions).
	IdempotencyKey string

	// CorrelationID and CausationID are optional saga/causation linkage.
	CorrelationID string
	CausationID   string

	// ChoraImdaDimension declares which IMDA Model AI Governance Framework
	// dimension this event is evidence for (per ADR-141 + envelope.proto
	// field 14). Build() will canonicalise v1 deprecated aliases.
	// Empty when the event is not IMDA-tagged.
	ChoraImdaDimension string

	// ImdaLifecycleStage classifies which point in the AI development
	// pipeline emitted the event (per envelope.proto field 15).
	// Empty when not applicable.
	ImdaLifecycleStage string

	// Now is injectable for tests; defaults to time.Now().UTC().
	Now func() time.Time
}

// Build constructs a fully-populated Envelope from the BuildOpts and the
// values stamped on ctx (tenant_id, gcid, traceparent). Missing tenant_id
// defaults to "platform" (system events). Missing traceparent is freshly
// minted to satisfy the OTLP-everywhere mandate.
func Build(ctx context.Context, opts BuildOpts) Envelope {
	now := time.Now().UTC
	if opts.Now != nil {
		now = opts.Now
	}

	tenantID := tracing.TenantIDFromContext(ctx)
	if tenantID == "" {
		tenantID = "platform"
	}

	tp := tracing.TraceparentFromContext(ctx)
	if tp == "" {
		tp = tracing.EnsureTraceparent("")
	}

	eventID := newUUIDv7()
	idemKey := opts.IdempotencyKey
	if idemKey == "" {
		idemKey = eventID
	}

	t := now()
	return Envelope{
		EventID:            eventID,
		IdempotencyKey:     idemKey,
		TenantID:           tenantID,
		GCID:               tracing.GCIDFromContext(ctx),
		OccurredAt:         t,
		PublishedAt:        t,
		Traceparent:        tp,
		Tracestate:         "", // optional — propagate when set by upstream
		SourceProject:      opts.SourceProject,
		SourceService:      opts.SourceService,
		SchemaVersion:      opts.SchemaVersion,
		CorrelationID:      opts.CorrelationID,
		CausationID:        opts.CausationID,
		ChoraImdaDimension: CanonicaliseImdaDimension(opts.ChoraImdaDimension),
		ImdaLifecycleStage: normaliseLifecycleStage(opts.ImdaLifecycleStage),
	}
}

// Validate (loose) checks all mandatory envelope fields are present per
// CLAUDE.md §6. Optional IMDA fields use closed-vocabulary validation only
// for imda_lifecycle_stage; chora_imda_dimension is loose to permit forward-
// compat unknowns. Use ValidateStrict in CI to enforce closed-vocabulary on
// chora_imda_dimension as well.
//
// Returns nil on success; a descriptive error otherwise.
func Validate(e Envelope) error {
	if err := validateMandatory(e); err != nil {
		return err
	}
	if err := validateLifecycleStage(e.ImdaLifecycleStage); err != nil {
		return err
	}
	// gcid intentionally NOT validated — system-emitted events have empty
	// gcid (per envelope.proto comment).
	return nil
}

// ValidateStrict is like Validate but additionally rejects
// chora_imda_dimension values outside the canonical v2 vocabulary
// (accountability / transparency / safety_and_robustness /
// fairness_and_human_oversight). Recommended for CI gating; loose Validate
// is recommended at runtime to allow forward-compat label rollout.
func ValidateStrict(e Envelope) error {
	if err := Validate(e); err != nil {
		return err
	}
	if e.ChoraImdaDimension != "" && !isCanonicalImdaDimension(e.ChoraImdaDimension) {
		return fmt.Errorf("envelope: chora_imda_dimension %q is not a canonical IMDA label (per ADR-141)", e.ChoraImdaDimension)
	}
	return nil
}

func validateMandatory(e Envelope) error {
	if e.EventID == "" {
		return fmt.Errorf("envelope: event_id is required")
	}
	if e.IdempotencyKey == "" {
		return fmt.Errorf("envelope: idempotency_key is required")
	}
	if e.TenantID == "" {
		return fmt.Errorf("envelope: tenant_id is required")
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("envelope: occurred_at is required")
	}
	if e.PublishedAt.IsZero() {
		return fmt.Errorf("envelope: published_at is required")
	}
	if e.Traceparent == "" {
		return fmt.Errorf("envelope: traceparent is required")
	}
	if e.SourceProject == "" {
		return fmt.Errorf("envelope: source_project is required")
	}
	if e.SourceService == "" {
		return fmt.Errorf("envelope: source_service is required")
	}
	if e.SchemaVersion < 1 {
		return fmt.Errorf("envelope: schema_version must be ≥ 1")
	}
	return nil
}

// IMDA label canonicalisation + validation -----------------------------------

// canonicalIMDADimensions is the closed v2 vocabulary per ADR-141.
var canonicalIMDADimensions = map[string]struct{}{
	"accountability":               {},
	"transparency":                 {},
	"safety_and_robustness":        {},
	"fairness_and_human_oversight": {},
}

// imdaV1ToV2 maps deprecated v1 IMDA labels onto canonical v2 labels per
// ADR-141. Producers may still emit v1 spellings during the migration; the
// envelope builder normalises them so subscribers only ever see canonical.
var imdaV1ToV2 = map[string]string{
	"risk_levels":             "accountability",
	"stakeholder_interaction": "transparency",
	"internal_governance":     "safety_and_robustness",
	"operations_management":   "fairness_and_human_oversight",
}

// canonicalLifecycleStages is the closed vocabulary for envelope field 15.
var canonicalLifecycleStages = map[string]struct{}{
	"ci_pre_merge": {},
	"pre_deploy":   {},
	"runtime":      {},
	"post_deploy":  {},
}

// CanonicaliseImdaDimension lowercases + trims the input and maps deprecated
// v1 aliases onto canonical v2 labels per ADR-141. Unknown values are
// returned in their normalised (lower+trim) form for forward compatibility;
// use ValidateStrict to reject unknowns. Empty input returns empty.
func CanonicaliseImdaDimension(in string) string {
	v := strings.ToLower(strings.TrimSpace(in))
	if v == "" {
		return ""
	}
	if mapped, ok := imdaV1ToV2[v]; ok {
		return mapped
	}
	return v
}

func isCanonicalImdaDimension(v string) bool {
	_, ok := canonicalIMDADimensions[v]
	return ok
}

func normaliseLifecycleStage(in string) string {
	return strings.ToLower(strings.TrimSpace(in))
}

func validateLifecycleStage(v string) error {
	if v == "" {
		return nil // optional
	}
	if _, ok := canonicalLifecycleStages[v]; !ok {
		return fmt.Errorf("envelope: imda_lifecycle_stage %q must be one of ci_pre_merge|pre_deploy|runtime|post_deploy", v)
	}
	return nil
}

// newUUIDv7 returns a UUIDv7 string. It panics only when the entropy source
// fails, which is exactly where the v4 constructors panic too.
func newUUIDv7() string {
	return uuid.Must(uuid.NewV7()).String()
}

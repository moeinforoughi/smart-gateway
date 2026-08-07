package domain

import (
	"context"
	"time"
)

// SwitchOutcome describes why a selector evaluation ended (architecture §6):
//
//	Current → Candidate? → Better? (hysteresis) → Cooldown? → Switch → Publish
type SwitchOutcome string

const (
	// SwitchNone means no eligible candidate exists yet.
	SwitchNone SwitchOutcome = "none"
	// SwitchNotBetter means a candidate exists but fails the hysteresis gate.
	SwitchNotBetter SwitchOutcome = "not_better"
	// SwitchCooldown means the gate passed but the dwell-time cooldown blocks.
	SwitchCooldown SwitchOutcome = "cooldown"
	// SwitchConfirmed means the decision was applied and published.
	SwitchConfirmed SwitchOutcome = "confirmed"
	// SwitchFailed means the apply step errored and no change happened.
	SwitchFailed SwitchOutcome = "failed"
)

// SwitchDecision is the explainable output of one selector evaluation
// (FR-705/706, FR-804): the candidate, the comparison and the outcome.
type SwitchDecision struct {
	At             time.Time
	CandidateID    string
	CurrentID      string
	CandidateScore float64
	CurrentScore   float64
	Delta          float64 // candidate - current
	DeltaRequired  float64 // configured switch_delta
	InCooldown     bool
	Reason         string
	Outcome        SwitchOutcome
}

// SwitchRecord is the persisted history entry for a confirmed switch.
type SwitchRecord struct {
	ID        string // UUIDv7
	At        time.Time
	FromID    string
	ToID      string
	Reason    string
	PrevScore float64
	NewScore  float64
}

// OutboundApplier is the data-plane port for selector control. It is
// implemented by the sing-box adapter (infrastructure); routing depends only
// on this port (architecture §3, invariant "proxy core never contains
// selection logic").
type OutboundApplier interface {
	// ApplySelection switches the smart selector to nodeID with no-drop
	// semantics (interrupt_exist_connections: false).
	ApplySelection(ctx context.Context, nodeID string) error
	// Current returns the tag currently selected by the smart selector.
	Current(ctx context.Context) (string, error)
}

// SwitchLogStore persists confirmed switch decisions (FR-705/706).
type SwitchLogStore interface {
	RecordSwitch(ctx context.Context, rec SwitchRecord) error
}

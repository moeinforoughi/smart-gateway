package domain

import "time"

// ScoreState models the lifecycle of a node score (architecture §6):
//
//	UNKNOWN → CALCULATED → ELIGIBLE → ACTIVE → REPLACED → ARCHIVED
type ScoreState string

const (
	ScoreUnknown    ScoreState = "unknown"
	ScoreCalculated ScoreState = "calculated"
	ScoreEligible   ScoreState = "eligible"
	ScoreActive     ScoreState = "active"
	ScoreReplaced   ScoreState = "replaced"
	ScoreArchived   ScoreState = "archived"
)

// Valid reports whether s is a known score state.
func (s ScoreState) Valid() bool {
	switch s {
	case ScoreUnknown, ScoreCalculated, ScoreEligible, ScoreActive, ScoreReplaced, ScoreArchived:
		return true
	}
	return false
}

// scoreTransitions is the allowed score state machine (architecture §6).
var scoreTransitions = map[ScoreState]map[ScoreState]bool{
	ScoreUnknown:    {ScoreCalculated: true},
	ScoreCalculated: {ScoreEligible: true, ScoreArchived: true},
	ScoreEligible:   {ScoreActive: true, ScoreReplaced: true, ScoreArchived: true},
	ScoreActive:     {ScoreReplaced: true, ScoreEligible: true, ScoreArchived: true},
	ScoreReplaced:   {ScoreArchived: true},
	ScoreArchived:   {ScoreCalculated: true}, // node re-enters the fleet
}

// CanTransitionScore reports whether a score state change is allowed.
func CanTransitionScore(from, to ScoreState) bool {
	if !from.Valid() || !to.Valid() {
		return false
	}
	return scoreTransitions[from][to]
}

// NodeScore is the persisted score record for a node (FR-607, FR-608).
// The score history keeps every NodeScore for auditability.
type NodeScore struct {
	NodeID      string
	Score       Score
	State       ScoreState
	EvaluatedAt time.Time
	// Reason explains the score/state change (e.g. first eligible sample,
	// confidence gate passed).
	Reason string
}

// Eligible reports the gated eligibility of the persisted score: the scorer's
// gates passed and the state is selectable.
func (n NodeScore) Eligible() bool {
	return n.Score.Eligible && (n.State == ScoreEligible || n.State == ScoreActive)
}

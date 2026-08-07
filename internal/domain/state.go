package domain

// healthTransitions is the allowed health state machine (architecture §6):
//
//	UNKNOWN → HEALTHY → DEGRADED → UNSTABLE → FAILED → RECOVERING → HEALTHY
//
// Additional allowed moves (documented exceptions):
//   - any state may go to FAILED on a confirmed hard failure;
//   - HEALTHY may regress to DEGRADED/UNSTABLE when metrics worsen;
//   - RECOVERING regresses to FAILED on a failed recovery probe.
var healthTransitions = map[NodeState]map[NodeState]bool{
	StateUnknown:    {StateHealthy: true, StateDegraded: true, StateUnstable: true, StateFailed: true},
	StateHealthy:    {StateDegraded: true, StateUnstable: true, StateFailed: true},
	StateDegraded:   {StateUnstable: true, StateHealthy: true, StateFailed: true},
	StateUnstable:   {StateFailed: true, StateDegraded: true, StateHealthy: true},
	StateFailed:     {StateRecovering: true},
	StateRecovering: {StateHealthy: true, StateFailed: true},
}

// CanTransitionHealth reports whether a health state change is allowed by
// the machine. Zero-value states are treated as unknown.
func CanTransitionHealth(from, to NodeState) bool {
	if from == "" {
		from = StateUnknown
	}
	if to == "" {
		to = StateUnknown
	}
	if !from.Valid() || !to.Valid() {
		return false
	}
	return healthTransitions[from][to]
}

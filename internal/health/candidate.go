package health

import (
	"sort"

	"smart-gateway/internal/domain"
)

// PickCandidates builds the rotating deep-probe candidate pool: the active
// node always participates, never-measured ("new") nodes get a share so they
// enter the fleet, and the remainder is the top-N healthy set by EWMA latency
// (ties broken by loss then node ID). Deterministic for a given input.
func PickCandidates(nodes []domain.Node, health func(id string) domain.NodeHealth,
	activeID string, size int) []domain.Node {
	if size <= 0 {
		return nil
	}
	sel := make(map[string]domain.Node)
	var order []string
	var newNodes []string
	for i := range nodes {
		n := nodes[i]
		if n.ID == activeID {
			if _, ok := sel[n.ID]; !ok {
				sel[n.ID] = n
				order = append(order, n.ID)
			}
			continue
		}
		h := health(n.ID)
		if h.Probes == 0 {
			newNodes = append(newNodes, n.ID)
			continue
		}
		sel[n.ID] = n
		order = append(order, n.ID)
	}
	// New nodes first (discovery), then healthy by EWMA asc.
	sort.Strings(newNodes)
	healthy := make([]domain.Node, 0, len(order))
	for _, id := range order {
		healthy = append(healthy, sel[id])
	}
	sort.SliceStable(healthy, func(i, j int) bool {
		hi := health(healthy[i].ID)
		hj := health(healthy[j].ID)
		li, lj := hi.LossRate < 1, hj.LossRate < 1
		if li != lj {
			return li
		}
		if hi.EWMA != hj.EWMA {
			return hi.EWMA < hj.EWMA
		}
		return healthy[i].ID < healthy[j].ID
	})

	out := make([]domain.Node, 0, size)
	seen := make(map[string]bool, size)
	add := func(n domain.Node) {
		if len(out) >= size || seen[n.ID] {
			return
		}
		seen[n.ID] = true
		out = append(out, n)
	}
	// Active first.
	for _, n := range nodes {
		if n.ID == activeID {
			add(n)
			break
		}
	}
	for _, id := range newNodes {
		for i := range nodes {
			if nodes[i].ID == id {
				add(nodes[i])
				break
			}
		}
	}
	for _, n := range healthy {
		add(n)
	}
	return out
}

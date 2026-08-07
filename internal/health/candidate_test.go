package health

import (
	"testing"
	"time"

	"smart-gateway/internal/domain"
)

func mkNode(id string) domain.Node {
	return domain.Node{ID: id, Protocol: domain.ProtocolVless}
}

func hof(hm map[string]domain.NodeHealth) func(string) domain.NodeHealth {
	return func(id string) domain.NodeHealth {
		h, ok := hm[id]
		if !ok {
			return domain.NodeHealth{NodeID: id}
		}
		return h
	}
}

func healthWithProbe(id string, probes int, ewma time.Duration, loss float64) domain.NodeHealth {
	return domain.NodeHealth{NodeID: id, Probes: probes, EWMA: ewma, LossRate: loss}
}

func TestPickCandidatesActiveFirst(t *testing.T) {
	nodes := []domain.Node{mkNode("a"), mkNode("b"), mkNode("c")}
	pick := PickCandidates(nodes, hof(map[string]domain.NodeHealth{
		"a": healthWithProbe("a", 1, 90*time.Millisecond, 0),
		"b": healthWithProbe("b", 1, 80*time.Millisecond, 0),
		"c": healthWithProbe("c", 1, 70*time.Millisecond, 0),
	}), "b", 3)
	if len(pick) != 3 {
		t.Fatalf("len = %d, want 3", len(pick))
	}
	if pick[0].ID != "b" {
		t.Fatalf("active node must be first, got %s", pick[0].ID)
	}
}

func TestPickCandidatesDeadNodesRankLast(t *testing.T) {
	nodes := []domain.Node{mkNode("a"), mkNode("b")}
	// a is fast but has lost every probe; b is slow but alive. Alive nodes must
	// rank before dead ones regardless of latency.
	pick := PickCandidates(nodes, hof(map[string]domain.NodeHealth{
		"a": healthWithProbe("a", 3, 10*time.Millisecond, 1.0),
		"b": healthWithProbe("b", 3, 200*time.Millisecond, 0.0),
	}), "", 2)
	if len(pick) != 2 {
		t.Fatalf("len = %d, want 2", len(pick))
	}
	if pick[0].ID != "b" {
		t.Fatalf("alive node should rank first, got %s", pick[0].ID)
	}
	if pick[1].ID != "a" {
		t.Fatalf("dead node should rank last, got %s", pick[1].ID)
	}
}

func TestPickCandidatesEWMABreaksTies(t *testing.T) {
	nodes := []domain.Node{mkNode("a"), mkNode("b")}
	// Both alive: faster EWMA ranks first.
	pick := PickCandidates(nodes, hof(map[string]domain.NodeHealth{
		"a": healthWithProbe("a", 3, 10*time.Millisecond, 0.0),
		"b": healthWithProbe("b", 3, 200*time.Millisecond, 0.0),
	}), "", 2)
	if pick[0].ID != "a" {
		t.Fatalf("fastest EWMA should rank first, got %s", pick[0].ID)
	}
}

func TestPickCandidatesNewNodesIncluded(t *testing.T) {
	nodes := []domain.Node{mkNode("new1"), mkNode("old1")}
	// new1 has no probes -> discovery bucket; old1 has probes.
	pick := PickCandidates(nodes, hof(map[string]domain.NodeHealth{
		"old1": healthWithProbe("old1", 5, 10*time.Millisecond, 0),
	}), "", 2)
	got := map[string]bool{}
	for _, n := range pick {
		got[n.ID] = true
	}
	if !got["new1"] || !got["old1"] {
		t.Fatalf("both new and old nodes must be candidates, got %v", got)
	}
}

func TestPickCandidatesSizeLimit(t *testing.T) {
	nodes := []domain.Node{mkNode("a"), mkNode("b"), mkNode("c"), mkNode("d")}
	pick := PickCandidates(nodes, func(string) domain.NodeHealth {
		return healthWithProbe("x", 1, 50*time.Millisecond, 0)
	}, "", 2)
	if len(pick) != 2 {
		t.Fatalf("len = %d, want 2 (size cap)", len(pick))
	}
}

func TestPickCandidatesDeterministic(t *testing.T) {
	nodes := []domain.Node{mkNode("a"), mkNode("b"), mkNode("c"), mkNode("d")}
	hm := map[string]domain.NodeHealth{
		"a": healthWithProbe("a", 1, 90*time.Millisecond, 0),
		"b": healthWithProbe("b", 1, 90*time.Millisecond, 0), // same EWMA -> ID tiebreak
		"c": healthWithProbe("c", 1, 70*time.Millisecond, 0),
		"d": healthWithProbe("d", 1, 80*time.Millisecond, 0),
	}
	first := PickCandidates(nodes, hof(hm), "", 4)
	second := PickCandidates(nodes, hof(hm), "", 4)
	if len(first) != len(second) {
		t.Fatalf("lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].ID != second[i].ID {
			t.Fatalf("order differs at %d: %s vs %s", i, first[i].ID, second[i].ID)
		}
	}
}

func TestPickCandidatesZeroSize(t *testing.T) {
	if got := PickCandidates(nil, nil, "", 0); got != nil {
		t.Fatalf("zero size must return nil, got %v", got)
	}
}

package api

import (
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"smart-gateway/internal/domain"
)

// nodeItem is the compact node representation used in list views.
type nodeItem struct {
	ID             string           `json:"id"`
	Name           string           `json:"name"`
	Server         string           `json:"server"`
	Port           int              `json:"port"`
	Protocol       domain.Protocol  `json:"protocol"`
	SubscriptionID string           `json:"subscription_id"`
	State          domain.NodeState `json:"state"`
	Active         bool             `json:"active"`
	Health         *healthSummary   `json:"health,omitempty"`
	Score          *scoreSummary    `json:"score,omitempty"`
}

type healthSummary struct {
	Probes              int       `json:"probes"`
	SuccessCount        int       `json:"success_count"`
	FailureCount        int       `json:"failure_count"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	LastLatencyMs       int64     `json:"last_latency_ms,omitempty"`
	EWMAMs              int64     `json:"ewma_ms,omitempty"`
	JitterMs            int64     `json:"jitter_ms,omitempty"`
	LossRate            float64   `json:"loss_rate"`
	ThroughputBps       float64   `json:"throughput_bps,omitempty"`
	LastProbedAt        time.Time `json:"last_probed_at,omitempty"`
	LastSuccessAt       time.Time `json:"last_success_at,omitempty"`
}

type scoreSummary struct {
	Total       float64           `json:"total"`
	Latency     float64           `json:"latency"`
	Stability   float64           `json:"stability"`
	Throughput  float64           `json:"throughput"`
	Loss        float64           `json:"loss"`
	Confidence  int               `json:"confidence"`
	Eligible    bool              `json:"eligible"`
	State       domain.ScoreState `json:"state"`
	EvaluatedAt time.Time         `json:"evaluated_at,omitempty"`
	Reason      string            `json:"reason,omitempty"`
}

func toHealthSummary(h domain.NodeHealth) *healthSummary {
	return &healthSummary{
		Probes:              h.Probes,
		SuccessCount:        h.SuccessCount,
		FailureCount:        h.FailureCount,
		ConsecutiveFailures: h.ConsecutiveFailures,
		LastLatencyMs:       h.LastLatency.Milliseconds(),
		EWMAMs:              h.EWMA.Milliseconds(),
		JitterMs:            h.Jitter.Milliseconds(),
		LossRate:            h.LossRate,
		ThroughputBps:       h.ThroughputBps,
		LastProbedAt:        h.LastProbedAt,
		LastSuccessAt:       h.LastSuccessAt,
	}
}

func toScoreSummary(ns domain.NodeScore) *scoreSummary {
	return &scoreSummary{
		Total:       ns.Score.Total,
		Latency:     ns.Score.Latency,
		Stability:   ns.Score.Stability,
		Throughput:  ns.Score.Throughput,
		Loss:        ns.Score.Loss,
		Confidence:  ns.Score.Confidence,
		Eligible:    ns.Score.Eligible,
		State:       ns.State,
		EvaluatedAt: ns.EvaluatedAt,
		Reason:      ns.Reason,
	}
}

// handleListNodes returns the fleet with filter/sort/pagination. Filters run
// in memory over the live fleet (single scan); only the returned page is
// enriched with health/score lookups, so the view stays cheap at 10k nodes.
func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodes, err := s.deps.Store.ListAllNodes(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "list nodes failed")
		return
	}
	if q := strings.TrimSpace(r.URL.Query().Get("state")); q != "" {
		nodes = filterNodes(nodes, func(n domain.Node) bool { return string(n.State) == q })
	}
	if q := strings.TrimSpace(r.URL.Query().Get("protocol")); q != "" {
		nodes = filterNodes(nodes, func(n domain.Node) bool { return string(n.Protocol) == q })
	}
	if q := strings.TrimSpace(r.URL.Query().Get("subscription_id")); q != "" {
		nodes = filterNodes(nodes, func(n domain.Node) bool { return n.SubscriptionID == q })
	}

	scores := make(map[string]domain.NodeScore)
	if all, err := s.deps.Store.ListScores(ctx); err == nil {
		for _, ns := range all {
			scores[ns.NodeID] = ns
		}
	}
	activeID, _ := s.deps.Routes.Current()
	sortNodes(nodes, scores, r.URL.Query().Get("sort"), r.URL.Query().Get("order"))

	total := len(nodes)
	page, pageSize := parsePage(r)
	start := (page - 1) * pageSize
	if start > len(nodes) {
		start = len(nodes)
	}
	end := start + pageSize
	if end > len(nodes) {
		end = len(nodes)
	}

	items := make([]nodeItem, 0, end-start)
	for _, n := range nodes[start:end] {
		item := nodeItem{
			ID: n.ID, Name: n.Name, Server: n.Server, Port: n.Port,
			Protocol: n.Protocol, SubscriptionID: n.SubscriptionID,
			State: n.State, Active: n.ID == activeID,
		}
		if h, err := s.deps.Store.GetNodeHealth(ctx, n.ID); err == nil && h.Probes > 0 {
			item.Health = toHealthSummary(h)
		}
		if ns, ok := scores[n.ID]; ok {
			item.Score = toScoreSummary(ns)
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"total": total, "page": page, "page_size": pageSize, "items": items,
	})
}

func filterNodes(nodes []domain.Node, keep func(domain.Node) bool) []domain.Node {
	out := nodes[:0]
	for _, n := range nodes {
		if keep(n) {
			out = append(out, n)
		}
	}
	return out
}

func sortNodes(nodes []domain.Node, scores map[string]domain.NodeScore, sortBy, order string) {
	less := func(i, j int) bool {
		a, b := nodes[i], nodes[j]
		switch sortBy {
		case "latency":
			ha := scoreOf(scores, a.ID).Score.Latency
			hb := scoreOf(scores, b.ID).Score.Latency
			if ha != hb {
				return ha < hb
			}
		case "name":
			if a.Name != b.Name {
				return a.Name < b.Name
			}
		case "server":
			if a.Server != b.Server {
				return a.Server < b.Server
			}
		case "created":
			if !a.CreatedAt.Equal(b.CreatedAt) {
				return a.CreatedAt.Before(b.CreatedAt)
			}
		default: // score
			sa, sb := scoreOf(scores, a.ID).Score.Total, scoreOf(scores, b.ID).Score.Total
			if sa != sb {
				return sa > sb
			}
			if a.Name != b.Name {
				return a.Name < b.Name
			}
		}
		return a.ID < b.ID
	}
	if order == "asc" && sortBy != "score" {
		base := less
		less = func(i, j int) bool { return base(j, i) }
	}
	sort.SliceStable(nodes, less)
}

func scoreOf(scores map[string]domain.NodeScore, id string) domain.NodeScore {
	if ns, ok := scores[id]; ok {
		return ns
	}
	return domain.NodeScore{}
}

func parsePage(r *http.Request) (page, pageSize int) {
	page = 1
	if v, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && v > 0 {
		page = v
	}
	pageSize = 50
	if v, err := strconv.Atoi(r.URL.Query().Get("page_size")); err == nil && v > 0 {
		pageSize = v
	}
	if pageSize > 200 {
		pageSize = 200
	}
	return page, pageSize
}

// handleGetNode returns one node with its health, current score and history.
func (s *Server) handleGetNode(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	n, err := s.deps.Store.GetNode(ctx, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "lookup node failed")
		return
	}
	if n == nil {
		writeErr(w, http.StatusNotFound, "not_found", "node not found")
		return
	}

	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > 500 {
		limit = 500
	}

	payload := map[string]any{
		"id":               n.ID,
		"name":             n.Name,
		"server":           n.Server,
		"port":             n.Port,
		"protocol":         n.Protocol,
		"transport":        n.Transport,
		"tls":              n.TLS,
		"sni":              n.SNI,
		"fingerprint":      n.Fingerprint,
		"host":             n.Host,
		"path":             n.Path,
		"service_name":     n.ServiceName,
		"flow":             n.Flow,
		"subscription_id":  n.SubscriptionID,
		"tags":             n.Tags,
		"state":            n.State,
		"state_changed_at": n.StateChangedAt,
		"created_at":       n.CreatedAt,
		"updated_at":       n.UpdatedAt,
	}
	if h, err := s.deps.Store.GetNodeHealth(ctx, n.ID); err == nil && h.Probes > 0 {
		payload["health"] = toHealthSummary(h)
	}
	if ns, err := s.deps.Store.GetNodeScore(ctx, n.ID); err == nil {
		payload["score"] = toScoreSummary(ns)
	}
	if history, err := s.deps.Store.ScoreHistory(ctx, n.ID, limit); err == nil {
		historyOut := make([]*scoreSummary, 0, len(history))
		for _, ns := range history {
			historyOut = append(historyOut, toScoreSummary(ns))
		}
		payload["score_history"] = historyOut
	}
	writeJSON(w, http.StatusOK, payload)
}

package api

import (
	"context"
	"net/http"
	"time"

	"smart-gateway/internal/domain"
)

// statusPayload is the response of GET /api/v1/status.
type statusPayload struct {
	Version       string              `json:"version"`
	UptimeSeconds int64               `json:"uptime_seconds"`
	API           apiSummary          `json:"api"`
	Core          coreSummary         `json:"core"`
	Nodes         nodeSummary         `json:"nodes"`
	Subscriptions subscriptionSummary `json:"subscriptions"`
	Selection     selectionSummary    `json:"selection"`
	GeneratedAt   time.Time           `json:"generated_at"`
}

type apiSummary struct {
	Dashboard bool `json:"dashboard"`
}

type coreSummary struct {
	Running    bool `json:"running"`
	Failed     bool `json:"failed"`
	Restarts1h int  `json:"restarts_1h"`
}

type nodeSummary struct {
	Total   int                      `json:"total"`
	Scored  int                      `json:"scored"`
	ByState map[domain.NodeState]int `json:"by_state"`
}

type subscriptionSummary struct {
	Total     int `json:"total"`
	Active    int `json:"active"`
	Error     int `json:"error"`
	Disabled  int `json:"disabled"`
	NodeCount int `json:"node_count"`
}

type selectionSummary struct {
	NodeID     string `json:"node_id,omitempty"`
	Name       string `json:"name,omitempty"`
	Reason     string `json:"reason,omitempty"`
	Overridden bool   `json:"overridden"`
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sel, _ := s.selection(ctx)
	subs, _ := s.deps.Store.ListSubscriptions(ctx)
	var subSummary subscriptionSummary
	for _, sub := range subs {
		subSummary.Total++
		subSummary.NodeCount += sub.NodeCount
		switch sub.State {
		case domain.SubscriptionActive:
			subSummary.Active++
		case domain.SubscriptionError:
			subSummary.Error++
		case domain.SubscriptionDisabled:
			subSummary.Disabled++
		}
	}
	total, _ := s.deps.Store.CountNodes(ctx)
	scored, _ := s.deps.Store.CountScores(ctx)
	byState, _ := s.deps.Store.CountNodesByState(ctx)

	writeJSON(w, http.StatusOK, statusPayload{
		Version:       s.deps.Version,
		UptimeSeconds: int64(time.Since(s.started).Seconds()),
		API:           apiSummary{Dashboard: s.deps.Config.API.Dashboard},
		Core:          s.coreSummary(),
		Nodes:         nodeSummary{Total: total, Scored: scored, ByState: byState},
		Subscriptions: subSummary,
		Selection:     sel,
		GeneratedAt:   time.Now().UTC(),
	})
}

func (s *Server) coreSummary() coreSummary {
	if s.deps.Core == nil {
		return coreSummary{}
	}
	return coreSummary{
		Running:    s.deps.Core.Running(),
		Failed:     s.deps.Core.Failed(),
		Restarts1h: s.deps.Core.RestartsInLastHour(),
	}
}

// selection builds the active-outbound summary (public status/outbound).
func (s *Server) selection(ctx context.Context) (selectionSummary, error) {
	out := selectionSummary{}
	nodeID, reason := s.deps.Routes.Current()
	out.NodeID = nodeID
	out.Reason = reason
	out.Overridden = reason == "override"
	if nodeID == "" {
		return out, nil
	}
	if n, err := s.deps.Store.GetNode(ctx, nodeID); err == nil && n != nil {
		out.Name = n.Name
	}
	return out, nil
}

// handleMetrics exposes an operation-oriented JSON metric snapshot. The
// Prometheus exposition format lands in M6 (docs/rest-api.md); the shape here
// is deliberately stable so the M6 exporter can reuse the same sources.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sel, _ := s.selection(ctx)
	total, _ := s.deps.Store.CountNodes(ctx)
	scored, _ := s.deps.Store.CountScores(ctx)
	byState, _ := s.deps.Store.CountNodesByState(ctx)

	writeJSON(w, http.StatusOK, map[string]any{
		"generated_at":   time.Now().UTC(),
		"uptime_seconds": int64(time.Since(s.started).Seconds()),
		"core":           s.coreSummary(),
		"nodes": map[string]any{
			"total":    total,
			"scored":   scored,
			"by_state": byState,
		},
		"selection": sel,
	})
}

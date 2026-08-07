package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"smart-gateway/internal/domain"
)

// handleOutbound reports the current active outbound plus the top eligible
// candidates (public view, FR-805).
func (s *Server) handleOutbound(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sel, _ := s.selection(ctx)
	nodes, _ := s.deps.Store.CountNodes(ctx)
	top, err := s.deps.Store.TopEligible(ctx, 10)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "candidates lookup failed")
		return
	}

	candidates := make([]map[string]any, 0, len(top))
	seen := map[string]bool{}
	for _, ns := range top {
		seen[ns.NodeID] = true
		name := ns.NodeID
		if n, err := s.deps.Store.GetNode(ctx, ns.NodeID); err == nil && n != nil {
			name = n.Name
		}
		candidates = append(candidates, map[string]any{
			"node_id": ns.NodeID, "name": name,
			"total": ns.Score.Total, "eligible": ns.Eligible(),
			"state": ns.State, "evaluated_at": ns.EvaluatedAt,
		})
	}
	if sel.NodeID != "" && !seen[sel.NodeID] {
		if ns, err := s.deps.Store.GetNodeScore(ctx, sel.NodeID); err == nil {
			name := sel.NodeID
			if n, err := s.deps.Store.GetNode(ctx, sel.NodeID); err == nil && n != nil {
				name = n.Name
			}
			candidates = append(candidates, map[string]any{
				"node_id": sel.NodeID, "name": name,
				"total": ns.Score.Total, "eligible": ns.Eligible(),
				"state": ns.State, "evaluated_at": ns.EvaluatedAt,
			})
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"current":     sel,
		"candidates":  candidates,
		"total_nodes": nodes,
	})
}

// overrideRequest is the POST /api/v1/outbound/override body.
type overrideRequest struct {
	NodeID           string `json:"node_id"`
	Reason           string `json:"reason,omitempty"`
	AutoReleaseAfter string `json:"auto_release_after,omitempty"` // Go duration or plain seconds
}

func (s *Server) handleOverride(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req overrideRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.NodeID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "node_id is required")
		return
	}
	nodes, err := s.deps.Store.ListAllNodes(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "list nodes failed")
		return
	}
	reason := req.Reason
	if reason == "" {
		reason = "override"
	}
	if err := s.deps.Routes.Override(ctx, nodes, req.NodeID, reason); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}

	var releaseAt *time.Time
	if req.AutoReleaseAfter != "" {
		d, err := parseDurationOrSeconds(req.AutoReleaseAfter)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "bad_request", "invalid auto_release_after")
			return
		}
		at := time.Now().Add(d)
		releaseAt = &at
		s.mu.Lock()
		s.auto[req.NodeID] = at
		s.mu.Unlock()
	}

	sel, _ := s.selection(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"selection": sel, "auto_release_at": releaseAt,
	})
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodes, err := s.deps.Store.ListAllNodes(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "list nodes failed")
		return
	}
	s.mu.Lock()
	clear(s.auto)
	s.mu.Unlock()
	if err := s.deps.Routes.Release(ctx, nodes); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "release failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleEvaluate dry-runs the selection policy without applying it.
func (s *Server) handleEvaluate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	nodes, err := s.deps.Store.ListAllNodes(ctx)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "list nodes failed")
		return
	}
	d, err := s.deps.Routes.Evaluate(ctx, nodes)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "evaluation failed")
		return
	}
	writeJSON(w, http.StatusOK, s.decisionPayload(ctx, d))
}

func (s *Server) decisionPayload(ctx context.Context, d domain.SwitchDecision) map[string]any {
	out := map[string]any{
		"at":              d.At,
		"candidate_id":    d.CandidateID,
		"current_id":      d.CurrentID,
		"reason":          d.Reason,
		"outcome":         d.Outcome,
		"candidate_score": d.CandidateScore,
		"current_score":   d.CurrentScore,
		"delta":           d.Delta,
	}
	if n, err := s.deps.Store.GetNode(ctx, d.CandidateID); err == nil && n != nil {
		out["candidate_name"] = n.Name
	}
	if n, err := s.deps.Store.GetNode(ctx, d.CurrentID); err == nil && n != nil {
		out["current_name"] = n.Name
	}
	return out
}

// Run owns the auto-release loop for admin overrides. The app starts it on
// the shared background context.
func (s *Server) Run(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.releaseExpired(ctx)
		}
	}
}

func (s *Server) releaseExpired(ctx context.Context) {
	now := time.Now()
	s.mu.Lock()
	var due []string
	for id, at := range s.auto {
		if now.After(at) {
			due = append(due, id)
		}
	}
	for _, id := range due {
		delete(s.auto, id)
	}
	s.mu.Unlock()
	if len(due) == 0 {
		return
	}
	curID, _ := s.deps.Routes.Current()
	expired := map[string]bool{}
	for _, id := range due {
		expired[id] = true
	}
	if !expired[curID] {
		return
	}
	nodes, err := s.deps.Store.ListAllNodes(ctx)
	if err != nil {
		s.logger.Warn("auto-release: list nodes", "error", err)
		return
	}
	if err := s.deps.Routes.Release(ctx, nodes); err != nil {
		s.logger.Warn("auto-release override", "error", err)
		return
	}
	s.logger.Info("override auto-released", "node", curID)
}

func parseDurationOrSeconds(s string) (time.Duration, error) {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(v * float64(time.Second)), nil
	}
	return time.ParseDuration(s)
}

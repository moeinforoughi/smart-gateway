package api

import (
	"encoding/json"
	"net/http"
	"time"

	"smart-gateway/internal/domain"
	"smart-gateway/internal/subscription"
)

// subscriptionItem is the public view of a subscription. Custom headers are
// omitted (they may carry provider credentials).
type subscriptionItem struct {
	ID                  string                   `json:"id"`
	URL                 string                   `json:"url"`
	Name                string                   `json:"name"`
	Provider            string                   `json:"provider"`
	Priority            int                      `json:"priority"`
	Tags                []string                 `json:"tags"`
	Enabled             bool                     `json:"enabled"`
	State               domain.SubscriptionState `json:"state"`
	ConsecutiveFailures int                      `json:"consecutive_failures"`
	NodeCount           int                      `json:"node_count"`
	LastFetchAt         time.Time                `json:"last_fetch_at,omitempty"`
	NextFetchAt         time.Time                `json:"next_fetch_at,omitempty"`
	LastError           string                   `json:"last_error,omitempty"`
	CreatedAt           time.Time                `json:"created_at"`
	UpdatedAt           time.Time                `json:"updated_at"`
}

func toSubscriptionItem(s domain.Subscription) subscriptionItem {
	return subscriptionItem{
		ID: s.ID, URL: s.URL, Name: s.Name, Provider: s.Provider,
		Priority: s.Priority, Tags: s.Tags, Enabled: s.Enabled,
		State: s.State, ConsecutiveFailures: s.ConsecutiveFailures,
		NodeCount: s.NodeCount, LastFetchAt: s.LastFetchAt,
		NextFetchAt: s.NextFetchAt, LastError: s.LastError,
		CreatedAt: s.CreatedAt, UpdatedAt: s.UpdatedAt,
	}
}

func (s *Server) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := s.deps.Store.ListSubscriptions(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "list subscriptions failed")
		return
	}
	items := make([]subscriptionItem, 0, len(subs))
	for _, sub := range subs {
		items = append(items, toSubscriptionItem(sub))
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

// addSubscriptionRequest is the POST /api/v1/subscriptions body.
type addSubscriptionRequest struct {
	URL         string            `json:"url"`
	Name        string            `json:"name"`
	Provider    string            `json:"provider,omitempty"`
	Priority    int               `json:"priority,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Enabled     *bool             `json:"enabled,omitempty"`
	UpdateEvery string            `json:"update_every,omitempty"`
	Timeout     string            `json:"timeout,omitempty"`
}

func (s *Server) handleAddSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req addSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", "invalid JSON body")
		return
	}
	if req.URL == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "url is required")
		return
	}
	now := time.Now()
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	name := req.Name
	if name == "" {
		name = req.Provider
	}
	sub := &domain.Subscription{
		URL:       req.URL,
		Name:      name,
		Provider:  req.Provider,
		Priority:  req.Priority,
		Tags:      req.Tags,
		Headers:   req.Headers,
		Enabled:   enabled,
		State:     domain.SubscriptionActive,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if d, err := time.ParseDuration(req.UpdateEvery); err == nil && d > 0 {
		sub.UpdateEvery = d
	}
	if d, err := time.ParseDuration(req.Timeout); err == nil && d > 0 {
		sub.Timeout = d
	}
	if err := s.deps.Store.SaveSubscription(ctx, sub); err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "save subscription failed")
		return
	}
	// Fetch immediately so the operator sees live nodes (best-effort).
	var result *subscription.Result
	if s.deps.Subscriber != nil {
		result, _ = s.deps.Subscriber.RefreshByID(ctx, sub.ID)
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"subscription": toSubscriptionItem(*sub),
		"refresh":      result,
	})
}

func (s *Server) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	existed, err := s.deps.Store.DeleteSubscription(ctx, id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "delete subscription failed")
		return
	}
	if !existed {
		writeErr(w, http.StatusNotFound, "not_found", "subscription not found")
		return
	}
	// The active outbound may have pointed at a removed node: re-run selection.
	if nodes, err := s.deps.Store.ListAllNodes(ctx); err == nil {
		if err := s.deps.Routes.EnsureActive(ctx, nodes); err != nil {
			s.logger.Warn("re-select after subscription delete", "error", err)
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRefreshSubscription(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if s.deps.Subscriber == nil {
		writeErr(w, http.StatusInternalServerError, "internal", "refresh unavailable")
		return
	}
	result, err := s.deps.Subscriber.RefreshByID(ctx, id)
	if err != nil {
		if err == subscription.ErrNotFound {
			writeErr(w, http.StatusNotFound, "not_found", "subscription not found")
			return
		}
		writeErr(w, http.StatusInternalServerError, "internal", "refresh failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}

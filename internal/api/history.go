package api

import (
	"net/http"
	"strconv"
)

// handleHistory returns the switch history with node names (FR-705/706,
// FR-804). Newest first.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit := 50
	if v, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > 500 {
		limit = 500
	}
	records, err := s.deps.Store.ListSwitchRecords(ctx, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "list switch history failed")
		return
	}
	names := make(map[string]string)
	name := func(id string) string {
		if id == "" {
			return ""
		}
		if n, ok := names[id]; ok {
			return n
		}
		if node, err := s.deps.Store.GetNode(ctx, id); err == nil && node != nil {
			names[id] = node.Name
		}
		return names[id]
	}

	items := make([]map[string]any, 0, len(records))
	for _, rec := range records {
		items = append(items, map[string]any{
			"id":         rec.ID,
			"at":         rec.At,
			"from_id":    rec.FromID,
			"from_name":  name(rec.FromID),
			"to_id":      rec.ToID,
			"to_name":    name(rec.ToID),
			"reason":     rec.Reason,
			"prev_score": rec.PrevScore,
			"new_score":  rec.NewScore,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "total": len(items)})
}

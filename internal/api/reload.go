package api

import (
	"net/http"
)

// handleReload performs a config-only reload (SIGHUP equivalent). Only
// runtime-safe sections are applied; the rest require a restart.
func (s *Server) handleReload(w http.ResponseWriter, r *http.Request) {
	if s.deps.Reload == nil {
		writeErr(w, http.StatusNotImplemented, "not_implemented", "reload not available")
		return
	}
	res, err := s.deps.Reload(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "reload failed: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

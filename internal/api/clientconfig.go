package api

import (
	"net/http"
)

// handleClientConfig returns the stable per-inbound client configs (sing-box
// JSON + v2rayN share links). The payload is derived from the configured
// listeners plus the persisted reality keypair (M5).
func (s *Server) handleClientConfig(w http.ResponseWriter, r *http.Request) {
	if s.deps.ClientConfig == nil {
		writeErr(w, http.StatusServiceUnavailable, "unavailable", "client config not available")
		return
	}
	cc, err := s.deps.ClientConfig()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "internal", "build client config failed")
		return
	}
	writeJSON(w, http.StatusOK, cc)
}

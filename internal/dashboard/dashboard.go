// Package dashboard embeds the single-page control UI (M5). It is served by
// the API server at /dashboard (architecture rank 3: no internal imports;
// the UI talks to the API over HTTP on the same origin).
package dashboard

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var indexHTML []byte

// Handler serves the embedded dashboard. Any path under /dashboard maps to
// the single index.html (hash-based SPA navigation).
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(indexHTML)
	})
}

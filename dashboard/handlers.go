// Package dashboard provides HTTP handlers for serving the dashboard.
// It serves the static dashboard.html and handles WebSocket connections.
package dashboard

import (
	"net/http"
	_ "embed"
)

//go:embed dashboard.html
var dashboardHTML []byte

// Handler serves the main dashboard HTML page at GET /.
func Handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(dashboardHTML)
}

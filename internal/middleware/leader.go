package middleware

import "net/http"

// RoleHeader tells every client which role answered: "leader" or "standby"
const RoleHeader = "X-Superpeer-Role"

// RequireLeader lets requests through only while this superpeer is the leader
// On standby it answers 421 Misdirected Request: "this is not the active superpeer". The peer counts
// each 421 as a failure and, after a few in a row, switches to the other superpeer
// (503 means something else for the peer: "active but still starting", it waits and does not switch)
// GET /health always passes, so health checks work on both superpeers; the header shows the role
func RequireLeader(isLeader func() bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isLeader() {
				w.Header().Set(RoleHeader, "leader")
				next.ServeHTTP(w, r)
				return
			}

			w.Header().Set(RoleHeader, "standby")
			if r.URL.Path == "/health" {
				next.ServeHTTP(w, r)
				return
			}
			writeError(w, http.StatusMisdirectedRequest, "this superpeer is on standby, use the other one")
		})
	}
}

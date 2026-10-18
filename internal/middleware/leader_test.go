package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRequireLeader(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })

	cases := []struct {
		name     string
		leader   bool
		path     string
		wantCode int
		wantRole string
	}{
		{"leader serves everything", true, "/fs/ls", http.StatusOK, "leader"},
		{"standby rejects with 421", false, "/fs/ls", http.StatusMisdirectedRequest, "standby"},
		{"standby rejects login too", false, "/auth/login", http.StatusMisdirectedRequest, "standby"},
		{"standby still answers health", false, "/health", http.StatusOK, "standby"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			RequireLeader(func() bool { return c.leader })(ok).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, c.path, nil))
			if rec.Code != c.wantCode || rec.Header().Get(RoleHeader) != c.wantRole {
				t.Fatalf("got %d role=%q, want %d role=%q", rec.Code, rec.Header().Get(RoleHeader), c.wantCode, c.wantRole)
			}
		})
	}
}

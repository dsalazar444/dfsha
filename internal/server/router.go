// Package server wires together all routes and middleware for the superpeer
package server

import (
	"dfsha/internal/config"
	"dfsha/internal/middleware"
	"net/http"

	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
)

// tokenValidator is the subset of auth.Service needed by the router
type tokenValidator interface {
	UserIDByToken(token string) (string, bool)
}

// NewRouter builds and returns the chi router with all superpeer routes
// Each phase will register its own sub-router here
func NewRouter(cfg *config.Config, tokens tokenValidator) http.Handler {
	r := chi.NewRouter()

	// Global middleware 
	r.Use(chiMiddleware.RequestID)   // attach unique X-Request-Id to each request
	r.Use(chiMiddleware.RealIP)      // read real IP from X-Forwarded-For
	r.Use(chiMiddleware.Logger)      // structured request log line
	r.Use(chiMiddleware.Recoverer)   // recover from panics and return 500

	// Health check (no auth) 
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})

	// Public routes (no token required) 
	// Phase 1: auth handler will be mounted here
	r.Post("/auth/login", notImplemented("POST /auth/login"))

	// Protected routes (require Bearer token)
	r.Group(func(protected chi.Router) {
		protected.Use(middleware.RequireAuth(tokens))

		// Phase 2: peer management
		protected.Post("/peers/register", notImplemented("POST /peers/register"))
		protected.Post("/peers/heartbeat", notImplemented("POST /peers/heartbeat"))

		// Phase 3: logical filesystem
		protected.Post("/fs/mkdir", notImplemented("POST /fs/mkdir"))
		protected.Delete("/fs/rmdir", notImplemented("DELETE /fs/rmdir"))
		protected.Get("/fs/ls", notImplemented("GET /fs/ls"))

		// Phase 4: file upload coordination
		protected.Post("/files/put/init", notImplemented("POST /files/put/init"))
		protected.Post("/files/put/ack", notImplemented("POST /files/put/ack"))

		// Phase 5: file download and deletion
		protected.Get("/files/get/{path}", notImplemented("GET /files/get/{path}"))
		protected.Delete("/files/{path}", notImplemented("DELETE /files/{path}"))

		// Phase 6: locking
		protected.Post("/files/lock/{path}", notImplemented("POST /files/lock/{path}"))
		protected.Post("/files/unlock/{path}", notImplemented("POST /files/unlock/{path}"))
	})

	return r
}

// notImplemented returns a placeholder handler that responds 501 with a clear
// message. Each handler is replaced as its phase is implemented.
func notImplemented(endpoint string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"error":"not implemented: ` + endpoint + `"}`))
	}
}

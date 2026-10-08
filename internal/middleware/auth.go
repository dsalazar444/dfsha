// Package middleware provides HTTP middleware for the superpeer
package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// contextKey is an unexported type for context keys in this package
type contextKey string

const userIDKey contextKey = "userID"

// tokenStore is the interface the auth middleware needs to validate tokens
// The concrete implementation lives in internal/auth
type tokenStore interface {
	// UserIDByToken returns the userID associated with a token
	// and false if the token is unknown or expired
	UserIDByToken(token string) (userID string, ok bool)
}

// RequireAuth returns a middleware that validates the Bearer token and
// stores the resolved userID in the request context (retrievable via UserIDFromContext)
func RequireAuth(store tokenStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := extractBearerToken(r)
			if token == "" {
				writeError(w, http.StatusUnauthorized, "missing or malformed Authorization header")
				return
			}

			userID, ok := store.UserIDByToken(token)
			if !ok {
				writeError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}

			ctx := context.WithValue(r.Context(), userIDKey, userID)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// UserIDFromContext retrieves the userID stored by RequireAuth, or an empty string if not found
func UserIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(userIDKey).(string)
	return v
}

// extractBearerToken parses "Authorization: Bearer <token>" and returns the token
func extractBearerToken(r *http.Request) string {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(header, "Bearer ")
}

// writeError writes a JSON error response: {"error": "message"}
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

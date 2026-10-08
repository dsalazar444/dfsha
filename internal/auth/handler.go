// Package auth - HTTP handlers for the auth group
package auth

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Handler holds the HTTP handlers for auth endpoints
type Handler struct {
	service *Service
}

// NewHandler creates an auth Handler backed by the given Service
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// Login handles POST /auth/login
//
// Contract: superpeer-openapi.yaml - /auth/login
//
//	Request:  { "username": string, "password": string }
//	Response: { "session_token": string }          200
//	          { "error": string }                  400 | 401 | 500

func (h *Handler) Login(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.Username == "" || body.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}

	token, err := h.service.Login(body.Username, body.Password)
	if err != nil {
		if errors.Is(err, ErrInvalidCredentials) {
			writeError(w, http.StatusUnauthorized, "invalid credentials")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"session_token": token})
}

// writeError writes {"error": msg} with the given HTTP status code
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// Package locks handles HTTP handlers for file locking endpoints
package locks

import (
	"dfsha/internal/superpeer/middleware"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// Handler holds HTTP handlers for file locking
type Handler struct {
	service *Service
}

// NewHandler creates a new locks Handler backed by the given Service
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// Lock handles POST /files/lock/{path}
func (h *Handler) Lock(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	path := "/" + chi.URLParam(r, "*")
	result, err := h.service.Lock(userID, path)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			writeError(w, http.StatusNotFound, "file not found")
			return
		}
		if errors.Is(err, ErrAlreadyLocked) {
			writeError(w, http.StatusConflict, "file is already locked by another user")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"lock_id":    result.LockID,
		"expires_at": result.ExpiresAt.Format(time.RFC3339),
	})
}

// Unlock handles POST /files/unlock/{path}
func (h *Handler) Unlock(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var body struct {
		LockID string `json:"lock_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.LockID == "" {
		writeError(w, http.StatusBadRequest, "lock_id is required")
		return
	}

	path := "/" + chi.URLParam(r, "*")
	err := h.service.Unlock(userID, path, body.LockID)
	if err != nil {
		if errors.Is(err, ErrFileNotFound) {
			writeError(w, http.StatusNotFound, "file not found")
			return
		}
		if errors.Is(err, ErrLockNotFound) {
			writeError(w, http.StatusNotFound, "lock has expired or does not exist")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

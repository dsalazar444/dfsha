// Package fs  HTTP handlers for filesystem metadata operations
package fs

import (
	"dfsha/internal/middleware"
	"dfsha/internal/store"
	"encoding/json"
	"errors"
	"net/http"
)

// Handler holds the HTTP handlers for filesystem endpoints
type Handler struct {
	service *Service
}

// NewHandler creates a new fs Handler backed by the given Service
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// Mkdir handles POST /fs/mkdir
func (h *Handler) Mkdir(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var body struct {
		Path string `json:"path"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.Path == "" {
		writeError(w, http.StatusBadRequest, "path is required")
		return
	}

	err := h.service.Mkdir(userID, body.Path)
	if err != nil {
		if errors.Is(err, store.ErrPathNotFound) {
			writeError(w, http.StatusBadRequest, "parent directory does not exist")
			return
		}
		if errors.Is(err, store.ErrPathAlreadyExists) {
			writeError(w, http.StatusBadRequest, "path already exists")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Rmdir handles DELETE /fs/rmdir
func (h *Handler) Rmdir(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var body struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}
	if body.Path == "" {
		writeError(w, http.StatusBadRequest, "path is required")
		return
	}

	err := h.service.Rmdir(userID, body.Path)
	if err != nil {
		if errors.Is(err, store.ErrPathNotFound) {
			writeError(w, http.StatusNotFound, "path not found")
			return
		}
		if errors.Is(err, store.ErrNotADirectory) {
			writeError(w, http.StatusBadRequest, "path is not a directory")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Ls handles GET /fs/ls?path=...
func (h *Handler) Ls(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	path := r.URL.Query().Get("path")
	if path == "" {
		writeError(w, http.StatusBadRequest, "path query parameter is required")
		return
	}

	entries, err := h.service.Ls(userID, path)
	if err != nil {
		if errors.Is(err, store.ErrPathNotFound) {
			writeError(w, http.StatusNotFound, "path not found")
			return
		}
		if errors.Is(err, store.ErrNotADirectory) {
			writeError(w, http.StatusBadRequest, "path is not a directory")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Always return an array, even if empty, instead of null
	if entries == nil {
		entries = make([]FsEntry, 0)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{"entries": entries})
}

// writeError writes {"error": msg} with the given HTTP status code
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

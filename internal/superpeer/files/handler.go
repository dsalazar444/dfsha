// Package files  HTTP handlers for file upload/download endpoints

package files

import (
	"dfsha/internal/superpeer/middleware"
	"dfsha/internal/superpeer/store"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
)

// Handler holds the HTTP handlers for file endpoints
type Handler struct {
	service *Service
}

// NewHandler creates a new file Handler backed by the given Service
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// PutInit handles POST /files/put/init
func (h *Handler) PutInit(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var body struct {
		FileID          string   `json:"file_id"`
		LogicalPath     string   `json:"logical_path"`
		ChunkCount      int      `json:"chunk_count"`
		HashOfEachChunk []string `json:"hash_of_each_chunk"`
		HashPerChunk    []string `json:"hash_per_chunk"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	hashes := body.HashOfEachChunk
	if len(hashes) == 0 && len(body.HashPerChunk) > 0 {
		hashes = body.HashPerChunk
	}

	if body.LogicalPath == "" || body.ChunkCount <= 0 {
		writeError(w, http.StatusBadRequest, "logical_path and positive chunk_count are required")
		return
	}
	if len(hashes) != body.ChunkCount {
		writeError(w, http.StatusBadRequest, "hashes length must match chunk_count")
		return
	}

	fileID, assignments, err := h.service.InitPut(userID, body.LogicalPath, body.FileID, body.ChunkCount, hashes)
	if err != nil {
		if errors.Is(err, store.ErrPathAlreadyExists) {
			writeError(w, http.StatusConflict, "file already exists at this path")
			return
		}
		if errors.Is(err, ErrFileIDConflict) {
			writeError(w, http.StatusConflict, "file_id is already in use by another file")
			return
		}
		if errors.Is(err, store.ErrPathNotFound) || errors.Is(err, store.ErrNotADirectory) {
			writeError(w, http.StatusBadRequest, "invalid parent directory")
			return
		}
		if errors.Is(err, ErrInsufficientPeers) {
			writeError(w, http.StatusInternalServerError, "not enough healthy peers to accept upload")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := map[string]interface{}{
		"file_id":    fileID,
		"assignment": assignments,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}

// PutAck handles POST /files/put/ack
func (h *Handler) PutAck(w http.ResponseWriter, r *http.Request) {
	// Require valid token as always
	if middleware.UserIDFromContext(r.Context()) == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	var body struct {
		ChunkID string `json:"chunk_id"`
		PeerID  string `json:"peer_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.ChunkID == "" || body.PeerID == "" {
		writeError(w, http.StatusBadRequest, "chunk_id and peer_id are required")
		return
	}

	err := h.service.AckPut(body.ChunkID, body.PeerID)
	if err != nil {
		if errors.Is(err, ErrInvalidChunk) {
			writeError(w, http.StatusNotFound, "chunk_id not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

// Get handles GET /files/get/{path}
func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	fileID, chunks, err := h.service.Get(userID, logicalPath(r))
	if err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "file not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"file_id": fileID,
		"chunks":  chunks,
	})
}

// Delete handles DELETE /files/{path}
func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	userID := middleware.UserIDFromContext(r.Context())
	if userID == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}

	if err := h.service.Delete(userID, logicalPath(r)); err != nil {
		if isNotFound(err) {
			writeError(w, http.StatusNotFound, "file not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": "Starting file deletion"})
}

// logicalPath returns the file path captured by the "*" route wildcard as an absolute path
// (the superpeer always works with absolute paths, the URL has no leading slash)
func logicalPath(r *http.Request) string {
	return "/" + chi.URLParam(r, "*")
}

// isNotFound tells if the error means that the path does not lead to a file
func isNotFound(err error) bool {
	return errors.Is(err, store.ErrPathNotFound) ||
		errors.Is(err, store.ErrNotADirectory) ||
		errors.Is(err, store.ErrFileNotFound)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

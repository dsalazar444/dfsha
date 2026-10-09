// Package files  HTTP handlers for file upload/download endpoints

package files

import (
	"dfsha/internal/middleware"
	"dfsha/internal/store"
	"encoding/json"
	"errors"
	"net/http"
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
		LogicalPath     string   `json:"logical_path"`
		ChunkCount      int      `json:"chunk_count"`
		HashOfEachChunk []string `json:"hash_of_each_chunk"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.LogicalPath == "" || body.ChunkCount <= 0 {
		writeError(w, http.StatusBadRequest, "logical_path and positive chunk_count are required")
		return
	}
	if len(body.HashOfEachChunk) != body.ChunkCount {
		writeError(w, http.StatusBadRequest, "hash_of_each_chunk length must match chunk_count")
		return
	}

	fileID, assignments, err := h.service.InitPut(userID, body.LogicalPath, body.ChunkCount, body.HashOfEachChunk)
	if err != nil {
		if errors.Is(err, store.ErrPathAlreadyExists) {
			writeError(w, http.StatusConflict, "file already exists at this path")
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

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

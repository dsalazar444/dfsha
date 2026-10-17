// Package peers HTTP handlers for peer endpoints
package peers

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Handler holds the HTTP handlers for peer endpoints
type Handler struct {
	service *Service
}

// NewHandler creates a new peer Handler backed by the given Service
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// Register handles POST /peers/register
//
// Contract:
//	Request:  { "address": string, "port": string }
//	Response: { "peer_id": string } 200
//	          { "error": string }   400 | 401 | 500

func (h *Handler) Register(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Address string `json:"address"`
		Port    string `json:"port"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.Address == "" || body.Port == "" {
		writeError(w, http.StatusBadRequest, "address and port are required")
		return
	}

	peerID := h.service.Register(body.Address, body.Port)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"peer_id": peerID})
}

// Heartbeat handles POST /peers/heartbeat
//
// Contract:
//	Request:  { "peer_id": string, "available_space": int64 (bytes), "chunks": [string] }
//	Response: 200 (empty body or success object)
//	          { "error": string } 400 | 404 | 500

func (h *Handler) Heartbeat(w http.ResponseWriter, r *http.Request) {
	var body struct {
		PeerID         string   `json:"peer_id"`
		AvailableSpace *int64   `json:"available_space"` // pointer to tell "missing" apart from 0 bytes free
		Chunks         []string `json:"chunks"`
	}

	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}

	if body.PeerID == "" || body.AvailableSpace == nil || *body.AvailableSpace < 0 {
		writeError(w, http.StatusBadRequest, "peer_id and available_space are required")
		return
	}

	if body.Chunks == nil {
		// Ensure we don't pass nil if the JSON omitted the field
		body.Chunks = []string{}
	}

	tasks, err := h.service.Heartbeat(body.PeerID, *body.AvailableSpace, body.Chunks)
	if err != nil {
		if errors.Is(err, ErrPeerNotFound) {
			writeError(w, http.StatusNotFound, "peer not registered")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if tasks == nil {
		tasks = []ReplicationTask{}
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"tasks":  tasks,
	})
}

// writeError writes {"error": msg} with the given HTTP status code.
func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

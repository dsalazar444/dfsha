package server

import (
	"dfsha/internal/peer/storage"
	"net/http"
)

type Server struct { /* store *storage.Store */
}

func NewServer(store *storage.Store) *Server
func (s *Server) Routes() http.Handler

// handlers privados: receiveChunk, getChunk, deleteChunk

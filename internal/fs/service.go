// Package fs handles the logical filesystem operations
package fs

import (
	"dfsha/internal/store"
)

// Service manages filesystem logic
type Service struct {
	store *store.FsStore
}

// NewService creates a new fs Service
func NewService(store *store.FsStore) *Service {
	return &Service{store: store}
}

// Mkdir creates a directory in the user's isolated namespace
func (s *Service) Mkdir(userID, path string) error {
	return s.store.Mkdir(userID, path)
}

// Rmdir removes a directory in the user's isolated namespace
func (s *Service) Rmdir(userID, path string) error {
	return s.store.Rmdir(userID, path)
}

// FsEntry represents a single file or directory for listing
type FsEntry struct {
	Name string `json:"name"`
	Type string `json:"type"` // "file" or "directory"
}

// Ls returns the contents of a directory
func (s *Service) Ls(userID, path string) ([]FsEntry, error) {
	nodes, err := s.store.Ls(userID, path)
	if err != nil {
		return nil, err
	}

	entries := make([]FsEntry, 0, len(nodes))
	for _, n := range nodes {
		entries = append(entries, FsEntry{
			Name: n.Name,
			Type: n.Type,
		})
	}
	return entries, nil
}

// Package fs handles the logical filesystem operations
package fs

import (
	"dfsha/internal/superpeer/store"
)

// FileDeleter deletes the stored data (chunks in the peers) of files that were removed from the tree
// Implemented by files.Service, defined here so fs does not depend on that package
type FileDeleter interface {
	DeleteFiles(userID string, fileIDs []string)
}

// Service manages filesystem logic
type Service struct {
	store *store.FsStore
	files FileDeleter
}

// NewService creates a new fs Service
func NewService(store *store.FsStore, files FileDeleter) *Service {
	return &Service{store: store, files: files}
}

// Mkdir creates a directory in the user's isolated namespace
func (s *Service) Mkdir(userID, path string) error {
	return s.store.Mkdir(userID, path)
}

// Rmdir removes a directory in the user's isolated namespace
// The files inside are also deleted from the peers, so no orphan chunks are left behind
func (s *Service) Rmdir(userID, path string) error {
	fileIDs, err := s.store.Rmdir(userID, path)
	if err != nil {
		return err
	}

	s.files.DeleteFiles(userID, fileIDs)
	return nil
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

// Package store holds in-memory data stores for the superpeer

package store

import (
	"errors"
	"sync"
)

var (
	ErrChunkNotFound = errors.New("chunk not found")
	ErrFileNotFound  = errors.New("file not found")
)

// ChunkMeta tracks the assignment of a single chunk
type ChunkMeta struct {
	ChunkID      string
	ExpectedHash string
	Peers        []string // List of peer URLs assigned to host this chunk
}

// FileMeta tracks the chunks belonging to a logical file
type FileMeta struct {
	FileID      string
	LogicalPath string
	ChunkCount  int
	Chunks      []*ChunkMeta
}

// FileStore tracks file metadata and chunk assignments
type FileStore struct {
	mu     sync.RWMutex
	byFile map[string]*FileMeta
}

// NewFileStore creates a new empty FileStore
func NewFileStore() *FileStore {
	return &FileStore{
		byFile: make(map[string]*FileMeta),
	}
}

// AddFile registers a new file and its chunk assignments
func (s *FileStore) AddFile(file *FileMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byFile[file.FileID] = file
}

// GetFile retrieves metadata for a specific file
func (s *FileStore) GetFile(fileID string) (*FileMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	file, ok := s.byFile[fileID]
	return file, ok
}

// AckChunk confirms that a peer successfully received and saved a chunk.
// For now, it just verifies the chunk exists. The actual locations are tracked
// via the assignment given during init and periodic heartbeats

func (s *FileStore) AckChunk(chunkID string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// chunkID format: {file_id}_{index}
	for _, file := range s.byFile {
		for _, chunk := range file.Chunks {
			if chunk.ChunkID == chunkID {
				return nil
			}
		}
	}
	return ErrChunkNotFound
}

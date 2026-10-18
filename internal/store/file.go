// Package store holds in-memory data stores for the superpeer

package store

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrChunkNotFound   = errors.New("chunk not found")
	ErrFileNotFound    = errors.New("file not found")
	ErrPeerNotAssigned = errors.New("peer is not assigned to this chunk")
)

// ChunkMeta tracks the assignment of a single chunk
type ChunkMeta struct {
	ChunkID      string   `json:"chunk_id"`
	ExpectedHash string   `json:"expected_hash"`
	Peers        []string `json:"peers"`       // List of peer URLs assigned to host this chunk
	AckedPeers   []string `json:"acked_peers"` // Subset of Peers that confirmed (put/ack) they hold the chunk
}

// FileMeta tracks the chunks belonging to a logical file
type FileMeta struct {
	FileID      string       `json:"file_id"`
	OwnerUserID string       `json:"owner_user_id"` // dueño del archivo: los peers guardan los chunks en disco como dueño/archivo/índice
	LogicalPath string       `json:"logical_path"`
	ChunkCount  int          `json:"chunk_count"`
	Chunks      []*ChunkMeta `json:"chunks"`
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

// GetAllFiles returns copies of all registered files
func (s *FileStore) GetAllFiles() []*FileMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*FileMeta, 0, len(s.byFile))
	for _, file := range s.byFile {
		fileCopy := *file
		fileCopy.Chunks = make([]*ChunkMeta, len(file.Chunks))
		for i, chunk := range file.Chunks {
			chunkCopy := *chunk
			chunkCopy.Peers = slices.Clone(chunk.Peers)
			chunkCopy.AckedPeers = slices.Clone(chunk.AckedPeers)
			fileCopy.Chunks[i] = &chunkCopy
		}
		result = append(result, &fileCopy)
	}
	return result
}

// ImportFiles replaces all the file metadata with the given files, to load the state from disk
// (to save it, GetAllFiles already returns a copy of everything)
func (s *FileStore) ImportFiles(files []*FileMeta) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.byFile = make(map[string]*FileMeta, len(files))
	for _, file := range files {
		s.byFile[file.FileID] = file
	}
}

// AddPeerAssignment registers an additional peer as an assigned replica for a chunk
func (s *FileStore) AddPeerAssignment(chunkID, peerURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, file := range s.byFile {
		for _, chunk := range file.Chunks {
			if chunk.ChunkID == chunkID {
				if !slices.Contains(chunk.Peers, peerURL) {
					chunk.Peers = append(chunk.Peers, peerURL)
				}
				return
			}
		}
	}
}

// GetFile retrieves metadata for a specific file
// It returns a copy, so the caller can read it while AckChunk keeps updating the store
func (s *FileStore) GetFile(fileID string) (*FileMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	file, ok := s.byFile[fileID]
	if !ok {
		return nil, false
	}

	fileCopy := *file
	fileCopy.Chunks = make([]*ChunkMeta, len(file.Chunks))
	for i, chunk := range file.Chunks {
		chunkCopy := *chunk
		chunkCopy.Peers = slices.Clone(chunk.Peers)
		chunkCopy.AckedPeers = slices.Clone(chunk.AckedPeers)
		fileCopy.Chunks[i] = &chunkCopy
	}
	return &fileCopy, true
}

// RemoveFile deletes the metadata of a file and returns it
// The bool is false if the file_id was not registered
func (s *FileStore) RemoveFile(fileID string) (*FileMeta, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	file, ok := s.byFile[fileID]
	delete(s.byFile, fileID)
	return file, ok
}

// AckChunk confirms that an assigned peer successfully received and saved a chunk
// and records the confirmation in AckedPeers (idempotent: a repeated ACK is a no-op)
func (s *FileStore) AckChunk(chunkID, peerURL string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// chunkID format: {file_id}_{index}
	for _, file := range s.byFile {
		for _, chunk := range file.Chunks {
			if chunk.ChunkID == chunkID {
				// Verify the peerURL is in the list of assigned peers for this chunk
				for _, assignedURL := range chunk.Peers {
					if assignedURL == peerURL {
						if !slices.Contains(chunk.AckedPeers, peerURL) {
							chunk.AckedPeers = append(chunk.AckedPeers, peerURL)
						}
						return nil // Match found, valid ACK
					}
				}
				return ErrPeerNotAssigned
			}
		}
	}
	return ErrChunkNotFound
}

// SyncPeerChunks uses the chunk list of a heartbeat as the truth about what a peer holds:
// every known chunk the peer reports gets the peer as assigned and confirmed (AckedPeers),
// and every chunk it no longer reports stops listing it as confirmed.
// This lets a standby superpeer, which never saw the original ACKs, rebuild AckedPeers
// shortcut: chunks the superpeer does not know (orphans) are ignored, answer them with delete tasks later
func (s *FileStore) SyncPeerChunks(peerURL string, chunkIDs []string) {
	held := make(map[string]bool, len(chunkIDs))
	for _, chunkID := range chunkIDs {
		held[chunkID] = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, file := range s.byFile {
		for _, chunk := range file.Chunks {
			if held[chunk.ChunkID] {
				if !slices.Contains(chunk.Peers, peerURL) {
					chunk.Peers = append(chunk.Peers, peerURL)
				}
				if !slices.Contains(chunk.AckedPeers, peerURL) {
					chunk.AckedPeers = append(chunk.AckedPeers, peerURL)
				}
			} else {
				chunk.AckedPeers = slices.DeleteFunc(chunk.AckedPeers, func(u string) bool { return u == peerURL })
			}
		}
	}
}

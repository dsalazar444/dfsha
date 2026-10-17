// Package files handles the logic for coordinating file uploads and downloads

package files

import (
	"dfsha/internal/store"
	"errors"
	"fmt"
	"log"

	"github.com/google/uuid"
)

var (
	ErrInsufficientPeers = errors.New("not enough healthy peers to meet replication factor")
	ErrInvalidChunk      = errors.New("invalid chunk or peer")
)

// Service manages file operations and chunk assignments
type Service struct {
	fsStore    *store.FsStore
	fileStore  *store.FileStore
	peerStore  *store.PeerStore
	replFactor int
	deleter    ChunkDeleter
}

// NewService creates a new files Service
func NewService(fsStore *store.FsStore, fileStore *store.FileStore, peerStore *store.PeerStore, replFactor int, deleter ChunkDeleter) *Service {
	return &Service{
		fsStore:    fsStore,
		fileStore:  fileStore,
		peerStore:  peerStore,
		replFactor: replFactor,
		deleter:    deleter,
	}
}

// Assignment represents the peer assignment for a specific chunk
type Assignment struct {
	ChunkID          string   `json:"chunk_id"`
	DestinationPeers []string `json:"destination_peers"`
}

// InitPut starts a file upload by generating assignments for each chunk
func (s *Service) InitPut(userID, logicalPath string, chunkCount int, hashes []string) (string, []Assignment, error) {
	// Get healthy peers
	healthyPeers := s.peerStore.GetHealthyPeers()
	if len(healthyPeers) == 0 {
		return "", nil, ErrInsufficientPeers
	}

	// Calculate actual replication factor (cap it if not enough peers)
	actualRepl := s.replFactor
	if len(healthyPeers) < actualRepl {
		actualRepl = len(healthyPeers)
	}

	//Generate new File ID and link it in the user's filesystem tree
	fileID := uuid.New().String()
	err := s.fsStore.CreateFile(userID, logicalPath, fileID)
	if err != nil {
		return "", nil, fmt.Errorf("creating logical file: %w", err)
	}

	// Create FileMeta and assign peers round-robin
	fileMeta := &store.FileMeta{
		FileID:      fileID,
		OwnerUserID: userID,
		LogicalPath: logicalPath,
		ChunkCount:  chunkCount,
		Chunks:      make([]*store.ChunkMeta, chunkCount),
	}

	assignments := make([]Assignment, chunkCount)
	peerIndex := 0

	for i := 0; i < chunkCount; i++ {
		chunkID := fmt.Sprintf("%s_%d", fileID, i)
		hash := ""
		if i < len(hashes) {
			hash = hashes[i]
		}

		// Pick `actualRepl` peers sequentially
		assignedPeers := make([]string, actualRepl)
		for r := 0; r < actualRepl; r++ {
			p := healthyPeers[peerIndex%len(healthyPeers)]
			assignedPeers[r] = p.URL()
			peerIndex++
		}

		fileMeta.Chunks[i] = &store.ChunkMeta{
			ChunkID:      chunkID,
			ExpectedHash: hash,
			Peers:        assignedPeers,
		}

		assignments[i] = Assignment{
			ChunkID:          chunkID,
			DestinationPeers: assignedPeers,
		}
	}

	// Save to FileStore
	s.fileStore.AddFile(fileMeta)

	return fileID, assignments, nil
}

// AckPut confirms that a chunk was successfully received by a peer
func (s *Service) AckPut(chunkID, peerID string) error {
	// Resolve peerID to its URL using PeerStore
	peerURL := s.peerStore.GetPeerURL(peerID)
	if peerURL == "" {
		return errors.New("invalid peer: peer not registered")
	}

	// Acknowledge and validate that this peerURL was actually assigned to the chunk
	err := s.fileStore.AckChunk(chunkID, peerURL)
	if err != nil {
		if errors.Is(err, store.ErrPeerNotAssigned) {
			return errors.New("forbidden: peer is not assigned to this chunk")
		}
		return ErrInvalidChunk
	}
	return nil
}

// ChunkLocation tells where a chunk can be downloaded from
type ChunkLocation struct {
	ChunkID string   `json:"chunk_id"`
	Hash    string   `json:"hash"`
	Peers   []string `json:"peers"`
}

// Get resolves a logical path to its file_id and the location of each chunk
// Only peers that confirmed the chunk (ACK) and are currently healthy are listed
func (s *Service) Get(userID, logicalPath string) (string, []ChunkLocation, error) {
	fileID, err := s.fsStore.Lookup(userID, logicalPath)
	if err != nil {
		return "", nil, err
	}

	file, ok := s.fileStore.GetFile(fileID)
	if !ok {
		return "", nil, store.ErrFileNotFound
	}

	healthyURLs := make(map[string]bool)
	for _, p := range s.peerStore.GetHealthyPeers() {
		healthyURLs[p.URL()] = true
	}

	locations := make([]ChunkLocation, len(file.Chunks))
	for i, chunk := range file.Chunks {
		peers := make([]string, 0, len(chunk.AckedPeers))
		for _, peerURL := range chunk.AckedPeers {
			if healthyURLs[peerURL] {
				peers = append(peers, peerURL)
			}
		}
		locations[i] = ChunkLocation{ChunkID: chunk.ChunkID, Hash: chunk.ExpectedHash, Peers: peers}
	}

	return fileID, locations, nil
}

// Delete removes the file from the user's tree right away (logical deletion)
// and asks the peers to drop its chunks in the background (eventual deletion)
func (s *Service) Delete(userID, logicalPath string) error {
	fileID, err := s.fsStore.RemoveFile(userID, logicalPath)
	if err != nil {
		return err
	}

	s.deleteFileData(userID, fileID)
	return nil
}

// DeleteFiles deletes the data of several files that were already removed from the tree (used by rmdir)
func (s *Service) DeleteFiles(userID string, fileIDs []string) {
	for _, fileID := range fileIDs {
		s.deleteFileData(userID, fileID)
	}
}

// deleteFileData drops the metadata of a file and asks the peers to delete its chunks in the background
func (s *Service) deleteFileData(userID, fileID string) {
	if file, ok := s.fileStore.RemoveFile(fileID); ok {
		go s.deleteChunks(userID, file)
	}
}

// deleteChunks asks every peer assigned to a chunk to delete it, not only the ones
// that confirmed it, so a chunk whose ACK was lost is cleaned up too
func (s *Service) deleteChunks(ownerUserID string, file *store.FileMeta) {
	for _, chunk := range file.Chunks {
		for _, peerURL := range chunk.Peers {
			if err := s.deleter.DeleteChunk(peerURL, chunk.ChunkID, ownerUserID); err != nil {
				log.Printf("[files] deleting chunk %s at %s: %v", chunk.ChunkID, peerURL, err)
			}
		}
	}
}

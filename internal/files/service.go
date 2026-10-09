// Package files handles the logic for coordinating file uploads and downloads

package files

import (
	"dfsha/internal/store"
	"errors"
	"fmt"

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
}

// NewService creates a new files Service
func NewService(fsStore *store.FsStore, fileStore *store.FileStore, peerStore *store.PeerStore, replFactor int) *Service {
	return &Service{
		fsStore:    fsStore,
		fileStore:  fileStore,
		peerStore:  peerStore,
		replFactor: replFactor,
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
			// Form URL using address and port
			peerURL := fmt.Sprintf("http://%s:%s", p.Address, p.Port)
			assignedPeers[r] = peerURL
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

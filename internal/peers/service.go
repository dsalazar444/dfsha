// Package peers handles peer registration, heartbeats, and health monitoring
package peers

import (
	"context"
	"dfsha/internal/store"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
)

var ErrPeerNotFound = errors.New("peer not found")

// ReplicationTask instructs a peer to pull a chunk from a healthy replica peer
type ReplicationTask struct {
	Action        string `json:"action"` // "replicate"
	ChunkID       string `json:"chunk_id"`
	SourcePeerURL string `json:"source_peer_url"`
	ExpectedHash  string `json:"expected_hash"`
}

// Service manages peer logic, health monitoring, and re-replication task dispatching
type Service struct {
	store      *store.PeerStore
	fileStore  *store.FileStore
	timeout    time.Duration
	replFactor int
}

// NewService creates a new peers Service
func NewService(store *store.PeerStore, fileStore *store.FileStore, timeout time.Duration, replFactor int) *Service {
	return &Service{
		store:      store,
		fileStore:  fileStore,
		timeout:    timeout,
		replFactor: replFactor,
	}
}

// Register creates a new Peer with a unique ID and adds it to the store
func (s *Service) Register(address, port string) string {
	peerID := uuid.New().String()
	p := &store.Peer{
		PeerID:    peerID,
		Address:   address,
		Port:      port,
		LastSeen:  time.Now(),
		IsHealthy: true, // Initially healthy upon registration
	}
	s.store.Add(p)
	log.Printf("[peers] Registered new peer %s at %s:%s", peerID, address, port)
	return peerID
}

// Heartbeat processes an incoming heartbeat from a peer and returns any pending replication tasks
func (s *Service) Heartbeat(peerID, availableSpace string, chunks []string) ([]ReplicationTask, error) {
	ok := s.store.UpdateHeartbeat(peerID, availableSpace, chunks)
	if !ok {
		return nil, ErrPeerNotFound
	}

	tasks := s.checkReplicationTasks(peerID)
	return tasks, nil
}

// checkReplicationTasks inspects all chunks in the system
// If any chunk has fewer healthy replicas than replFactor, and the current peer does not hold or is not assigned to it,
// assign this peer to pull the chunk from an available healthy replica
func (s *Service) checkReplicationTasks(peerID string) []ReplicationTask {
	if s.fileStore == nil || s.replFactor <= 1 {
		return nil
	}

	peerURL := s.store.GetPeerURL(peerID)
	if peerURL == "" {
		return nil
	}

	// Map of healthy peer URLs
	healthyURLs := make(map[string]bool)
	for _, p := range s.store.GetHealthyPeers() {
		healthyURLs[p.URL()] = true
	}

	var tasks []ReplicationTask
	files := s.fileStore.GetAllFiles()

	for _, file := range files {
		for _, chunk := range file.Chunks {
			// Count confirmed (ACKed) replicas that are currently healthy
			var healthyAckedReplicas []string
			callerAlreadyAcked := false

			for _, pURL := range chunk.AckedPeers {
				if pURL == peerURL {
					callerAlreadyAcked = true
				}
				if healthyURLs[pURL] {
					healthyAckedReplicas = append(healthyAckedReplicas, pURL)
				}
			}

			// If caller already confirmed having this chunk, no task needed for them
			if callerAlreadyAcked {
				continue
			}

			// If the chunk is under-replicated (fewer healthy confirmed copies than replFactor),
			// and there is at least one healthy confirmed copy to download from:
			if len(healthyAckedReplicas) > 0 && len(healthyAckedReplicas) < s.replFactor {
				sourceURL := healthyAckedReplicas[0]

				// Ensure this peer is authorized in Peers so when it finishes it can put/ack it
				s.fileStore.AddPeerAssignment(chunk.ChunkID, peerURL)

				task := ReplicationTask{
					Action:        "replicate",
					ChunkID:       chunk.ChunkID,
					SourcePeerURL: sourceURL,
					ExpectedHash:  chunk.ExpectedHash,
				}
				tasks = append(tasks, task)

				// Limit to at most 3 tasks per heartbeat to avoid overwhelming the peer
				if len(tasks) >= 3 {
					return tasks
				}
			}
		}
	}

	return tasks
}

// StartHealthMonitor periodically sweeps the PeerStore to mark unresponsive
// peers as unhealthy. Stops when ctx is canceled
func (s *Service) StartHealthMonitor(ctx context.Context) {
	// Check twice as often as the timeout to catch failures promptly
	tickerInterval := s.timeout / 2
	if tickerInterval < 1*time.Second {
		tickerInterval = 1 * time.Second
	}

	ticker := time.NewTicker(tickerInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("[peers] Health monitor stopped")
				return
			case <-ticker.C:
				marked := s.store.SweepHealth(s.timeout)
				if marked > 0 {
					log.Printf("[peers] Health monitor marked %d peer(s) as unhealthy", marked)
				}
			}
		}
	}()
}

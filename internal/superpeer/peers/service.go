// Package peers handles peer registration, heartbeats, and health monitoring
package peers

import (
	"context"
	"dfsha/internal/superpeer/store"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
)

var ErrPeerNotFound = errors.New("peer not found")

// Acciones posibles de una tarea (mismos valores que model.ReplicationAction del peer)
const (
	ActionGet    = "GET"    // replicar: bajar el chunk de alguna de las SourceURLs y guardarlo
	ActionDelete = "DELETE" // borrar el chunk del disco del peer
)

// ReplicationTask es una orden que el peer recibe en la respuesta de su heartbeat.
// El formato es el del contrato (superpeer-openapi.yaml) y el de model.ReplicationTask del peer
type ReplicationTask struct {
	Action       string   `json:"action"`        // ActionGet o ActionDelete
	ChunkID      string   `json:"chunk_id"`      // chunk sobre el que actuar
	OwnerUserID  string   `json:"owner_user_id"` // el peer lo necesita para armar la ruta en disco
	SourceURLs   []string `json:"source_urls"`   // GET: peers de donde bajarlo (en orden de preferencia); DELETE: vacío
	ExpectedHash string   `json:"expected_hash"` // GET: hash para verificar lo descargado; DELETE: vacío
}

// Service manages peer logic, health monitoring, and re-replication task dispatching
type Service struct {
	store      *store.PeerStore
	fileStore  *store.FileStore
	taskStore  *store.TaskStore // tareas de borrado pendientes que se entregan en el heartbeat
	timeout    time.Duration
	replFactor int
}

// NewService creates a new peers Service
func NewService(store *store.PeerStore, fileStore *store.FileStore, taskStore *store.TaskStore, timeout time.Duration, replFactor int) *Service {
	return &Service{
		store:      store,
		fileStore:  fileStore,
		taskStore:  taskStore,
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

// Heartbeat processes an incoming heartbeat from a peer and returns its pending tasks:
// first the deletions, then the replications
func (s *Service) Heartbeat(peerID string, availableSpace int64, chunks []string) ([]ReplicationTask, error) {
	ok := s.store.UpdateHeartbeat(peerID, availableSpace, chunks)
	if !ok {
		return nil, ErrPeerNotFound
	}
	peerURL := s.store.GetPeerURL(peerID)

	// 1. La lista de chunks del heartbeat es la verdad sobre lo que tiene el peer:
	//    actualiza AckedPeers antes de decidir la replicación
	if s.fileStore != nil {
		s.fileStore.SyncPeerChunks(peerURL, chunks)
	}

	// 2. Borrados: los que el peer ya cumplió (el chunk dejó de aparecer) se quitan,
	//    y los que siguen pendientes se le vuelven a mandar
	var tasks []ReplicationTask
	if s.taskStore != nil {
		s.taskStore.CompleteDeletes(peerURL, chunks)
		for _, del := range s.taskStore.PendingDeletes(peerURL) {
			tasks = append(tasks, ReplicationTask{
				Action:      ActionDelete,
				ChunkID:     del.ChunkID,
				OwnerUserID: del.OwnerUserID,
				SourceURLs:  []string{}, // el contrato lo pide siempre; en un borrado va vacío
			})
		}
	}

	// 3. Replicación (lógica de Sahian): chunks con menos réplicas sanas de las necesarias
	tasks = append(tasks, s.checkReplicationTasks(peerID)...)
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
				
				// Ensure this peer is authorized in Peers so when it finishes it can put/ack it
				s.fileStore.AddPeerAssignment(chunk.ChunkID, peerURL)

				// Todas las réplicas sanas van como fuentes: el peer prueba una por una hasta que alguna responda
				task := ReplicationTask{
					Action:       ActionGet,
					ChunkID:      chunk.ChunkID,
					OwnerUserID:  file.OwnerUserID,
					SourceURLs:   healthyAckedReplicas,
					ExpectedHash: chunk.ExpectedHash,
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

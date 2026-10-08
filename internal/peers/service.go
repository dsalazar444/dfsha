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

// Service manages peer logic and health monitoring
type Service struct {
	store   *store.PeerStore
	timeout time.Duration
}

// NewService creates a new peers Service
func NewService(store *store.PeerStore, timeout time.Duration) *Service {
	return &Service{
		store:   store,
		timeout: timeout,
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

// Heartbeat processes an incoming heartbeat from a peer
func (s *Service) Heartbeat(peerID, availableSpace string, chunks []string) error {
	ok := s.store.UpdateHeartbeat(peerID, availableSpace, chunks)
	if !ok {
		return ErrPeerNotFound
	}
	return nil
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

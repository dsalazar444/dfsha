// Package store holds in-memory data stores for the superpeer
package store

import (
	"sync"
	"time"
)

// Peer represents a storage node registered with the superpeer
type Peer struct {
	PeerID         string
	Address        string
	Port           string
	AvailableSpace string
	Chunks         []string
	LastSeen       time.Time
	IsHealthy      bool
}

// PeerStore is a thread-safe in-memory collection of peers
type PeerStore struct {
	mu      sync.RWMutex
	byID    map[string]*Peer
}

// NewPeerStore creates a new empty PeerStore
func NewPeerStore() *PeerStore {
	return &PeerStore{
		byID: make(map[string]*Peer),
	}
}

// Add registers a new peer in the store
func (s *PeerStore) Add(p *Peer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[p.PeerID] = p
}

// UpdateHeartbeat refreshes the LastSeen timestamp and updates peer state
// Returns false if the peer does not exist
func (s *PeerStore) UpdateHeartbeat(peerID, availableSpace string, chunks []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.byID[peerID]
	if !ok {
		return false
	}

	p.AvailableSpace = availableSpace
	p.Chunks = chunks
	p.LastSeen = time.Now()
	p.IsHealthy = true // A heartbeat means it's alive
	return true
}

// SweepHealth checks all peers against the timeout
// Marks peers as IsHealthy = false if they haven't sent a heartbeat recently
// Returns the number of peers marked as unhealthy in this sweep
func (s *PeerStore) SweepHealth(timeout time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	marked := 0

	for _, p := range s.byID {
		if p.IsHealthy && now.Sub(p.LastSeen) > timeout {
			p.IsHealthy = false
			marked++
		}
	}
	return marked
}

// GetHealthyPeers returns a list of peers currently marked as healthy
// Will be used in Phase 4 for chunk assignment
func (s *PeerStore) GetHealthyPeers() []*Peer {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var healthy []*Peer
	for _, p := range s.byID {
		if p.IsHealthy {
			// Return a shallow copy so the caller doesn't accidentally mutate the store's pointers
			pCopy := *p
			healthy = append(healthy, &pCopy)
		}
	}
	return healthy
}

// GetPeerURL returns the formatted URL (http://address:port) for a given peerID
// Returns empty string if the peer is not found
func (s *PeerStore) GetPeerURL(peerID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	if p, ok := s.byID[peerID]; ok {
		return "http://" + p.Address + ":" + p.Port
	}
	return ""
}

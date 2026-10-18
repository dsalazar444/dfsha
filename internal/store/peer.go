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
	AvailableSpace int64 // free bytes reported in the last heartbeat
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
func (s *PeerStore) UpdateHeartbeat(peerID string, availableSpace int64, chunks []string) bool {
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

// PeerRecord is the part of a peer that is worth saving: who it is and where to reach it
// The rest (free space, chunks, health) is rebuilt by the next heartbeats
type PeerRecord struct {
	PeerID  string `json:"peer_id"`
	Address string `json:"address"`
	Port    string `json:"port"`
}

// Export returns the identity of every registered peer, to save the state to disk
func (s *PeerStore) Export() []PeerRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()

	records := make([]PeerRecord, 0, len(s.byID))
	for _, p := range s.byID {
		records = append(records, PeerRecord{PeerID: p.PeerID, Address: p.Address, Port: p.Port})
	}
	return records
}

// Import replaces all the peers with the given ones, to load the state from disk
// They start as unhealthy: a peer only counts as alive after its first heartbeat,
// so a peer that keeps its peer_id does not need to register again after a restart or a failover
func (s *PeerStore) Import(records []PeerRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.byID = make(map[string]*Peer, len(records))
	for _, r := range records {
		s.byID[r.PeerID] = &Peer{PeerID: r.PeerID, Address: r.Address, Port: r.Port}
	}
}

// URL returns the base URL (http://address:port) where the peer can be reached
// It is the only place that builds peer URLs (switch to https here when TLS arrives)
func (p *Peer) URL() string {
	return "http://" + p.Address + ":" + p.Port
}

// GetPeerURL returns the formatted URL (http://address:port) for a given peerID
// Returns empty string if the peer is not found
func (s *PeerStore) GetPeerURL(peerID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if p, ok := s.byID[peerID]; ok {
		return p.URL()
	}
	return ""
}

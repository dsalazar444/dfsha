// Package leader decides which of the two superpeers is the leader (active) and which one is on standby
//
// Both superpeers share a folder (NFS/EFS) with a lease file, leader.json: {"owner": id, "expires_at": time}
//   - The leader rewrites it every renewEvery, pushing expires_at forward
//   - The standby reads it every renewEvery; when the lease expired (the leader died or lost the folder),
//     it writes its own lease, loads the saved state and starts serving
//   - A leader that sees the lease owned by the other one, or cannot renew it in time, steps down to standby
//
// shortcut: this is a lease, not a consensus algorithm (like Raft). It needs the clocks of both machines in sync
// (NTP), and a leader that freezes for longer than the lease could still write once after the other took over
package leader

import (
	"context"
	"dfsha/internal/superpeer/persist"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

const fileName = "leader.json"

// lease is the content of leader.json
type lease struct {
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Elector keeps this superpeer's role up to date
type Elector struct {
	id         string        // SUPERPEER_ID of this superpeer
	path       string        // STATE_DIR/leader.json
	ttl        time.Duration // how long a lease is valid without being renewed
	renewEvery time.Duration // how often the lease is checked / renewed (well below ttl)

	// onPromote runs right before becoming the leader: it loads the state the old leader saved
	// If it fails, this superpeer does not lead (it tries again on the next check)
	onPromote func() error

	isLeader  atomic.Bool // read by every HTTP request, written by the loop
	heldUntil time.Time   // expiry of our last written lease (only used by the loop goroutine)
}

// New creates an Elector for the superpeer id, with the lease file in dir
func New(dir, id string, ttl, renewEvery time.Duration, onPromote func() error) *Elector {
	return &Elector{
		id:         id,
		path:       filepath.Join(dir, fileName),
		ttl:        ttl,
		renewEvery: renewEvery,
		onPromote:  onPromote,
	}
}

// IsLeader reports whether this superpeer is the leader right now
func (e *Elector) IsLeader() bool {
	return e.isLeader.Load()
}

// Run checks the lease once right away (so the role is known before serving requests)
// and then every renewEvery, until ctx is canceled
func (e *Elector) Run(ctx context.Context) {
	e.check()

	ticker := time.NewTicker(e.renewEvery)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.check()
			}
		}
	}()
}

// check reads the lease and acts on it: renew it, take it over, or step down
func (e *Elector) check() {
	now := time.Now()

	current, err := e.read()
	if err != nil {
		log.Printf("[leader] reading lease: %v", err)
		e.stepDownIfLeaseLost(now)
		return
	}

	// Someone else holds a valid lease: we must not lead
	if current.Owner != e.id && now.Before(current.ExpiresAt) {
		if e.isLeader.Load() {
			e.isLeader.Store(false)
			log.Printf("[leader] lease taken by %s — stepping down to standby", current.Owner)
		}
		return
	}

	// The lease is ours or free (missing / expired): write ours, or renew it
	next := lease{Owner: e.id, ExpiresAt: now.Add(e.ttl)}
	if err := e.write(next); err != nil {
		log.Printf("[leader] writing lease: %v", err)
		e.stepDownIfLeaseLost(now)
		return
	}

	if !e.isLeader.Load() {
		if !e.confirmAndPromote() {
			return
		}
	}
	e.heldUntil = next.ExpiresAt
}

// confirmAndPromote turns this superpeer into the leader after taking a free lease
// Both superpeers could see the lease free and write at the same time; the last rename wins,
// so after a short wait we read it again and only the one that finds its own id goes on
func (e *Elector) confirmAndPromote() bool {
	time.Sleep(e.renewEvery / 4)
	current, err := e.read()
	if err != nil || current.Owner != e.id {
		return false
	}

	if err := e.onPromote(); err != nil {
		log.Printf("[leader] cannot load the saved state, not taking over yet: %v", err)
		return false
	}

	e.isLeader.Store(true)
	log.Printf("[leader] %s is now the LEADER", e.id)
	return true
}

// stepDownIfLeaseLost makes the leader stop serving when it could not renew and its lease ran out:
// the standby may already be taking over, and two leaders would accept different writes
func (e *Elector) stepDownIfLeaseLost(now time.Time) {
	if e.isLeader.Load() && now.After(e.heldUntil) {
		e.isLeader.Store(false)
		log.Printf("[leader] could not renew the lease in time — stepping down to standby")
	}
}

// read returns the current lease; a missing file means nobody holds it (zero lease)
func (e *Elector) read() (lease, error) {
	data, err := os.ReadFile(e.path)
	if errors.Is(err, fs.ErrNotExist) {
		return lease{}, nil
	}
	if err != nil {
		return lease{}, err
	}

	var l lease
	if err := json.Unmarshal(data, &l); err != nil {
		return lease{}, fmt.Errorf("parsing %q: %w", e.path, err)
	}
	return l, nil
}

// write replaces leader.json atomically (same helper used for state.json)
func (e *Elector) write(l lease) error {
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return persist.WriteFileAtomic(e.path, data)
}

// Package store holds in-memory data stores for the superpeer
// Phase 1: user store seeded from constants
// Future phases: peers, filesystem metadata, chunk assignments, locks

package store

import (
	"fmt"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

// User represents an authenticated DFSha user
type User struct {
	UserID       string
	Username     string
	PasswordHash string // bcrypt hash (plaintext is never stored)
}

// UserStore is a thread-safe in-memory collection of users
type UserStore struct {
	mu         sync.RWMutex
	byUsername map[string]*User
}

// NewUserStore creates a UserStore pre-seeded with development users
// Passwords are bcrypt-hashed at startup, plaintext never stays in memory

// To do: replace seed with persistent storage (file / DB) in a future phase
func NewUserStore() *UserStore {
	s := &UserStore{
		byUsername: make(map[string]*User),
	}
	s.seed()
	return s
}

// FindByUsername returns the User for the given username
// Returns nil, false if the username does not exist
func (s *UserStore) FindByUsername(username string) (*User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.byUsername[username]
	return u, ok
}

// seedEntry is a convenience struct used only inside seed()
type seedEntry struct {
	id       string
	username string
	password string // plaintext (only used to generate the hash at startup)
}

// seed populates the store with development users
// Panics on bcrypt failure rather than starting with an empty store
func (s *UserStore) seed() {
	entries := []seedEntry{
		{id: "user-001", username: "alice", password: "alice1234"},
		{id: "user-002", username: "bob", password: "bob1234"},
	}

	for _, e := range entries {
		hash, err := bcrypt.GenerateFromPassword([]byte(e.password), bcrypt.DefaultCost)
		if err != nil {
			panic(fmt.Sprintf("[store] failed to hash password for seed user %q: %v", e.username, err))
		}
		s.byUsername[e.username] = &User{
			UserID:       e.id,
			Username:     e.username,
			PasswordHash: string(hash),
		}
	}
}

// Package store holds in-memory data stores for the superpeer
package store

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrLockConflict = errors.New("file is already locked by another user")
	ErrLockNotFound = errors.New("lock not found or expired")
)

// FileLock tracks an active write lock on a specific file path
type FileLock struct {
	LockID    string    `json:"lock_id"`
	UserID    string    `json:"user_id"`
	Path      string    `json:"path"`
	ExpiresAt time.Time `json:"expires_at"` // hora absoluta: el TTL sigue corriendo aunque el superpeer se reinicie
}

// LockStore tracks write locks in memory
type LockStore struct {
	mu     sync.RWMutex
	byPath map[string]*FileLock // keyed by canonical path: "{userID}:{path}"
	byID   map[string]*FileLock // keyed by lockID
}

// NewLockStore creates a new empty LockStore
func NewLockStore() *LockStore {
	return &LockStore{
		byPath: make(map[string]*FileLock),
		byID:   make(map[string]*FileLock),
	}
}

// key generates the unique composite key for a user's file path
func (s *LockStore) key(userID, logicalPath string) string {
	return userID + ":" + logicalPath
}

// Acquire attempts to acquire a write lock on the specified path for the given TTL
// If already locked by another active lock, it returns ErrLockConflict
// If the existing lock on this path has expired, it is overwritten
func (s *LockStore) Acquire(userID, logicalPath, lockID string, ttl time.Duration) (*FileLock, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := s.key(userID, logicalPath)
	now := time.Now()

	// Check if there is an active non-expired lock
	if existing, ok := s.byPath[k]; ok {
		if now.Before(existing.ExpiresAt) {
			// If already held by the same user, it's still a conflict unless unlocked or if held by another user
			return nil, ErrLockConflict
		}
		// Existing lock expired clean it up
		delete(s.byID, existing.LockID)
		delete(s.byPath, k)
	}

	lock := &FileLock{
		LockID:    lockID,
		UserID:    userID,
		Path:      logicalPath,
		ExpiresAt: now.Add(ttl),
	}

	s.byPath[k] = lock
	s.byID[lockID] = lock

	lockCopy := *lock
	return &lockCopy, nil
}

// Release releases a lock by its lockID and verifies that it belongs to the target path and user
// It returns ErrLockNotFound if the lock does not exist or has expired
func (s *LockStore) Release(userID, logicalPath, lockID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	lock, ok := s.byID[lockID]
	if !ok || time.Now().After(lock.ExpiresAt) {
		return ErrLockNotFound
	}

	if lock.UserID != userID || lock.Path != logicalPath {
		return ErrLockNotFound
	}

	delete(s.byID, lockID)
	delete(s.byPath, s.key(userID, logicalPath))
	return nil
}

// Export returns a copy of the locks that have not expired yet, to save the state to disk
func (s *LockStore) Export() []FileLock {
	s.mu.RLock()
	defer s.mu.RUnlock()

	now := time.Now()
	locks := make([]FileLock, 0, len(s.byID))
	for _, lock := range s.byID {
		if now.Before(lock.ExpiresAt) {
			locks = append(locks, *lock)
		}
	}
	return locks
}

// Import replaces all the locks with the given ones (skipping the expired), to load the state from disk
func (s *LockStore) Import(locks []FileLock) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.byPath = make(map[string]*FileLock, len(locks))
	s.byID = make(map[string]*FileLock, len(locks))

	now := time.Now()
	for _, saved := range locks {
		if now.After(saved.ExpiresAt) {
			continue
		}
		lock := saved
		s.byPath[s.key(lock.UserID, lock.Path)] = &lock
		s.byID[lock.LockID] = &lock
	}
}

// SweepExpired removes all locks that have exceeded their expiration time
func (s *LockStore) SweepExpired() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	expiredCount := 0

	for k, lock := range s.byPath {
		if now.After(lock.ExpiresAt) {
			delete(s.byID, lock.LockID)
			delete(s.byPath, k)
			expiredCount++
		}
	}

	return expiredCount
}

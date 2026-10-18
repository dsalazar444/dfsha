// Package locks handles business logic for file write locks and concurrency control
package locks

import (
	"context"
	"dfsha/internal/superpeer/store"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
)

var (
	ErrFileNotFound = errors.New("file not found")
	ErrAlreadyLocked = errors.New("file is already locked")
	ErrLockNotFound  = errors.New("lock not found or expired")
)

// Service manages write locks on files
type Service struct {
	lockStore *store.LockStore
	fsStore   *store.FsStore
	ttl       time.Duration
}

// NewService creates a new locks Service
func NewService(lockStore *store.LockStore, fsStore *store.FsStore, ttl time.Duration) *Service {
	return &Service{
		lockStore: lockStore,
		fsStore:   fsStore,
		ttl:       ttl,
	}
}

// LockResult returns the details of an acquired lock
type LockResult struct {
	LockID    string
	ExpiresAt time.Time
}

// Lock requests a write lock on a specific file path
func (s *Service) Lock(userID, logicalPath string) (*LockResult, error) {
	// 1. Verify that the file actually exists in the user's filesystem
	_, err := s.fsStore.Lookup(userID, logicalPath)
	if err != nil {
		if errors.Is(err, store.ErrPathNotFound) || errors.Is(err, store.ErrNotADirectory) {
			return nil, ErrFileNotFound
		}
		return nil, err
	}

	// 2. Try to acquire the lock
	lockID := uuid.New().String()
	acquired, err := s.lockStore.Acquire(userID, logicalPath, lockID, s.ttl)
	if err != nil {
		if errors.Is(err, store.ErrLockConflict) {
			return nil, ErrAlreadyLocked
		}
		return nil, err
	}

	return &LockResult{
		LockID:    acquired.LockID,
		ExpiresAt: acquired.ExpiresAt,
	}, nil
}

// Unlock releases an existing write lock on a specific file path
func (s *Service) Unlock(userID, logicalPath, lockID string) error {
	// 1. Verify that the file exists
	_, err := s.fsStore.Lookup(userID, logicalPath)
	if err != nil {
		if errors.Is(err, store.ErrPathNotFound) || errors.Is(err, store.ErrNotADirectory) {
			return ErrFileNotFound
		}
		return err
	}

	// 2. Release lock
	err = s.lockStore.Release(userID, logicalPath, lockID)
	if err != nil {
		if errors.Is(err, store.ErrLockNotFound) {
			return ErrLockNotFound
		}
		return err
	}

	return nil
}

// StartExpirySweeper runs a periodic background task to clean up expired locks
func (s *Service) StartExpirySweeper(ctx context.Context) {
	tickerInterval := 5 * time.Second
	ticker := time.NewTicker(tickerInterval)

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Println("[locks] Expiry sweeper stopped")
				return
			case <-ticker.C:
				swept := s.lockStore.SweepExpired()
				if swept > 0 {
					log.Printf("[locks] Sweeper cleaned up %d expired lock(s)", swept)
				}
			}
		}
	}()
}

// Package auth handles user authentication and session token management
// for the DFSha superpeer

package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"dfsha/internal/store"

	"golang.org/x/crypto/bcrypt"
)

// tokenTTL is how long a session token stays valid after login
const tokenTTL = 24 * time.Hour

// ErrInvalidCredentials is returned by Login when the username or password
// is wrong. A single error value is used for both cases intentionally,
// the caller must not reveal which field was wrong to the HTTP client
var ErrInvalidCredentials = errors.New("invalid username or password")

// session pairs a resolved userID with its expiry timestamp
type session struct {
	userID    string
	expiresAt time.Time
}

// Service manages login and session lifecycle
// It is safe for concurrent use
type Service struct {
	users    *store.UserStore
	mu       sync.RWMutex
	sessions map[string]session // token - session
}

// NewService creates a Service backed by the provided UserStore
func NewService(users *store.UserStore) *Service {
	return &Service{
		users:    users,
		sessions: make(map[string]session),
	}
}

// LoginResult holds the authentication response metadata
type LoginResult struct {
	SessionToken string
	UserID       string
	Salt         string
}

// Login validates credentials and returns a session token, user ID, and salt
func (s *Service) Login(username, password string) (*LoginResult, error) {
	user, ok := s.users.FindByUsername(username)
	if !ok {
		// Run dummy bcrypt hash to prevent timing-based user enumeration
		_ = bcrypt.CompareHashAndPassword(
			[]byte("$2a$10$dummydummydummydummyduu1Xp3k7hXi5MCb7c1IZ6ySB34pqVlrO"),
			[]byte(password),
		)
		return nil, ErrInvalidCredentials
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		return nil, ErrInvalidCredentials
	}

	token, err := generateToken()
	if err != nil {
		return nil, fmt.Errorf("generating session token: %w", err)
	}

	s.mu.Lock()
	s.sessions[token] = session{
		userID:    user.UserID,
		expiresAt: time.Now().Add(tokenTTL),
	}
	s.mu.Unlock()

	return &LoginResult{
		SessionToken: token,
		UserID:       user.UserID,
		Salt:         user.Salt,
	}, nil
}

// UserIDByToken validates the token and returns its userID, or "", false if invalid or expired
func (s *Service) UserIDByToken(token string) (string, bool) {
	s.mu.RLock()
	sess, ok := s.sessions[token]
	s.mu.RUnlock()

	if !ok || time.Now().After(sess.expiresAt) {
		return "", false
	}
	return sess.userID, true
}

// generateToken generates a 64-char hex token from 32 cryptographically random bytes
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("reading random bytes: %w", err)
	}
	return hex.EncodeToString(b), nil
}

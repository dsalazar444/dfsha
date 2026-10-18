// Package auth handles user authentication and session token management
// for the DFSha superpeer

package auth

import (
	"errors"
	"fmt"
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

// Service manages login and session tokens
// Tokens are signed JWTs (see token.go): nothing is kept in memory, so it is safe for concurrent use
// and any superpeer with the same secret accepts them
type Service struct {
	users  *store.UserStore
	secret []byte // SESSION_SECRET, the same on both superpeers
}

// NewService creates a Service backed by the provided UserStore that signs tokens with secret
func NewService(users *store.UserStore, secret []byte) *Service {
	return &Service{
		users:  users,
		secret: secret,
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

	token, err := s.signToken(user.UserID, time.Now())
	if err != nil {
		return nil, fmt.Errorf("generating session token: %w", err)
	}

	return &LoginResult{
		SessionToken: token,
		UserID:       user.UserID,
		Salt:         user.Salt,
	}, nil
}

// UserIDByToken validates the token (signature and expiry) and returns its userID, or "", false if invalid or expired
func (s *Service) UserIDByToken(token string) (string, bool) {
	return s.verifyToken(token, time.Now())
}

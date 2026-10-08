// Package auth provides a minimal stub so the router can compile
// during Phase 0. It will be fully implemented in other phases
package auth

// Service manages user sessions in memory
// Phase 0: always returns false (no valid tokens yet)
type Service struct{}

// NewService creates a new auth.Service
func NewService() *Service {
	return &Service{}
}

// UserIDByToken satisfies the tokenValidator interface required by the router
// Phase 0 stub: always rejects every token until Phase 1 fills this in
func (s *Service) UserIDByToken(_ string) (string, bool) {
	return "", false
}

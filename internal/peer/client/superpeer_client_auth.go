package client

import (
	"context"
	"errors"
	"net/http"

	"dfsha/internal/shared/model"
)

// Login authenticates against the active superpeer (POST /auth/login) and
// keeps the session token inside the client for all later calls.
//
// The response also carries the user ID and the Argon2id salt the caller
// needs to derive the master key locally. The password is not stored: if
// the token expires later, calls fail with ErrUnauthorized and the caller
// must ask for the password again and call Login once more.
func (c *SuperpeerClient) Login(ctx context.Context, username, password string) (model.LoginResponse, error) {
	var resp model.LoginResponse
	// Empty token on purpose: login is the one endpoint that must not
	// send credentials from an older session -> token ""
	err := c.send(ctx, http.MethodPost, "/auth/login", "", model.LoginRequest{
		Username: username,
		Password: password,
	}, &resp)
	if err != nil {
		return model.LoginResponse{}, err // Nos retorna el struct asignado usando las etiquetas json
	}

	// A 200 without these would make every later step fail in confusing
	// ways (no key derivation without salt, no owner without user ID).
	if resp.Token == "" || resp.UserID == "" || resp.Salt == "" {
		return model.LoginResponse{}, errors.New("incomplete login response from superpeer")
	}

	c.setToken(resp.Token)
	return resp, nil
}

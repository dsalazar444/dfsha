// Package auth  session tokens: JWT signed with HMAC-SHA256 (HS256)
//
// The token carries the userID and its expiry, and is signed with SESSION_SECRET. Any superpeer that
// knows the secret can validate it without remembering anything, so a token issued by the leader is
// still valid on the standby after a failover (the peers and clients do not need to log in again)
//
// Only the standard library is used. The tokens are only issued and read by our superpeers,
// so the header is always the same and anything else is rejected

package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// b64 is the encoding JWT uses: base64 for URLs, without "=" padding
var b64 = base64.RawURLEncoding

// jwtHeader is the first part of every token. HS256 is the only algorithm accepted: comparing the whole
// header blocks the classic trick of sending "alg": "none" to skip the signature check
var jwtHeader = b64.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))

// claims is the content of the token (standard JWT names: sub = subject, iat = issued at, exp = expires)
type claims struct {
	Sub string `json:"sub"` // userID
	Iat int64  `json:"iat"` // unix seconds
	Exp int64  `json:"exp"` // unix seconds
}

// signToken creates a token for userID, valid for tokenTTL from now
func (s *Service) signToken(userID string, now time.Time) (string, error) {
	payload, err := json.Marshal(claims{Sub: userID, Iat: now.Unix(), Exp: now.Add(tokenTTL).Unix()})
	if err != nil {
		return "", err
	}

	unsigned := jwtHeader + "." + b64.EncodeToString(payload)
	return unsigned + "." + b64.EncodeToString(s.signature(unsigned)), nil
}

// verifyToken checks the signature and the expiry, and returns the userID inside the token
func (s *Service) verifyToken(token string, now time.Time) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != jwtHeader {
		return "", false
	}

	// hmac.Equal compares in constant time, so the time it takes does not leak how much of the signature matched
	got, err := b64.DecodeString(parts[2])
	if err != nil || !hmac.Equal(got, s.signature(parts[0]+"."+parts[1])) {
		return "", false
	}

	payload, err := b64.DecodeString(parts[1])
	if err != nil {
		return "", false
	}
	var c claims
	if err := json.Unmarshal(payload, &c); err != nil || c.Sub == "" || now.Unix() >= c.Exp {
		return "", false
	}
	return c.Sub, true
}

// signature is the HMAC-SHA256 of data with the shared secret
func (s *Service) signature(data string) []byte {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

package auth

import (
	"dfsha/internal/superpeer/store"
	"strings"
	"testing"
	"time"
)

var (
	sharedSecret = []byte("secreto-compartido-de-32-bytes!!")
	otherSecret  = []byte("otro-secreto-distinto-de-32-byte")
)

// El caso del failover: el token que emite un superpeer lo acepta el otro, si comparten el secreto
func TestTokenFromOneSuperpeerIsValidOnTheOther(t *testing.T) {
	users := store.NewUserStore()
	sp1 := NewService(users, sharedSecret)
	sp2 := NewService(users, sharedSecret)

	result, err := sp1.Login("alice", "alice1234")
	if err != nil {
		t.Fatal(err)
	}

	userID, ok := sp2.UserIDByToken(result.SessionToken)
	if !ok || userID != result.UserID {
		t.Fatalf("sp2 rejected the token from sp1: userID=%q ok=%v", userID, ok)
	}
}

func TestInvalidTokensAreRejected(t *testing.T) {
	s := NewService(store.NewUserStore(), sharedSecret)
	now := time.Now()
	valid, _ := s.signToken("user-001", now)
	parts := strings.Split(valid, ".")

	// Contenido cambiado a mano (otro usuario) sin poder recalcular la firma
	forgedPayload := b64.EncodeToString([]byte(`{"sub":"user-002","iat":0,"exp":9999999999}`))

	// "alg":"none": el truco clásico para que no se revise la firma
	noneHeader := b64.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))

	fromOtherSecret, _ := NewService(store.NewUserStore(), otherSecret).signToken("user-001", now)
	expired, _ := s.signToken("user-001", now.Add(-tokenTTL-time.Minute))

	cases := map[string]string{
		"payload alterado":                parts[0] + "." + forgedPayload + "." + parts[2],
		"alg none sin firma":              noneHeader + "." + parts[1] + ".",
		"firmado con otro secreto":        fromOtherSecret,
		"vencido":                         expired,
		"vacío":                           "",
		"basura":                          "esto-no-es-un-token",
		"token aleatorio (formato viejo)": "a3f1c2d4e5b60718293a4b5c6d7e8f90a3f1c2d4e5b60718293a4b5c6d7e8f90",
	}
	for name, token := range cases {
		if userID, ok := s.UserIDByToken(token); ok {
			t.Errorf("%s: accepted (userID=%q), want rejected", name, userID)
		}
	}

	if userID, ok := s.UserIDByToken(valid); !ok || userID != "user-001" {
		t.Fatalf("valid token rejected: userID=%q ok=%v", userID, ok)
	}
}

func TestLoginStillRejectsWrongPassword(t *testing.T) {
	s := NewService(store.NewUserStore(), sharedSecret)
	if _, err := s.Login("alice", "mala"); err != ErrInvalidCredentials {
		t.Fatalf("got %v, want ErrInvalidCredentials", err)
	}
}

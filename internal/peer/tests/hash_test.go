package hash_test

import (
	"dfsha/internal/peer/hash"
	"testing"
)

func TestCalculateAndVerify(t *testing.T) {
	data := []byte("example encrypted chunk")
	expected := hash.CalculateSHA256(data)

	if len(expected) != 64 {
		t.Fatalf("expected 64 hexadecimal characters, got %d", len(expected))
	}

	if !hash.VerifySHA256(data, expected) {
		t.Fatal("expected matching hash to verify")
	}

	if hash.VerifySHA256([]byte("modified chunk"), expected) {
		t.Fatal("expected modified content to fail verification")
	}
}

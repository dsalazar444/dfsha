package hash

import (
	"testing"
)

func TestCalculateAndVerify(t *testing.T) {
	data := []byte("example encrypted chunk")
	expected := CalculateSHA256(data)

	if len(expected) != 64 {
		t.Fatalf("expected 64 hexadecimal characters, got %d", len(expected))
	}

	if !VerifySHA256(data, expected) {
		t.Fatal("expected matching hash to verify")
	}

	if VerifySHA256([]byte("modified chunk"), expected) {
		t.Fatal("expected modified content to fail verification")
	}
}

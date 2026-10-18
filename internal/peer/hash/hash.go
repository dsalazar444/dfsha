package hash

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// Calculate SHA-256 of received bytes and return hexadecimal hash
func CalculateSHA256(data []byte) string {
	sum := sha256.Sum256(data)        //type:[32]byte
	return hex.EncodeToString(sum[:]) // transforma bytes a su representación hexadecimal
}

// Verify hash received vs hash expected, and return if they match or not.
func VerifySHA256(data []byte, expectedHash string) bool {
	actualHash := CalculateSHA256(data)
	return strings.EqualFold(actualHash, expectedHash)
}

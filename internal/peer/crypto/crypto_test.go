package crypto

import (
	"bytes"
	"testing"
)

// testKey returns a valid file key without running Argon2id, to keep
// the tests that do not care about derivation fast.
func testKey(t *testing.T, fileID string) []byte {
	t.Helper()
	masterKey := bytes.Repeat([]byte{0x42}, KeySize)
	fileKey, err := DeriveFileKey(masterKey, fileID)
	if err != nil {
		t.Fatalf("DeriveFileKey: %v", err)
	}
	return fileKey
}

func TestSealOpenRoundTrip(t *testing.T) {
	fileKey := testKey(t, "file1")

	aad := []byte("file1_0") //associatedData

	// Empty, tiny and multi-block sizes. Verify Seal, Open, and content match
	for _, size := range []int{0, 1, 15, 16, 17, 4096, 1 << 20} {
		plaintext := bytes.Repeat([]byte{0xAB}, size)

		envelope, err := Seal(plaintext, fileKey, aad)
		if err != nil {
			t.Fatalf("size %d: Seal: %v", size, err)
		}
		if want := nonceSize + size + 16; len(envelope) != want {
			t.Fatalf("size %d: envelope is %d bytes, want %d", size, len(envelope), want)
		}

		got, err := Open(envelope, fileKey, aad)
		if err != nil {
			t.Fatalf("size %d: Open: %v", size, err)
		}
		if !bytes.Equal(got, plaintext) {
			t.Fatalf("size %d: round trip changed the data", size)
		}
	}
}

func TestSealUsesFreshNonce(t *testing.T) {
	fileKey := testKey(t, "file1")
	plaintext := []byte("same chunk")

	a, _ := Seal(plaintext, fileKey, nil)
	b, _ := Seal(plaintext, fileKey, nil)

	if bytes.Equal(a[:nonceSize], b[:nonceSize]) {
		t.Fatal("two Seal calls produced the same nonce")
	}
	if bytes.Equal(a, b) {
		t.Fatal("two Seal calls produced identical envelopes")
	}
}

func TestOpenDetectsTampering(t *testing.T) {
	fileKey := testKey(t, "file1")
	envelope, _ := Seal([]byte("secret chunk content"), fileKey, nil)

	// Flipping one bit anywhere (nonce, ciphertext or tag) must fail.
	for i := range envelope {
		tampered := bytes.Clone(envelope)
		tampered[i] ^= 0x01
		if _, err := Open(tampered, fileKey, nil); err == nil {
			t.Fatalf("tampering at byte %d was not detected", i)
		}
	}
}

func TestOpenRejectsWrongKeyAndWrongAssociatedData(t *testing.T) {
	fileKey := testKey(t, "file1")
	otherFileKey := testKey(t, "file2")
	envelope, _ := Seal([]byte("data"), fileKey, []byte("file1_0"))

	// diff filekey, same aad
	if _, err := Open(envelope, otherFileKey, []byte("file1_0")); err == nil {
		t.Error("Open succeeded with a different file key")
	}
	if _, err := Open(envelope, fileKey, []byte("file1_1")); err == nil { // same filekey, diff aad
		t.Error("Open succeeded with a different associated data (chunk swap)")
	}
	if _, err := Open(envelope, fileKey, nil); err == nil { // same filekey, no aad
		t.Error("Open succeeded without the associated data it was sealed with")
	}
}

func TestOpenShortEnvelopeDoesNotPanic(t *testing.T) {
	key := testKey(t, "file1")

	for _, size := range []int{0, 1, nonceSize - 1, nonceSize, nonceSize + 15} {
		if _, err := Open(make([]byte, size), key, nil); err == nil {
			t.Errorf("Open accepted a %d-byte envelope", size)
		}
	}
}

func TestSealOpenRejectBadKeySize(t *testing.T) {
	for _, size := range []int{0, 16, 24, 31, 33} {
		if _, err := Seal([]byte("x"), make([]byte, size), nil); err == nil {
			t.Errorf("Seal accepted a %d-byte key", size)
		}
		if _, err := Open(make([]byte, 64), make([]byte, size), nil); err == nil {
			t.Errorf("Open accepted a %d-byte key", size)
		}
	}
}

func TestDeriveFileKey(t *testing.T) {
	master := bytes.Repeat([]byte{0x42}, KeySize)

	a1, _ := DeriveFileKey(master, "file-a")
	a2, _ := DeriveFileKey(master, "file-a")
	b, _ := DeriveFileKey(master, "file-b")

	// DeriveFilekey es determinista
	if !bytes.Equal(a1, a2) {
		t.Error("same master key and file ID must give the same file key")
	}
	if bytes.Equal(a1, b) {
		t.Error("different file IDs must give different file keys")
	}
	if len(a1) != KeySize {
		t.Errorf("file key is %d bytes, want %d", len(a1), KeySize)
	}
	if _, err := DeriveFileKey(master[:16], "file-a"); err == nil {
		t.Error("accepted a short master key")
	}
	if _, err := DeriveFileKey(master, ""); err == nil {
		t.Error("accepted an empty file ID")
	}
}

func TestDeriveMasterKey(t *testing.T) {
	salt := bytes.Repeat([]byte{0x01}, MinSaltSize)

	k1, err := DeriveMasterKey("correct horse", salt)
	if err != nil {
		t.Fatalf("DeriveMasterKey: %v", err)
	}

	k2, _ := DeriveMasterKey("correct horse", salt)
	k3, _ := DeriveMasterKey("another password", salt)
	k4, _ := DeriveMasterKey("correct horse", bytes.Repeat([]byte{0x02}, MinSaltSize))

	if len(k1) != KeySize {
		t.Errorf("master key is %d bytes, want %d", len(k1), KeySize)
	}
	if !bytes.Equal(k1, k2) {
		t.Error("same password and salt must give the same master key")
	}
	if bytes.Equal(k1, k3) {
		t.Error("different passwords must give different master keys")
	}
	if bytes.Equal(k1, k4) {
		t.Error("different salts must give different master keys")
	}
	if _, err := DeriveMasterKey("", salt); err == nil {
		t.Error("accepted an empty password")
	}
	if _, err := DeriveMasterKey("pw", salt[:MinSaltSize-1]); err == nil {
		t.Error("accepted a short salt")
	}
}

// TestFullChain runs the whole hierarchy the way a peer would:
// password -> master key -> file key -> seal -> open.
func TestFullChain(t *testing.T) {
	salt := bytes.Repeat([]byte{0x07}, MinSaltSize)
	master, _ := DeriveMasterKey("my password", salt)

	fileKey, _ := DeriveFileKey(master, "file-123")
	chunkID := []byte("file-123_0") // es el aad

	envelope, err := Seal([]byte("chunk zero"), fileKey, chunkID)
	if err != nil {
		t.Fatal(err)
	}

	// A second session derives everything again from scratch.
	master2, _ := DeriveMasterKey("my password", salt)
	fileKey2, _ := DeriveFileKey(master2, "file-123")

	got, err := Open(envelope, fileKey2, chunkID)
	if err != nil || string(got) != "chunk zero" {
		t.Fatalf("Open after re-derivation: %q, %v", got, err)
	}
}

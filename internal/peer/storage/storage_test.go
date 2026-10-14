package storage

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"dfsha/internal/shared/model"
)

func TestSaveAndLoad(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	owner := "user123"
	chunkID := model.NewChunkID("thesis", 0)

	// Bytes binarios que representan un envelope cifrado.
	original := []byte{0x00, 0xFF, 0x12, 0x34, 0x80}

	// recordar que acá vamos a guardar, no a leer. Por eso pasamos bytes
	if err := store.Save(owner, chunkID, original); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := store.Load(owner, chunkID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !bytes.Equal(got, original) {
		t.Fatalf("Load() = %v, want %v", got, original)
	}

	expectedPath := filepath.Join(root, owner, "thesis", "0")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("chunk not found at expected path %q: %v",
			expectedPath, err)
	}
}

// fallará
func TestSaveCreatesDirectories(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not-created-yet")
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	// confirmamos resto de flujo
	chunkID := model.NewChunkID("thesis", 0)
	data := []byte("encrypted data")

	if err := store.Save("user123", chunkID, data); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	got, err := store.Load("user123", chunkID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Fatal("loaded data differs from saved data")
	}
}

func TestSaveOverwritesExistingChunk(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	chunkID := model.NewChunkID("thesis", 0)

	if err := store.Save("user123", chunkID, []byte("old")); err != nil {
		t.Fatalf("first Save() error = %v", err)
	}

	if err := store.Save("user123", chunkID, []byte("new")); err != nil {
		t.Fatalf("second Save() error = %v", err)
	}

	got, err := store.Load("user123", chunkID)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if string(got) != "new" {
		t.Fatalf("Load() = %q, want %q", got, "new")
	}
}

func TestLoadMissingChunk(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	chunkID := model.NewChunkID("thesis", 0)

	_, err = store.Load("user123", chunkID)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() error = %v, want os.ErrNotExist", err)
	}
}

func TestDelete(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	chunkID := model.NewChunkID("thesis", 0)

	if err := store.Save("user123", chunkID, []byte("data")); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if !store.Exists("user123", chunkID) {
		t.Fatal("Exists() = false, want true after Save")
	}

	if err := store.Delete("user123", chunkID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	if store.Exists("user123", chunkID) {
		t.Fatal("Exists() = true, want false after Delete")
	}

	_, err = store.Load("user123", chunkID)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Load() error = %v, want os.ErrNotExist", err)
	}
}

func TestDeleteMissingChunkIsSuccessful(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	chunkID := model.NewChunkID("thesis", 0)

	if err := store.Delete("user123", chunkID); err != nil {
		t.Fatalf("Delete() on missing chunk error = %v, want nil", err)
	}
}

func TestExists(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	chunkID := model.NewChunkID("thesis", 0)

	if store.Exists("user123", chunkID) {
		t.Fatal("Exists() = true for a chunk not stored")
	}

	if err := store.Save("user123", chunkID, []byte("data")); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if !store.Exists("user123", chunkID) {
		t.Fatal("Exists() = false for a stored chunk")
	}
}

func TestSaveRejectsInvalidPathComponents(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	tests := []struct {
		name  string
		owner string
		id    model.ChunkID
	}{
		{
			name:  "empty owner",
			owner: "",
			id:    model.NewChunkID("thesis", 0),
		},
		{
			name:  "owner path traversal",
			owner: "../outside",
			id:    model.NewChunkID("thesis", 0),
		},
		{
			name:  "owner contains slash",
			owner: "user/other",
			id:    model.NewChunkID("thesis", 0),
		},
		{
			name:  "file path traversal",
			owner: "user123",
			id:    model.NewChunkID("../outside", 0),
		},
		{
			name:  "file contains invalid characters",
			owner: "user123",
			id:    model.NewChunkID("thesis.txt", 0),
		},
		{
			name:  "malformed chunk ID",
			owner: "user123",
			id:    model.ChunkID("thesis"),
		},
		{
			name:  "negative index",
			owner: "user123",
			id:    model.NewChunkID("thesis", -1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := store.Save(tt.owner, tt.id, []byte("data"))
			if err == nil {
				t.Fatal("Save() error = nil, want validation error")
			}
		})
	}
}

func TestExistsReturnsFalseForInvalidChunkID(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if store.Exists("user123", model.ChunkID("invalid")) {
		t.Fatal("Exists() = true for invalid ChunkID")
	}
}

func TestListChunkIDs(t *testing.T) {
	tmp := t.TempDir()
	store, err := NewStore(tmp)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	chunks := []struct {
		owner string
		id    model.ChunkID
		data  []byte
	}{
		{"user123", model.NewChunkID("thesis", 0), []byte("a")},
		{"user123", model.NewChunkID("thesis", 1), []byte("b")},
		{"user123", model.NewChunkID("notes", 0), []byte("c")},
	}

	for _, chunk := range chunks {
		if err := store.Save(chunk.owner, chunk.id, chunk.data); err != nil {
			t.Fatalf("Save(%q) error = %v", chunk.id, err)
		}
	}

	got, err := store.ListChunkIDs()
	if err != nil {
		t.Fatalf("ListChunkIDs() error = %v", err)
	}

	if len(got) != len(chunks) {
		t.Fatalf("ListChunkIDs() returned %d IDs, want %d: %v",
			len(got), len(chunks), got)
	}

	found := make(map[model.ChunkID]bool)
	for _, sc := range got {
		found[sc.ChunkID] = true
	}

	for _, chunk := range chunks {
		if !found[chunk.id] {
			t.Errorf("ListChunkIDs() missing %q", chunk.id)
		}
	}
}

func TestFreeSpace(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	free, err := store.FreeSpace()
	if err != nil {
		t.Fatalf("FreeSpace() error = %v", err)
	}

	if free < 0 {
		t.Fatalf("FreeSpace() = %d, want non-negative value", free)
	}
}

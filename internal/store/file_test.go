package store

import (
	"errors"
	"slices"
	"testing"
)

func TestAckChunk(t *testing.T) {
	s := NewFileStore()
	chunk := &ChunkMeta{ChunkID: "f1_0", Peers: []string{"http://p1:9000", "http://p2:9000"}}
	s.AddFile(&FileMeta{FileID: "f1", Chunks: []*ChunkMeta{chunk}})

	// Un peer asignado se registra -> y un ACK repetido no lo duplica
	for i := 0; i < 2; i++ {
		if err := s.AckChunk("f1_0", "http://p1:9000"); err != nil {
			t.Fatalf("ack of assigned peer: %v", err)
		}
	}
	if !slices.Equal(chunk.AckedPeers, []string{"http://p1:9000"}) {
		t.Fatalf("AckedPeers = %v, want only p1", chunk.AckedPeers)
	}

	if err := s.AckChunk("f1_0", "http://other:9000"); !errors.Is(err, ErrPeerNotAssigned) {
		t.Fatalf("unassigned peer: got %v, want ErrPeerNotAssigned", err)
	}
	if err := s.AckChunk("nope_0", "http://p1:9000"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("unknown chunk: got %v, want ErrChunkNotFound", err)
	}
}

func TestRmdirReturnsFileIDsInside(t *testing.T) {
	s := NewFsStore()
	for _, dir := range []string{"/docs", "/docs/tesis"} {
		if err := s.Mkdir("u1", dir); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CreateFile("u1", "/docs/a.txt", "fileA"); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateFile("u1", "/docs/tesis/b.txt", "fileB"); err != nil {
		t.Fatal(err)
	}

	fileIDs, err := s.Rmdir("u1", "/docs")
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(fileIDs)
	if !slices.Equal(fileIDs, []string{"fileA", "fileB"}) {
		t.Fatalf("file ids = %v, want fileA and fileB", fileIDs)
	}
}

func TestSyncPeerChunks(t *testing.T) {
	s := NewFileStore()
	chunk0 := &ChunkMeta{ChunkID: "f1_0", Peers: []string{"http://p1:9000"}}
	chunk1 := &ChunkMeta{ChunkID: "f1_1", Peers: []string{"http://p1:9000"}, AckedPeers: []string{"http://p1:9000"}}
	s.AddFile(&FileMeta{FileID: "f1", Chunks: []*ChunkMeta{chunk0, chunk1}})

	// p1 reporta solo f1_0 (y un chunk que el superpeer no conoce)
	s.SyncPeerChunks("http://p1:9000", []string{"f1_0", "huerfano_0"})
	if !slices.Equal(chunk0.AckedPeers, []string{"http://p1:9000"}) {
		t.Fatalf("f1_0 AckedPeers = %v, want p1 (reportado en el heartbeat)", chunk0.AckedPeers)
	}
	if len(chunk1.AckedPeers) != 0 {
		t.Fatalf("f1_1 AckedPeers = %v, want vacío (p1 ya no lo reporta)", chunk1.AckedPeers)
	}

	// Un peer no asignado que reporta el chunk (p. ej. tras un cambio al standby) queda asignado y confirmado
	s.SyncPeerChunks("http://p2:9000", []string{"f1_0"})
	if !slices.Contains(chunk0.Peers, "http://p2:9000") || !slices.Contains(chunk0.AckedPeers, "http://p2:9000") {
		t.Fatalf("p2 should be assigned and confirmed for f1_0: %+v", chunk0)
	}
}

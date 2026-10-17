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

package files

import (
	"dfsha/internal/store"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeDeleter anota cada borrado pedido en vez de llamar a un peer real
type fakeDeleter struct {
	calls chan string
}

func (f fakeDeleter) DeleteChunk(peerURL, chunkID, ownerUserID string) error {
	f.calls <- peerURL + "|" + chunkID + "|" + ownerUserID
	return nil
}

// newTestService crea un Service con dos peers sanos (p1 y p2) y replicación 2
func newTestService(deleter ChunkDeleter) (*Service, *store.PeerStore) {
	peerStore := store.NewPeerStore()
	for _, id := range []string{"p1", "p2"} {
		peerStore.Add(&store.Peer{PeerID: id, Address: "host-" + id, Port: "9000", IsHealthy: true, LastSeen: time.Now().Add(-time.Hour)})
	}
	return NewService(store.NewFsStore(), store.NewFileStore(), peerStore, 2, deleter), peerStore
}

func TestGetOnlyAckedAndHealthyPeers(t *testing.T) {
	s, peerStore := newTestService(nil)
	_, assignments, err := s.InitPut("u1", "/a.txt", 1, []string{"hash0"})
	if err != nil {
		t.Fatal(err)
	}
	chunkID := assignments[0].ChunkID

	// Sin ningún ACK el chunk no tiene peers donde descargarlo
	_, chunks, err := s.Get("u1", "/a.txt")
	if err != nil || len(chunks[0].Peers) != 0 {
		t.Fatalf("before ack: peers = %v, err = %v", chunks[0].Peers, err)
	}

	// Solo p1 confirma: el get devuelve únicamente a p1, con el hash de put/init
	if err := s.AckPut(chunkID, "p1"); err != nil {
		t.Fatal(err)
	}
	_, chunks, _ = s.Get("u1", "/a.txt")
	if !slices.Equal(chunks[0].Peers, []string{"http://host-p1:9000"}) || chunks[0].Hash != "hash0" {
		t.Fatalf("after ack of p1: %+v", chunks[0])
	}

	// Si p1 deja de dar heartbeat, deja de aparecer
	peerStore.SweepHealth(time.Minute)
	_, chunks, _ = s.Get("u1", "/a.txt")
	if len(chunks[0].Peers) != 0 {
		t.Fatalf("unhealthy peer listed: %v", chunks[0].Peers)
	}

	// Otro usuario no ve el archivo
	if _, _, err := s.Get("u2", "/a.txt"); !errors.Is(err, store.ErrPathNotFound) {
		t.Fatalf("other user: got %v, want ErrPathNotFound", err)
	}
}

func TestDeleteRemovesFileAndAsksAssignedPeers(t *testing.T) {
	deleter := fakeDeleter{calls: make(chan string, 4)}
	s, _ := newTestService(deleter)
	if _, _, err := s.InitPut("u1", "/a.txt", 1, []string{"hash0"}); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete("u1", "/a.txt"); err != nil {
		t.Fatal(err)
	}

	// El archivo desaparece del árbol de inmediato
	if _, _, err := s.Get("u1", "/a.txt"); !errors.Is(err, store.ErrPathNotFound) {
		t.Fatalf("after delete: got %v, want ErrPathNotFound", err)
	}
	if err := s.Delete("u1", "/a.txt"); !errors.Is(err, store.ErrPathNotFound) {
		t.Fatalf("second delete: got %v, want ErrPathNotFound", err)
	}

	// El borrado en los peers es en segundo plano: se pide a los dos asignados, con el dueño
	var calls []string
	for i := 0; i < 2; i++ {
		select {
		case c := <-deleter.calls:
			calls = append(calls, c)
		case <-time.After(2 * time.Second):
			t.Fatalf("expected 2 delete calls, got %v", calls)
		}
	}
	slices.Sort(calls)
	for i, peer := range []string{"host-p1", "host-p2"} {
		if !strings.HasPrefix(calls[i], "http://"+peer+":9000|") || !strings.HasSuffix(calls[i], "|u1") {
			t.Fatalf("unexpected call %q", calls[i])
		}
	}
}

func TestDeleteFilesAsksPeersLikeRmdir(t *testing.T) {
	deleter := fakeDeleter{calls: make(chan string, 4)}
	s, _ := newTestService(deleter)
	fileID, _, err := s.InitPut("u1", "/a.txt", 1, []string{"hash0"})
	if err != nil {
		t.Fatal(err)
	}

	// rmdir ya quitó el archivo del árbol; DeleteFiles borra sus datos en los peers
	s.DeleteFiles("u1", []string{fileID})

	for i := 0; i < 2; i++ {
		select {
		case <-deleter.calls:
		case <-time.After(2 * time.Second):
			t.Fatalf("expected 2 delete calls, got %d", i)
		}
	}
}

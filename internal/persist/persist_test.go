package persist

import (
	"context"
	"dfsha/internal/store"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func newStores() Stores {
	return Stores{
		Peers: store.NewPeerStore(),
		Fs:    store.NewFsStore(),
		Files: store.NewFileStore(),
		Locks: store.NewLockStore(),
		Tasks: store.NewTaskStore(),
	}
}

// fill deja algo en cada store, como si el superpeer hubiera trabajado un rato
func fill(t *testing.T, s Stores) {
	t.Helper()

	s.Peers.Import([]store.PeerRecord{{PeerID: "p1", Address: "host1", Port: "9000"}})

	if err := s.Fs.Mkdir("u1", "/docs"); err != nil {
		t.Fatal(err)
	}
	if err := s.Fs.Mkdir("u1", "/vacia"); err != nil { // carpeta sin nada adentro
		t.Fatal(err)
	}
	if err := s.Fs.CreateFile("u1", "/docs/a.txt", "f1"); err != nil {
		t.Fatal(err)
	}

	s.Files.AddFile(&store.FileMeta{
		FileID: "f1", OwnerUserID: "u1", LogicalPath: "/docs/a.txt", ChunkCount: 1,
		Chunks: []*store.ChunkMeta{{
			ChunkID: "f1_0", ExpectedHash: "hash0",
			Peers: []string{"http://host1:9000"}, AckedPeers: []string{"http://host1:9000"},
		}},
	})

	if _, err := s.Locks.Acquire("u1", "/docs/a.txt", "lock1", time.Hour); err != nil {
		t.Fatal(err)
	}
	s.Tasks.AddDelete("http://host1:9000", "old_0", "u1")
}

func TestSaveAndLoadRestoresEverything(t *testing.T) {
	dir := t.TempDir()

	// Primer superpeer: trabaja y guarda
	first := newStores()
	fill(t, first)
	saver, err := New(dir, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := saver.Save(); err != nil {
		t.Fatal(err)
	}

	// "Reinicio" (o el otro superpeer): stores vacíos que cargan el mismo directorio
	second := newStores()
	loader, _ := New(dir, second)
	if err := loader.Load(); err != nil {
		t.Fatal(err)
	}

	if fileID, err := second.Fs.Lookup("u1", "/docs/a.txt"); err != nil || fileID != "f1" {
		t.Fatalf("Lookup = %q, %v; want f1", fileID, err)
	}
	file, ok := second.Files.GetFile("f1")
	if !ok || file.OwnerUserID != "u1" || file.Chunks[0].ExpectedHash != "hash0" ||
		!slices.Equal(file.Chunks[0].AckedPeers, []string{"http://host1:9000"}) {
		t.Fatalf("file = %+v, want the saved metadata with its acked peers", file)
	}
	if _, err := second.Locks.Acquire("u1", "/docs/a.txt", "lock3", time.Hour); !errors.Is(err, store.ErrLockConflict) {
		t.Fatalf("lock after load: got %v, want ErrLockConflict (the lock must survive)", err)
	}
	if pending := second.Tasks.PendingDeletes("http://host1:9000"); len(pending) != 1 || pending[0].ChunkID != "old_0" {
		t.Fatalf("pending deletes = %+v, want old_0", pending)
	}
	// El peer queda registrado (con su peer_id) pero no cuenta como vivo hasta su primer heartbeat
	if second.Peers.GetPeerURL("p1") != "http://host1:9000" {
		t.Fatal("peer p1 should be registered after load")
	}
	if len(second.Peers.GetHealthyPeers()) != 0 {
		t.Fatal("a loaded peer must wait for its first heartbeat to be healthy")
	}

	// Una carpeta vacía guardada tiene que seguir aceptando contenido (su mapa no puede volver nil)
	if err := second.Fs.Mkdir("u1", "/vacia/sub"); err != nil {
		t.Fatalf("mkdir inside a saved empty folder: %v", err)
	}
}

func TestSaveSkipsWriteWhenNothingChanged(t *testing.T) {
	dir := t.TempDir()
	stores := newStores()
	fill(t, stores)
	p, _ := New(dir, stores)

	if err := p.Save(); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(dir, fileName)
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(statePath, old, old); err != nil {
		t.Fatal(err)
	}

	// Mismo estado: no debe reescribir el archivo (el mismo estado da siempre los mismos bytes)
	if err := p.Save(); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(statePath)
	if !info.ModTime().Equal(old) {
		t.Fatal("state.json was rewritten although nothing changed")
	}

	// Un cambio sí se escribe
	stores.Tasks.AddDelete("http://host1:9000", "otro_0", "u1")
	if err := p.Save(); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(statePath)
	if info.ModTime().Equal(old) {
		t.Fatal("state.json was not written after a change")
	}
}

func TestLoadWithoutFileStartsEmpty(t *testing.T) {
	p, _ := New(t.TempDir(), newStores())
	if err := p.Load(); err != nil {
		t.Fatalf("first start (no state.json) must not fail: %v", err)
	}
}

func TestLoadRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, fileName), []byte("{esto no es json"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, _ := New(dir, newStores())
	if err := p.Load(); err == nil {
		t.Fatal("a corrupt state.json must be an error, not an empty start that overwrites it")
	}
}

func TestLoadDropsExpiredLocks(t *testing.T) {
	dir := t.TempDir()
	first := newStores()
	if _, err := first.Locks.Acquire("u1", "/a.txt", "lock1", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	saver, _ := New(dir, first)
	if err := saver.Save(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond) // el superpeer estuvo caído más que el TTL del lock

	second := newStores()
	loader, _ := New(dir, second)
	if err := loader.Load(); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Locks.Acquire("u2", "/a.txt", "lock2", time.Hour); err != nil {
		t.Fatalf("expired lock must not survive the restart: %v", err)
	}
}

func TestRunOnlySavesWhileAllowed(t *testing.T) {
	dir := t.TempDir()
	stores := newStores()
	fill(t, stores)
	p, _ := New(dir, stores)

	// Un standby (canSave = false) nunca escribe, aunque pasen varios intervalos
	ctx, stop := context.WithCancel(context.Background())
	p.Run(ctx, 20*time.Millisecond, func() bool { return false })
	time.Sleep(100 * time.Millisecond)
	stop()
	if _, err := os.Stat(filepath.Join(dir, fileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a standby wrote state.json (err = %v)", err)
	}

	// El líder (canSave = true) sí escribe
	ctx, stop = context.WithCancel(context.Background())
	defer stop()
	p.Run(ctx, 20*time.Millisecond, func() bool { return true })
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, fileName)); err != nil {
		t.Fatalf("the leader did not write state.json: %v", err)
	}
}

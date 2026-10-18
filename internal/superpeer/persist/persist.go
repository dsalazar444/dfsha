// Package persist saves the superpeer's in-memory state to a directory and loads it back at startup
//
// All the state goes in a single file, STATE_DIR/state.json. If STATE_DIR is a shared folder (NFS/EFS),
// a second superpeer can load the same file when it takes over
//
// What is saved: registered peers (only who they are), every user's folder tree, the file metadata
// (chunks, hashes, who holds each chunk), the write locks and the pending chunk deletions
// What is not saved: peer health and free space (rebuilt by the next heartbeats) and the users,
// that are seeded at startup. Sessions need no saving: tokens are signed JWTs any superpeer can validate

package persist

import (
	"bytes"
	"context"
	"dfsha/internal/superpeer/store"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	fileName      = "state.json"
	formatVersion = 1 // bump it if the format of the file changes
)

// Stores groups the stores whose content is saved
type Stores struct {
	Peers *store.PeerStore
	Fs    *store.FsStore
	Files *store.FileStore
	Locks *store.LockStore
	Tasks *store.TaskStore
}

// snapshot is everything that goes into state.json
type snapshot struct {
	Version     int                           `json:"version"`
	Peers       []store.PeerRecord            `json:"peers"`
	FsRoots     map[string]*store.FsNode      `json:"fs_roots"`
	Files       []*store.FileMeta             `json:"files"`
	Locks       []store.FileLock              `json:"locks"`
	DeleteTasks map[string][]store.DeleteTask `json:"delete_tasks"`
}

// Persister saves and loads the state of the stores
type Persister struct {
	path   string
	stores Stores

	mu   sync.Mutex // Save can be called by the autosave loop and by the shutdown at the same time
	last []byte     // content of the last write, to skip writes when nothing changed
}

// New creates a Persister that keeps the state in dir (created if it does not exist)
func New(dir string, stores Stores) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("creating state dir %q: %w", dir, err)
	}
	return &Persister{path: filepath.Join(dir, fileName), stores: stores}, nil
}

// Load fills the stores with the saved state. If there is no file yet (first start) it does nothing
// A file that cannot be read is an error: starting empty would overwrite it on the next save
func (p *Persister) Load() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("reading %q: %w", p.path, err)
	}

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("parsing %q: %w", p.path, err)
	}
	if snap.Version != formatVersion {
		return fmt.Errorf("%q has format version %d, this superpeer reads version %d", p.path, snap.Version, formatVersion)
	}

	p.stores.Peers.Import(snap.Peers)
	p.stores.Fs.Import(snap.FsRoots)
	p.stores.Files.ImportFiles(snap.Files)
	p.stores.Locks.Import(snap.Locks)
	p.stores.Tasks.Import(snap.DeleteTasks)

	p.last = data
	return nil
}

// Save writes the state to disk, unless it is the same as the last write
// The file is written whole to a temporary file and then renamed, so a crash in the middle
// never leaves a half-written state.json
func (p *Persister) Save() error {
	data, err := json.MarshalIndent(p.snapshot(), "", "  ")
	if err != nil {
		return fmt.Errorf("encoding state: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if bytes.Equal(data, p.last) {
		return nil
	}
	if err := WriteFileAtomic(p.path, data); err != nil {
		return err
	}
	p.last = data
	return nil
}

// Run saves the state every interval until ctx is canceled, but only while canSave() is true:
// with two superpeers sharing the folder, only the leader may write, or the standby would
// overwrite the good state with its old copy
// Saving on a timer (and not on every change) keeps the stores unaware of the disk
// shortcut: a crash loses up to one interval of changes; call Save once more on a clean shutdown
func (p *Persister) Run(ctx context.Context, interval time.Duration, canSave func() bool) {
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !canSave() {
					continue
				}
				if err := p.Save(); err != nil {
					log.Printf("[persist] saving state: %v", err)
				}
			}
		}
	}()
}

// snapshot copies the state out of the stores
// Everything is sorted so the same state always gives the same bytes (maps are not ordered)
func (p *Persister) snapshot() snapshot {
	snap := snapshot{
		Version:     formatVersion,
		Peers:       p.stores.Peers.Export(),
		FsRoots:     p.stores.Fs.Export(),
		Files:       p.stores.Files.GetAllFiles(),
		Locks:       p.stores.Locks.Export(),
		DeleteTasks: p.stores.Tasks.Export(),
	}

	slices.SortFunc(snap.Peers, func(a, b store.PeerRecord) int { return strings.Compare(a.PeerID, b.PeerID) })
	slices.SortFunc(snap.Files, func(a, b *store.FileMeta) int { return strings.Compare(a.FileID, b.FileID) })
	slices.SortFunc(snap.Locks, func(a, b store.FileLock) int { return strings.Compare(a.LockID, b.LockID) })
	for _, tasks := range snap.DeleteTasks {
		slices.SortFunc(tasks, func(a, b store.DeleteTask) int { return strings.Compare(a.ChunkID, b.ChunkID) })
	}
	return snap
}

// WriteFileAtomic writes data to a temporary file in the same folder and renames it over path,
// so readers see the old content or the new one, never a half-written file
// (the rename is only atomic inside the same filesystem, that is why the temporary file is not in /tmp)
func WriteFileAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temporary file: %w", err)
	}
	defer os.Remove(tmp.Name()) // does nothing if the rename worked

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temporary file: %w", err)
	}
	// Sync forces the data to disk before the rename, otherwise a crash could leave an empty file
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("syncing temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporary file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("replacing %q: %w", path, err)
	}
	return nil
}

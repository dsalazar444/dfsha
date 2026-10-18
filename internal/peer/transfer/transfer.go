// Package transfer orchestrates the two user-initiated operations that
// move file content: Put (upload) and Get (download).
//
// It is the only place where the other peer packages meet:
//
//	chunker  splits and joins the file
//	crypto   seals and opens each chunk (keys never leave this peer)
//	hash     fingerprints each sealed chunk
//	client   talks to the superpeer (where chunks go / are) and to peers
//
// Trust model, in one paragraph: the superpeer is the source of truth for
// hashes and chunk locations; storage peers are untrusted couriers. So
// every chunk that comes back from a peer is checked against the hash the
// SUPERPEER gave, and then authenticated by AES-GCM, which only the owner
// can do. Nothing a storage peer says is believed.
package transfer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dfsha/internal/peer/crypto"
	"dfsha/internal/shared/model"
)

const (
	// defaultChunkSize is the plaintext size of each chunk. The size is
	// chosen per upload and every chunk is self-describing, so changing
	// it later does not break files that were already uploaded.
	defaultChunkSize = 64 << 20

	// defaultParallelism is how many chunk transfers run at once.
	defaultParallelism = 4

	// maxSendAttempts is how many times one chunk is sent to one peer when
	// the failure looks temporary (see sendWithRetry).
	maxSendAttempts = 3

	// defaultRetryBackoff is the base wait between attempts; it grows
	// linearly (1x, 2x, ...).
	defaultRetryBackoff = 200 * time.Millisecond

	// minReplicas is how many peers must accept a chunk for the upload to
	// count as successful. Missing replicas beyond this are repaired later
	// by the superpeer through replication tasks (see package heartbeat).
	minReplicas = 1
)

// chunkAAD builds the associated data that every chunk is sealed with:
// "{chunkID}/{totalChunks}".
//
// The chunk ID binds a ciphertext to its file and position. The total
// binds it to the file's length too, and that closes a gap: the only
// chunk count a downloader sees comes from the superpeer's list length, so a
// superpeer that dropped the last chunk (or simply had no ack for it yet)
// would otherwise get a silently truncated file accepted. With the total
// authenticated, a shortened list changes the associated data of every
// chunk, so decryption fails instead.
func chunkAAD(id model.ChunkID, total int) []byte {
	return fmt.Appendf(nil, "%s/%d", id, total) // convierte a []byte texto para AAD
}

// ErrEmptyFile is returned by Put for a file with no bytes: there would be
// no chunks to store.
var ErrEmptyFile = errors.New("file is empty")

// SuperpeerAPI is what transfer needs from the superpeer client. Declared
// here, by the consumer, so tests can use a fake. client.SuperpeerClient
// satisfies it.
type SuperpeerAPI interface { // recordar que estas son operaciones hacia SP -> Get es metadata
	PutInit(ctx context.Context, req model.PutInitRequest) (model.PutInitResponse, error)
	GetFile(ctx context.Context, logicalPath string) (model.GetFileResponse, error)
}

// PeerAPI is what transfer needs to talk to other peers. client.PeerClient
// satisfies it.
type PeerAPI interface {
	SendChunk(ctx context.Context, peerURL, ownerUserID string, chunkID model.ChunkID, envelope []byte, expectedHash string) error
	FetchChunk(ctx context.Context, peerURL, ownerUserID string, chunkID model.ChunkID) ([]byte, error)
}

// Config holds the per-session values of a Transfer.
type Config struct {
	// UserID is the logged-in user (from the login response). It is the
	// owner of every chunk this peer uploads or downloads.
	UserID string

	// MasterKey is the Argon2id-derived key of the user. It stays in
	// memory only. Transfer reads it and never modifies or stores it
	// anywhere else.
	MasterKey []byte

	// ChunkSize is the plaintext bytes per chunk. 0 selects the default.
	ChunkSize int

	// Parallelism is how many chunk transfers run at once. 0 selects the
	// default.
	Parallelism int
}

// Transfer performs uploads and downloads for one logged-in user. It is
// safe to use from several goroutines.
type Transfer struct {
	superpeer SuperpeerAPI
	peers     PeerAPI

	userID      string
	masterKey   []byte
	chunkSize   int
	parallelism int

	// backoff is a field (not only a constant) so tests can make retries
	// instant.
	backoff time.Duration
}

// New builds a Transfer, validating the session values up front so that a
// bad configuration fails here and not in the middle of an upload.
func New(cfg Config, superpeer SuperpeerAPI, peers PeerAPI) (*Transfer, error) {
	if cfg.UserID == "" {
		return nil, errors.New("user ID must not be empty")
	}
	if len(cfg.MasterKey) != crypto.KeySize {
		return nil, fmt.Errorf("master key must be %d bytes, got %d", crypto.KeySize, len(cfg.MasterKey))
	}
	if cfg.ChunkSize < 0 || cfg.Parallelism < 0 {
		return nil, errors.New("chunk size and parallelism must not be negative")
	}

	t := &Transfer{
		superpeer:   superpeer,
		peers:       peers,
		userID:      cfg.UserID,
		masterKey:   cfg.MasterKey,
		chunkSize:   cfg.ChunkSize,
		parallelism: cfg.Parallelism,
		backoff:     defaultRetryBackoff,
	}
	if t.chunkSize == 0 {
		t.chunkSize = defaultChunkSize
	}
	if t.parallelism == 0 {
		t.parallelism = defaultParallelism
	}
	return t, nil
}

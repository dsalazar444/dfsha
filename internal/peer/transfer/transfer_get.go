package transfer

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"

	"dfsha/internal/peer/chunker"
	"dfsha/internal/peer/crypto"
	"dfsha/internal/peer/hash"
	"dfsha/internal/shared/model"
)

// Get downloads and decrypts the file at logicalPath.
//
//  1. Ask the superpeer where the chunks are. It answers with the file ID
//     and, per chunk, the peers that confirmed holding it and the hash the
//     owner declared at upload time.
//  2. Validate that answer (see orderChunks).
//  3. Fetch the chunks a few at a time. Each one is checked against the
//     SUPERPEER's hash, then opened with AES-GCM using the file key.
//  4. Join the plaintext chunks in order.
//
// Memory: the whole file is held in RAM while it is rebuilt.
func (t *Transfer) Get(ctx context.Context, logicalPath string) ([]byte, error) {
	resp, err := t.superpeer.GetFile(ctx, logicalPath)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	locations, err := orderChunks(resp)
	if err != nil {
		return nil, err
	}

	fileKey, err := crypto.DeriveFileKey(t.masterKey, resp.FileID)
	if err != nil {
		return nil, err
	}

	// Each goroutine writes only its own index, so no lock is needed.
	plainChunks := make([][]byte, len(locations))
	err = forEach(ctx, len(locations), t.parallelism, func(ctx context.Context, i int) error {
		plain, err := t.fetchChunk(ctx, fileKey, locations[i], len(locations))
		if err != nil {
			return err
		}
		plainChunks[i] = plain
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("downloading %q: %w", logicalPath, err)
	}

	return chunker.Join(plainChunks), nil
}

// fetchChunk gets one chunk from the first peer that serves a valid copy.
//
// total is the number of chunks the file is believed to have. It is part
// of the associated data, so if the superpeer's list was shortened, every
// chunk fails to decrypt (see chunkAAD).
func (t *Transfer) fetchChunk(ctx context.Context, fileKey []byte, loc model.ChunkLocation, total int) ([]byte, error) {
	var causes []error

	for _, peerURL := range shuffled(loc.PeerURLs) {
		envelope, err := t.peers.FetchChunk(ctx, peerURL, t.userID, loc.ChunkID)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			causes = append(causes, fmt.Errorf("peer %s: %w", peerURL, err))
			continue // another replica may be healthy
		}

		// First check: the bytes are what the owner uploaded, according to
		// the superpeer. A mismatch means this replica is corrupt or
		// tampered with; it is not the file's fault, so try another one.
		if !hash.VerifySHA256(envelope, loc.ExpectedHash) {
			causes = append(causes, fmt.Errorf("peer %s: hash mismatch", peerURL))
			continue
		}

		// Second check, cryptographic: only the owner's key opens it, and
		// the associated data proves it is THIS chunk of THIS file, of a
		// file with THIS many chunks. If this fails the hash matched, so
		// every replica holds the same bytes: trying other peers would not
		// help.
		plain, err := crypto.Open(envelope, fileKey, chunkAAD(loc.ChunkID, total))
		if err != nil {
			return nil, fmt.Errorf("chunk %s could not be decrypted (wrong password, an incomplete chunk list from the superpeer, or the file was corrupted before upload): %w",
				loc.ChunkID, err)
		}
		return plain, nil
	}

	return nil, fmt.Errorf("chunk %s: no peer could serve a valid copy: %w", loc.ChunkID, errors.Join(causes...))
}

// orderChunks validates the superpeer's get answer and returns the chunk
// locations sorted by position.
//
// The chunks must be exactly positions 0..n-1 of THIS file, each once,
// each with a hash and at least one peer. Reassembling anything else would
// silently produce a wrong or truncated file.
func orderChunks(resp model.GetFileResponse) ([]model.ChunkLocation, error) {
	if resp.FileID == "" {
		return nil, errors.New("superpeer answered without a file ID")
	}
	n := len(resp.Chunks)
	if n == 0 {
		return nil, errors.New("superpeer reports no chunks for this file")
	}

	ordered := make([]model.ChunkLocation, n)
	filled := make([]bool, n)
	for _, loc := range resp.Chunks {
		fileID, err := loc.ChunkID.FileID()
		if err != nil {
			return nil, err
		}
		if fileID != resp.FileID {
			return nil, fmt.Errorf("chunk %s does not belong to file %s", loc.ChunkID, resp.FileID)
		}
		index, err := loc.ChunkID.Index()
		if err != nil {
			return nil, err
		}
		if index >= n {
			return nil, fmt.Errorf("chunk %s is out of range: the file has %d chunks", loc.ChunkID, n)
		}
		if filled[index] {
			return nil, fmt.Errorf("chunk %s listed twice", loc.ChunkID)
		}
		if loc.ExpectedHash == "" {
			return nil, fmt.Errorf("chunk %s came without a hash", loc.ChunkID)
		}
		if len(loc.PeerURLs) == 0 {
			return nil, fmt.Errorf("chunk %s has no peer holding it (not confirmed yet, or all copies lost)", loc.ChunkID)
		}
		ordered[index] = loc
		filled[index] = true
	}
	// n entries, all in range and without repeats, fill every slot: no
	// gap check is needed.
	return ordered, nil
}

// shuffled returns a copy of urls in random order, so that downloads are
// spread across replicas instead of always hitting the first one.
func shuffled(urls []string) []string {
	out := append([]string(nil), urls...)
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

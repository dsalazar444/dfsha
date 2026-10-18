package transfer

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"dfsha/internal/peer/chunker"
	"dfsha/internal/peer/crypto"
	"dfsha/internal/peer/http_client"

	"dfsha/internal/peer/hash"
	"dfsha/internal/shared/model"
)

// Put uploads data as the file at logicalPath and returns its file ID.
//
// Order matters, and it is dictated by the cryptography:
//
//  1. Generate the file ID HERE. The file key is derived from it, and the
//     hashes sent in put/init are hashes of ciphertext, so the ID has to
//     exist before the superpeer is contacted.
//  2. Derive the file key, split the file, seal every chunk, hash every
//     sealed chunk. Chunk i is sealed with its chunk ID and the
//     total chunk count as associated data (see chunkAAD), which binds the
//     ciphertext to its file, its position and the file's length.
//  3. put/init: the superpeer validates the ID, stores the hashes and says
//     which peers receive each chunk.
//  4. Check that answer (see checkAssignment), then send every chunk to
//     every assigned peer, a few at a time. The receiving peers ack the
//     superpeer themselves.
//
// The upload succeeds when every chunk was accepted by at least
// minReplicas peers. Fewer replicas than assigned is only logged: the
// superpeer notices and schedules the missing copies.
//
// Memory: the whole file, plus its sealed copy, is held in RAM, because
// put/init needs every hash before the first chunk can be sent.
//
// Known gap: if the upload fails halfway, chunks already stored stay
// orphaned on peers. Cleaning them needs an abort endpoint in the
// superpeer (or a decision on what put does to an existing path).
// data contains all the file

func (t *Transfer) Put(ctx context.Context, logicalPath string, data []byte) (string, error) {
	if len(data) == 0 {
		return "", ErrEmptyFile
	}

	fileID, err := newFileID()
	if err != nil {
		return "", err
	}
	// derivamos fileKey
	fileKey, err := crypto.DeriveFileKey(t.masterKey, fileID)
	if err != nil {
		return "", err
	}
	// dividimos data en chunks
	plainChunks := chunker.Split(data, t.chunkSize)
	if len(plainChunks) == 0 {
		return "", ErrEmptyFile
	}

	chunkIDs := make([]model.ChunkID, len(plainChunks))
	envelopes := make([][]byte, len(plainChunks))
	hashes := make([]string, len(plainChunks))

	// calculamos datos para request
	for i, plain := range plainChunks {
		chunkIDs[i] = model.NewChunkID(fileID, i)
		envelope, err := crypto.Seal(plain, fileKey, chunkAAD(chunkIDs[i], len(plainChunks)))

		if err != nil {
			return "", fmt.Errorf("sealing chunk %s: %w", chunkIDs[i], err)
		}
		envelopes[i] = envelope
		// The hash covers the whole envelope (nonce + ciphertext + tag),
		// the exact bytes a storage peer will hold and verify.
		hashes[i] = hash.CalculateSHA256(envelope)
	}

	// mandamos a superpeer
	resp, err := t.superpeer.PutInit(ctx, model.PutInitRequest{
		FileID:       fileID,
		LogicalPath:  logicalPath,
		ChunkCount:   len(plainChunks),
		HashPerChunk: hashes,
	})
	if err != nil {
		return "", fmt.Errorf("put/init: %w", err)
	}
	// obtenemos assigment
	targets, err := checkAssignment(resp, fileID, chunkIDs)
	if err != nil {
		return "", err
	}

	if err := t.sendAll(ctx, chunkIDs, envelopes, hashes, targets); err != nil {
		return "", fmt.Errorf("uploading %q: %w", logicalPath, err)
	}
	return fileID, nil
}

// sendAll sends every chunk to every peer assigned to it and checks that
// each chunk ended up on at least minReplicas peers.
func (t *Transfer) sendAll(ctx context.Context, chunkIDs []model.ChunkID, envelopes [][]byte, hashes []string, targets [][]string) error {
	// One job per (chunk, peer) pair. A flat list lets a single bounded
	// pool limit the total number of simultaneous requests.
	type job struct {
		chunk   int
		peerURL string
	}
	var jobs []job
	for chunk, urls := range targets {
		for _, url := range urls {
			jobs = append(jobs, job{chunk: chunk, peerURL: url})
		}
	}

	var (
		mu     sync.Mutex
		stored = make([]int, len(chunkIDs))     // peers that accepted each chunk
		causes = make([][]error, len(chunkIDs)) // why the others did not
	)
	// The job function never returns an error: a failed replica must not
	// cancel the others, because one healthy replica is enough. Only the
	// parent ctx can stop the whole thing.
	err := forEach(ctx, len(jobs), t.parallelism, func(ctx context.Context, j int) error {
		jb := jobs[j]
		sendErr := t.sendWithRetry(ctx, jb.peerURL, chunkIDs[jb.chunk], envelopes[jb.chunk], hashes[jb.chunk])

		mu.Lock()
		defer mu.Unlock()
		if sendErr == nil {
			stored[jb.chunk]++
		} else {
			causes[jb.chunk] = append(causes[jb.chunk], fmt.Errorf("peer %s: %w", jb.peerURL, sendErr))
		}
		return nil
	})
	if err != nil {
		return err
	}

	var failed []error
	for i, id := range chunkIDs {
		switch {
		case stored[i] < minReplicas:
			failed = append(failed, fmt.Errorf("chunk %s was accepted by 0 of %d peers: %w",
				id, len(targets[i]), errors.Join(causes[i]...)))
		case stored[i] < len(targets[i]):
			log.Printf("chunk %s stored on %d of %d assigned peers; the superpeer will re-replicate",
				id, stored[i], len(targets[i]))
		}
	}
	return errors.Join(failed...)
}

// sendWithRetry sends one chunk to one peer, retrying only when the
// failure looks temporary (peer unreachable, or a 5xx).
//
// It does NOT retry a refusal such as 400 "hash mismatch": the same bytes
// would be refused again, and the contract says to use another peer, which
// sendAll already does by sending to every assigned peer.
func (t *Transfer) sendWithRetry(ctx context.Context, peerURL string, chunkID model.ChunkID, envelope []byte, expectedHash string) error {
	for attempt := 1; ; attempt++ {
		err := t.peers.SendChunk(ctx, peerURL, t.userID, chunkID, envelope, expectedHash)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !http_client.IsUnavailable(err) || attempt == maxSendAttempts {
			return err
		}

		select {
		case <-time.After(t.backoff * time.Duration(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// checkAssignment validates the superpeer's put/init answer and returns,
// for each chunk index, the deduplicated peer URLs to send it to.
//
// The superpeer is trusted for WHERE chunks go, not for what they are
// called: the client already knows the exact chunk IDs it expects, and
// those IDs are the associated data of the encryption. So the answer must
// contain exactly those chunks, each with at least one peer.
func checkAssignment(resp model.PutInitResponse, fileID string, chunkIDs []model.ChunkID) ([][]string, error) {

	// TODO: si fileIDs difieren, retorna error y que caller vuelva a hacer Put, para que se genere otro fileID, y asi
	if resp.FileID != fileID {
		return nil, fmt.Errorf("superpeer answered with file ID %q, expected %q", resp.FileID, fileID)
	}

	byID := make(map[model.ChunkID][]string, len(resp.Assignment)) // creamos mapa para buscar asignaciones por id

	// Detectar chunks duplicados y eliminar URLs repetidas
	for _, a := range resp.Assignment {
		if _, dup := byID[a.ChunkID]; dup {
			return nil, fmt.Errorf("superpeer assigned chunk %s twice", a.ChunkID)
		}
		byID[a.ChunkID] = dedupe(a.PeerURLs)
	}

	// Verificar la cantidad de chunks
	if len(byID) != len(chunkIDs) {
		return nil, fmt.Errorf("superpeer assigned %d chunks, expected %d", len(byID), len(chunkIDs))
	}

	// Construir los destinatarios en el orden esperado
	targets := make([][]string, len(chunkIDs))
	for i, id := range chunkIDs {
		urls, ok := byID[id]
		if !ok {
			return nil, fmt.Errorf("superpeer did not assign chunk %s", id)
		}
		if len(urls) == 0 {
			return nil, fmt.Errorf("superpeer assigned no peers to chunk %s", id)
		}
		// guarda las URLs en la posición correspondiente
		targets[i] = urls
	}
	return targets, nil
}

// dedupe removes repeated and empty URLs, keeping the first occurrence's order.
func dedupe(urls []string) []string {
	seen := make(map[string]struct{}, len(urls))
	out := make([]string, 0, len(urls))
	for _, u := range urls {
		if u == "" {
			continue
		}
		if _, dup := seen[u]; dup {
			continue
		}
		seen[u] = struct{}{}
		out = append(out, u)
	}
	return out
}

// newFileID returns a random UUID (version 4), for example
// "3f2b8c1e-9a4d-4e7b-8c21-5d6f0a1b2c3d".
//
// Its alphabet (hex digits and dashes) is inside what the storage layer
// accepts for a path segment, and it contains no underscore, which matters
// because ChunkID is "{fileID}_{index}" and is split at the first
// underscore.
func newFileID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generating file ID: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

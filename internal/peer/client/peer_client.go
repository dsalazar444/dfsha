package client

import (
	"bytes"
	"context"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"

	"hdfsha/internal/shared/model"
)

// PeerClient makes the outbound calls to OTHER peers' chunk endpoints
// (the inbound side of those endpoints lives in internal/peer/server).
//
// Peer URLs come from the superpeer ("http://host:port"), one per call:
// there is no fixed peer, so the URL is a parameter, not a field.
//
// It does not verify hashes. The caller compares the bytes it gets with
// the hash the SUPERPEER gave it, never with anything the other peer says.
type PeerClient struct {
	http *HTTPClient
}

// NewPeerClient builds a PeerClient on top of a shared HTTPClient.
func NewPeerClient(h *HTTPClient) *PeerClient {
	return &PeerClient{http: h}
}

// SendChunk uploads one chunk envelope to a peer (POST /chunks/receive).
// The receiving peer re-computes the SHA-256 and rejects the chunk with
// 400 if it differs from expectedHash; the caller should then try ANOTHER
// peer from the replica list, not the same one.
func (c *PeerClient) SendChunk(ctx context.Context, peerURL, ownerUserID string, chunkID model.ChunkID, envelope []byte, expectedHash string) error {
	endpoint, err := chunkEndpoint(peerURL, "receive", "")
	if err != nil {
		return err
	}

	// Build the multipart body with the field names the peer expects.
	var buf bytes.Buffer
	form := multipart.NewWriter(&buf)
	for name, value := range map[string]string{
		"chunk_id":      string(chunkID),
		"owner_user_id": ownerUserID,
		"hash_esperado": expectedHash,
	} {
		if err := form.WriteField(name, value); err != nil {
			return fmt.Errorf("building multipart field %q: %w", name, err)
		}
	}
	part, err := form.CreateFormFile("chunk", string(chunkID))
	if err != nil {
		return fmt.Errorf("building multipart file: %w", err)
	}
	if _, err := part.Write(envelope); err != nil {
		return fmt.Errorf("building multipart file: %w", err)
	}
	if err := form.Close(); err != nil {
		return fmt.Errorf("closing multipart body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &buf)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	req.Header.Set("Content-Type", form.FormDataContentType())

	_, err = c.http.Do(req)
	return err
}

// FetchChunk downloads a chunk envelope (GET /chunks/{chunk_id}). The
// bytes are returned exactly as the peer stored them.
func (c *PeerClient) FetchChunk(ctx context.Context, peerURL, ownerUserID string, chunkID model.ChunkID) ([]byte, error) {
	return c.chunkRequest(ctx, http.MethodGet, peerURL, ownerUserID, chunkID)
}

// DeleteChunk asks a peer to delete a chunk (DELETE /chunks/{chunk_id}).
// The peer treats it as idempotent, so repeating it is safe.
func (c *PeerClient) DeleteChunk(ctx context.Context, peerURL, ownerUserID string, chunkID model.ChunkID) error {
	_, err := c.chunkRequest(ctx, http.MethodDelete, peerURL, ownerUserID, chunkID)
	return err
}

// chunkRequest is the shared body of FetchChunk and DeleteChunk: same URL
// shape, only the method differs.
func (c *PeerClient) chunkRequest(ctx context.Context, method, peerURL, ownerUserID string, chunkID model.ChunkID) ([]byte, error) {
	if ownerUserID == "" {
		return nil, fmt.Errorf("%w: empty owner user ID", errInvalidInput)
	}
	endpoint, err := chunkEndpoint(peerURL, string(chunkID), ownerUserID)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}
	return c.http.Do(req)
}

// chunkEndpoint builds "{scheme}://{host}/chunks/{segment}[?owner_user_id=...]".
//
// The URL is rebuilt from its scheme and host only, and the last path
// segment is escaped, so neither a strange peer URL nor a strange chunk ID
// can change which endpoint is called ("..", "/" and "?" stay inert).
func chunkEndpoint(peerURL, segment, ownerUserID string) (string, error) {
	u, err := url.Parse(peerURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("%w: peer URL %q must look like http://host:port", errInvalidInput, peerURL)
	}
	if segment == "" || segment == "." || segment == ".." {
		return "", fmt.Errorf("%w: invalid chunk ID %q", errInvalidInput, segment)
	}

	endpoint := u.Scheme + "://" + u.Host + "/chunks/" + url.PathEscape(segment)
	if ownerUserID != "" {
		endpoint += "?" + url.Values{"owner_user_id": {ownerUserID}}.Encode()
	}
	return endpoint, nil
}

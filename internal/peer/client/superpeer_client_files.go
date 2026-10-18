package client

import (
	"context"
	"net/http"
	"time"

	"dfsha/internal/shared/model"
)

// ackTimeout bounds PutAck, which has no context parameter (its signature
// predates this package and is used as is by the server and the heartbeat).
const ackTimeout = 10 * time.Second

// PutInit starts an upload (POST /files/put/init): the superpeer answers
// with the peers that must receive each chunk.
func (c *SuperpeerClient) PutInit(ctx context.Context, req model.PutInitRequest) (model.PutInitResponse, error) {
	clean, err := cleanLogicalPath(req.LogicalPath)
	if err != nil {
		return model.PutInitResponse{}, err
	}

	req.LogicalPath = clean

	var resp model.PutInitResponse

	if err := c.call(ctx, http.MethodPost, "/files/put/init", req, &resp); err != nil {
		return model.PutInitResponse{}, err
	}
	return resp, nil
}

// PutAck tells the superpeer that peerID now stores chunkID
// (POST /files/put/ack). It is called by the peer that RECEIVED the chunk.
func (c *SuperpeerClient) PutAck(chunkID model.ChunkID, peerID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), ackTimeout)
	defer cancel()

	return c.call(ctx, http.MethodPost, "/files/put/ack", model.PutAckRequest{
		ChunkID: chunkID,
		PeerID:  peerID,
	}, nil)
}

// GetFile asks where the chunks of a file are (GET /files/get/{ruta}).
// Each chunk comes with the peers that confirmed holding it and the hash
// to verify it against.
func (c *SuperpeerClient) GetFile(ctx context.Context, logicalPath string) (model.GetFileResponse, error) {
	endpoint, err := filesEndpoint("/files/get", logicalPath)
	if err != nil {
		return model.GetFileResponse{}, err
	}

	var resp model.GetFileResponse
	if err := c.call(ctx, http.MethodGet, endpoint, nil, &resp); err != nil {
		return model.GetFileResponse{}, err
	}
	return resp, nil
}

// DeleteFile deletes a file (DELETE /files/{ruta}). The superpeer answers
// as soon as the deletion starts; chunks disappear from peers eventually.
func (c *SuperpeerClient) DeleteFile(ctx context.Context, logicalPath string) error {
	endpoint, err := filesEndpoint("/files", logicalPath)

	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodDelete, endpoint, nil, nil)
}

// Lock asks for the write lock of a file (POST /files/lock/{ruta}). It
// fails with ErrConflict if someone else holds it. The caller must Unlock
// before LockResponse.ExpiresAt, or the superpeer releases it by itself.
func (c *SuperpeerClient) Lock(ctx context.Context, logicalPath string) (model.LockResponse, error) {
	endpoint, err := filesEndpoint("/files/lock", logicalPath)
	if err != nil {
		return model.LockResponse{}, err
	}

	var resp model.LockResponse
	if err := c.call(ctx, http.MethodPost, endpoint, nil, &resp); err != nil {
		return model.LockResponse{}, err
	}
	return resp, nil
}

// Unlock releases a lock (POST /files/unlock/{ruta}). Idempotent on the
// superpeer: ErrNotFound only means the lock already expired, which the
// caller can usually ignore.
func (c *SuperpeerClient) Unlock(ctx context.Context, logicalPath, lockID string) error {
	endpoint, err := filesEndpoint("/files/unlock", logicalPath)
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodPost, endpoint, model.UnlockRequest{LockID: lockID}, nil)
}

package client

import "dfsha/internal/shared/model"

type SuperpeerClient struct { /* *httpclient.Client */
}

func NewSuperpeerClient(baseURL, token string) *SuperpeerClient
func (c *SuperpeerClient) PutInit(path string, chunkCount int, hashes []string) (*model.PutInitResponse, error)
func (c *SuperpeerClient) PutAck(chunkID model.ChunkID, peerID string) error
func (c *SuperpeerClient) GetFile(path string) (*model.GetFileResponse, error)
func (c *SuperpeerClient) Heartbeat(payload model.HeartbeatPayload) error
func (c *SuperpeerClient) Lock(path string) (*model.LockResponse, error)
func (c *SuperpeerClient) Unlock(path, lockID string) error

type PeerClient struct { /* *httpclient.Client */
}

func NewPeerClient(baseURL string) *PeerClient

func (c *PeerClient) SendChunk(ownerUserID string, chunkID model.ChunkID, data []byte, hash string) error
func (c *PeerClient) FetchChunk(chunkID model.ChunkID) ([]byte, error)
func (c *PeerClient) DeleteChunk(chunkID model.ChunkID) error

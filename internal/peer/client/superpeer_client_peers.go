package client

import (
	"context"
	"errors"
	"net/http"

	"dfsha/internal/shared/model"
)

// RegisterPeer tells the superpeer where other peers can reach him
// (POST /peers/register) and returns the peer ID it assigned. The ID goes
// into every heartbeat and ack.
func (c *SuperpeerClient) RegisterPeer(ctx context.Context, address, port string) (string, error) {
	var resp model.RegisterPeerResponse

	err := c.call(ctx, http.MethodPost, "/peers/register", model.RegisterPeerRequest{
		Address: address,
		Port:    port,
	}, &resp)
	if err != nil {
		return "", err
	}
	if resp.PeerID == "" {
		return "", errors.New("superpeer returned an empty peer ID")
	}
	return resp.PeerID, nil
}

// Heartbeat reports this peer's state (POST /peers/heartbeat) and returns
// the superpeer's reply, which may carry replication tasks.
func (c *SuperpeerClient) Heartbeat(ctx context.Context, payload model.HeartbeatPayload) (model.HeartbeatResponse, error) {
	var resp model.HeartbeatResponse

	if err := c.call(ctx, http.MethodPost, "/peers/heartbeat", payload, &resp); err != nil {
		return model.HeartbeatResponse{}, err
	}
	return resp, nil
}

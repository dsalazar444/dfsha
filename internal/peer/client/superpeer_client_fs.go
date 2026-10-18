package client

import (
	"context"
	"net/http"
	"net/url"

	"dfsha/internal/shared/model"
)

// Mkdir creates a logical directory (POST /fs/mkdir). Metadata only.
func (c *SuperpeerClient) Mkdir(ctx context.Context, logicalPath string) error {
	clean, err := cleanLogicalPath(logicalPath)
	if err != nil {
		return err
	}

	return c.call(ctx, http.MethodPost, "/fs/mkdir", model.PathRequest{Path: clean}, nil)
}

// Rmdir removes a logical directory (DELETE /fs/rmdir). Metadata only:
// deleting the chunks of the files inside is a separate step, driven by
// the caller through DeleteFile.
func (c *SuperpeerClient) Rmdir(ctx context.Context, logicalPath string) error {
	clean, err := cleanLogicalPath(logicalPath)
	if err != nil {
		return err
	}
	return c.call(ctx, http.MethodDelete, "/fs/rmdir", model.PathRequest{Path: clean}, nil)
}

// Ls lists a logical directory (GET /fs/ls?path=...).
func (c *SuperpeerClient) Ls(ctx context.Context, logicalPath string) ([]model.DirEntry, error) {
	clean, err := cleanLogicalPath(logicalPath)
	if err != nil {
		return nil, err
	}

	var resp model.ListDirResponse

	endpoint := "/fs/ls?" + url.Values{"path": {clean}}.Encode() // armamos ruta completa

	if err := c.call(ctx, http.MethodGet, endpoint, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

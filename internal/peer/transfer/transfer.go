package transfer

import "context"

func Put(ctx context.Context, localPath, remotePath string, deps Deps) error
func Get(ctx context.Context, remotePath, localPath string, deps Deps) error

package heartbeat

import (
	"context"
	"dfsha/internal/peer/client"
	"dfsha/internal/peer/storage"
	"time"
)

func Start(ctx context.Context, interval time.Duration, store *storage.Store, superpeer *client.SuperpeerClient)

// Superpeer entrypoint
// Configuration is loaded entirely from environment variables

package main

import (
	"context"
	"dfsha/internal/auth"
	"dfsha/internal/config"
	"dfsha/internal/files"
	"dfsha/internal/fs"
	"dfsha/internal/leader"
	"dfsha/internal/locks"
	"dfsha/internal/middleware"
	"dfsha/internal/peers"
	"dfsha/internal/persist"
	"dfsha/internal/server"
	"dfsha/internal/shared/tlsconf"
	"dfsha/internal/store"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Load all configuration from environment variables
	cfg := config.Load()

	log.Printf("[superpeer] starting on port %s", cfg.Port)
	log.Printf("[superpeer] chunk_size=%d bytes | replication=%d | heartbeat_timeout=%s",
		cfg.ChunkSize, cfg.ReplicationFactor, cfg.HeartbeatTimeout)

	// Build stores
	userStore := store.NewUserStore()
	peerStore := store.NewPeerStore()
	peerStore.SetScheme(cfg.PeerScheme) // before loading the saved state: it decides the URL of every peer
	fsStore := store.NewFsStore()
	fileStore := store.NewFileStore()
	lockStore := store.NewLockStore()
	taskStore := store.NewTaskStore() // borrados pendientes: los escribe files, los entrega el heartbeat de peers

	// Restore the state saved by a previous run (or by the other superpeer) before serving anything
	persister := startPersistence(cfg.StateDir, persist.Stores{
		Peers: peerStore, Fs: fsStore, Files: fileStore, Locks: lockStore, Tasks: taskStore,
	})

	// Build services
	authService := auth.NewService(userStore, cfg.SessionSecret)
	peerService := peers.NewService(peerStore, fileStore, taskStore, cfg.HeartbeatTimeout, cfg.ReplicationFactor)
	fileService := files.NewService(fsStore, fileStore, peerStore, cfg.ReplicationFactor, files.NewTaskChunkDeleter(taskStore))
	fsService := fs.NewService(fsStore, fileService)
	lockService := locks.NewService(lockStore, fsStore, 30*time.Second)

	// Build HTTP handlers
	authHandler := auth.NewHandler(authService)
	peerHandler := peers.NewHandler(peerService)
	fsHandler := fs.NewHandler(fsService)
	fileHandler := files.NewHandler(fileService)
	lockHandler := locks.NewHandler(lockService)

	// Build the HTTP router with all routes registered
	router := server.NewRouter(cfg, authService, authHandler, peerHandler, fsHandler, fileHandler, lockHandler)

	// Leader election: with a shared STATE_DIR only one superpeer serves, the other waits on standby
	// (answering 421) and takes over when the leader's lease expires
	elector := newElector(cfg, persister)
	isLeader := func() bool { return elector == nil || elector.IsLeader() }

	// Configure the HTTP server
	httpServer := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      middleware.RequireLeader(isLeader)(router),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start background tasks
	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel() // ensure tasks are cancelled on exit
	if elector != nil {
		elector.Run(bgCtx) // decides the role before the server starts accepting requests
	}
	peerService.StartHealthMonitor(bgCtx)
	lockService.StartExpirySweeper(bgCtx)
	if persister != nil {
		persister.Run(bgCtx, stateSaveInterval, isLeader)
	}

	// Start listening in a goroutine so we can handle shutdown signals
	serverErr := make(chan error, 1)
	go func() {
		if err := tlsconf.ListenAndServe(httpServer, cfg.TLSCert, cfg.TLSKey, cfg.AllowInsecure); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	log.Printf("[superpeer] listening on port %s | peers are reached over %s", cfg.Port, cfg.PeerScheme)
	log.Printf("[superpeer] health check: GET /health")

	// Block until SIGINT / SIGTERM or a server error
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serverErr:
		log.Fatalf("[superpeer] server error: %v", err)
	case sig := <-quit:
		log.Printf("[superpeer] received signal %s — shutting down gracefully", sig)
	}

	// Graceful shutdown: give in-flight requests up to 15 s to finish
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("[superpeer] forced shutdown: %v", err)
	}

	// Last save, after the server stopped taking requests, so a clean shutdown loses nothing
	// (only the leader writes: a standby's copy is older than the file)
	if persister != nil && isLeader() {
		if err := persister.Save(); err != nil {
			log.Printf("[superpeer] saving state on shutdown: %v", err)
		}
	}

	log.Println("[superpeer] stopped cleanly")
}

const (
	// stateSaveInterval is how often the state is written to disk
	stateSaveInterval = 2 * time.Second

	// leaseTTL is how long the leader's lease lasts without being renewed: after the leader dies,
	// the standby takes over within leaseTTL + leaseRenewEvery (about 20 s)
	leaseTTL        = 15 * time.Second
	leaseRenewEvery = 5 * time.Second
)

// newElector returns the leader elector, or nil when there is no STATE_DIR (a single superpeer, always leader)
// When it becomes the leader it loads state.json again, to pick up what the old leader saved
func newElector(cfg *config.Config, persister *persist.Persister) *leader.Elector {
	if persister == nil {
		return nil
	}
	log.Printf("[superpeer] id=%s — leader election through %s/leader.json", cfg.SuperpeerID, cfg.StateDir)
	return leader.New(cfg.StateDir, cfg.SuperpeerID, leaseTTL, leaseRenewEvery, persister.Load)
}

// startPersistence loads the saved state into the stores and returns the Persister that keeps saving it
// It returns nil when stateDir is empty (state only in memory)
// An error stops the superpeer: starting empty over a state that cannot be read would overwrite it
func startPersistence(stateDir string, stores persist.Stores) *persist.Persister {
	if stateDir == "" {
		log.Println("[superpeer] STATE_DIR not set — state only in memory, lost on restart")
		return nil
	}

	persister, err := persist.New(stateDir, stores)
	if err != nil {
		log.Fatalf("[superpeer] %v", err)
	}
	if err := persister.Load(); err != nil {
		log.Fatalf("[superpeer] loading state: %v", err)
	}
	log.Printf("[superpeer] state kept in %s (saved every %s)", stateDir, stateSaveInterval)
	return persister
}

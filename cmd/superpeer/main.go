// Superpeer entrypoint
// Configuration is loaded entirely from environment variables

package main

import (
	"context"
	"dfsha/internal/auth"
	"dfsha/internal/config"
	"dfsha/internal/server"
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

	// Build services
	authService := auth.NewService(userStore)

	// Build HTTP handlers
	authHandler := auth.NewHandler(authService)

	// Build the HTTP router with all routes registered
	router := server.NewRouter(cfg, authService, authHandler)

	// Configure the HTTP server
	httpServer := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start listening in a goroutine so we can handle shutdown signals
	serverErr := make(chan error, 1)
	go func() {
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	log.Printf("[superpeer] listening at http://localhost:%s", cfg.Port)
	log.Printf("[superpeer] health check: GET http://localhost:%s/health", cfg.Port)

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

	log.Println("[superpeer] stopped cleanly")
}

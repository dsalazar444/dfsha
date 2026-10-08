// Package config loads all runtime configuration from environment variables

// Usage:
//
//	cfg := config.Load()
//	fmt.Println(cfg.Port)

package config

import (
	"encoding/hex"
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds all superpeer runtime settings
type Config struct {
	// Port on which the superpeer HTTP server listens
	Port string

	// ChunkSize is the size in bytes of each file chunk
	ChunkSize int64

	// ReplicationFactor is the number of peers each chunk is stored on
	ReplicationFactor int

	// HeartbeatTimeout is how long the superpeer waits before marking
	// a peer as unhealthy after its last heartbeat.
	HeartbeatTimeout time.Duration

	// AESKey holds the decoded 32-byte AES-256 key ready to use with crypto/aes
	AESKey []byte
}

const (
	defaultPort              = "8080"
	defaultChunkSize   int64 = 4 * 1024 * 1024 // 4 MB
	defaultReplication       = 2
	defaultHeartbeatS        = 15

	// devAESKeyHex is the hex-encoded form of the dev placeholder key
	//Used only when AES_KEY is not set
	// It is intentionally visible here to make it obvious it is not secret
	devAESKeyHex = "6466736861646576656b65793030303030303030303030303030303030303030"
)

// Load reads environment variables and returns a populated Config
func Load() *Config {
	return &Config{
		Port:              getEnvStr("PORT", defaultPort),
		ChunkSize:         getEnvInt64("CHUNK_SIZE", defaultChunkSize),
		ReplicationFactor: getEnvInt("REPLICATION_FACTOR", defaultReplication),
		HeartbeatTimeout:  time.Duration(getEnvInt("HEARTBEAT_TIMEOUT_SECONDS", defaultHeartbeatS)) * time.Second,
		AESKey:            loadAESKey(),
	}
}

// loadAESKey reads AES_KEY from the environment (64 hex chars = 32 bytes)
// Falls back to the dev placeholder when not set, logging a clear warning

func loadAESKey() []byte {
	hexKey := os.Getenv("AES_KEY")
	if hexKey == "" {
		log.Println("[config] WARNING: AES_KEY not set — using insecure dev key. DO NOT use in production.")
		hexKey = devAESKeyHex
	}

	key, err := hex.DecodeString(hexKey)
	if err != nil {
		log.Fatalf("[config] AES_KEY is not valid hex: %v", err)
	}
	if len(key) != 32 {
		log.Fatalf("[config] AES_KEY must decode to exactly 32 bytes (AES-256). Got %d bytes from %d hex chars. Generate with: openssl rand -hex 32", len(key), len(hexKey))
	}
	return key
}

// getEnvStr returns the value of the env var or the fallback default
func getEnvStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// getEnvInt returns the integer value of the env var or the fallback default
func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("[config] Invalid value for %s: %q — expected integer", key, v)
	}
	return n
}

// getEnvInt64 returns the int64 value of the env var or the fallback default
func getEnvInt64(key string, fallback int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Fatalf("[config] Invalid value for %s: %q — expected integer", key, v)
	}
	return n
}


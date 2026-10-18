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

	// SessionSecret signs the session tokens (JWT, HS256). Both superpeers must use the same one,
	// so a token issued by one is still valid on the other after a failover
	SessionSecret []byte

	// StateDir is the folder where the superpeer saves its state (state.json), so it survives a restart
	// If it is a shared folder (NFS/EFS), a second superpeer can take over with the same state
	// Empty means the state only lives in memory
	StateDir string

	// SuperpeerID names this superpeer in the leader lease (STATE_DIR/leader.json)
	// The two superpeers that share STATE_DIR must have different ids. Defaults to the hostname
	SuperpeerID string
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

	// devSessionSecretHex is the dev placeholder for SESSION_SECRET, used only when it is not set
	// Being a constant, two superpeers without SESSION_SECRET still share it (fine for local tests only)
	devSessionSecretHex = "646673686173657373696f6e7365637265743030303030303030303030303030"
)

// Load reads environment variables and returns a populated Config
func Load() *Config {
	return &Config{
		Port:              getEnvStr("PORT", defaultPort),
		ChunkSize:         getEnvInt64("CHUNK_SIZE", defaultChunkSize),
		ReplicationFactor: getEnvInt("REPLICATION_FACTOR", defaultReplication),
		HeartbeatTimeout:  time.Duration(getEnvInt("HEARTBEAT_TIMEOUT_SECONDS", defaultHeartbeatS)) * time.Second,
		AESKey:            loadKey("AES_KEY", devAESKeyHex),
		SessionSecret:     loadKey("SESSION_SECRET", devSessionSecretHex),
		StateDir:          getEnvStr("STATE_DIR", ""),
		SuperpeerID:       getEnvStr("SUPERPEER_ID", hostname()),
	}
}

// loadKey reads a 32-byte key from the env var name (64 hex chars), used for AES_KEY and SESSION_SECRET
// Falls back to the dev placeholder when not set, logging a clear warning

func loadKey(name, devHex string) []byte {
	hexKey := os.Getenv(name)
	if hexKey == "" {
		log.Printf("[config] WARNING: %s not set — using insecure dev key. DO NOT use in production.", name)
		hexKey = devHex
	}

	key, err := hex.DecodeString(hexKey)
	if err != nil {
		log.Fatalf("[config] %s is not valid hex: %v", name, err)
	}
	if len(key) != 32 {
		log.Fatalf("[config] %s must decode to exactly 32 bytes. Got %d bytes from %d hex chars. Generate with: openssl rand -hex 32", name, len(key), len(hexKey))
	}
	return key
}

// hostname is the default SUPERPEER_ID (in Docker, the container name or id)
func hostname() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "superpeer"
	}
	return name
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


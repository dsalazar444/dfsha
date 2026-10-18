package config

// Package config loads the peer's settings from a .env file and from
// environment variables.
//
// Precedence, highest first: real environment variables, then the .env
// file, then built-in defaults. A missing .env file is fine (production
// usually sets real variables); a malformed line in it is an error, so a
// typo is not silently ignored.
//
// Credentials are NOT settings: the password is asked interactively and
// never read from here, so it cannot end up in a file or in a process
// listing.

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Built-in defaults for the optional settings.
const (
	defaultStoragePath       = "./data"
	defaultHeartbeatInterval = 10 * time.Second
	defaultChunkSize         = 64 << 20
	defaultMaxChunkSizeMB    = 64
	defaultParallelism       = 4
)

// TLS es obligatorio por defecto: sin TLS_CERT y TLS_KEY el peer se niega a iniciar, y solo acepta
// superpeers con URL https. Para desarrollo local sin certificados hay que pedirlo de forma explícita
// con ALLOW_INSECURE=true (igual que en el superpeer)
const defaultAllowInsecure = false

// Config is the validated configuration of one peer process.
type Config struct {
	// SuperpeerURLs is the comma-separated, ordered list of superpeer base
	// URLs (SUPERPEER_URLS). The first one is where the peer starts.
	// client.NewSuperpeerPool parses it.
	SuperpeerURLs string

	// PeerAddress is the host other peers must use to reach THIS peer
	// (PEER_ADDRESS). It is sent to the superpeer at registration, so it
	// must be reachable from the other machines: not "localhost" unless
	// everything runs on one machine.
	PeerAddress string

	// PeerPort is the TCP port this peer listens on and advertises
	// (PEER_PORT).
	PeerPort string

	// StoragePath is the directory where chunks are stored (STORAGE_PATH).
	StoragePath string

	// HeartbeatInterval is the time between heartbeats (HEARTBEAT_INTERVAL,
	// a Go duration such as "10s").
	HeartbeatInterval time.Duration

	// ChunkSize is the plaintext bytes per chunk on upload (CHUNK_SIZE).
	ChunkSize int

	// MaxChunkSizeMB caps the size of one accepted chunk envelope in MiB
	// (MAX_CHUNK_SIZE_MB).
	MaxChunkSizeMB int

	// Parallelism is how many chunk transfers run at once (PARALLELISM).
	Parallelism int

	// Username pre-fills the login prompt (DFSHA_USERNAME). Optional.
	Username string

	// TLSCert and TLSKey are the files of this peer's certificate and private key
	// (deploy/certs/gen-certs.sh). With both set the peer serves HTTPS
	// (TLS_CERT, TLS_KEY). Setting only one is an error.
	TLSCert string
	TLSKey  string

	// TLSCA is the CA certificate (TLS_CA). It is used to trust the superpeers and the
	// other peers when they are reached over https, and, when the peer serves HTTPS, to
	// check the certificate that every client must present (mutual TLS). Required with
	// TLS_CERT.
	TLSCA string

	// AllowInsecure lets the peer serve plain HTTP when it has no certificate
	// (ALLOW_INSECURE, local development and tests).
	AllowInsecure bool
}

// Load reads envFile (if it exists) and the process environment and
// returns a validated Config.
func Load(envFile string) (Config, error) {
	return load(envFile, os.LookupEnv)
}

// load is Load with the environment injected, so tests do not touch the
// real one.
func load(envFile string, lookupEnv func(string) (string, bool)) (Config, error) {
	fileVars, err := readDotEnv(envFile)
	if err != nil {
		return Config{}, err
	}
	get := func(key string) string {
		if v, ok := lookupEnv(key); ok && v != "" {
			return v
		}
		return fileVars[key]
	}

	cfg := Config{
		SuperpeerURLs: get("SUPERPEER_URLS"),
		PeerAddress:   get("PEER_ADDRESS"),
		PeerPort:      get("PEER_PORT"),
		StoragePath:   get("STORAGE_PATH"),
		Username:      get("DFSHA_USERNAME"),
		TLSCert:       get("TLS_CERT"),
		TLSKey:        get("TLS_KEY"),
		TLSCA:         get("TLS_CA"),
	}

	// Required settings first, with the variable name in the message.
	if cfg.SuperpeerURLs == "" {
		return Config{}, errors.New("SUPERPEER_URLS is required (comma-separated list, for example https://10.0.0.1:8080,https://10.0.0.2:8080)")
	}
	if err := checkURLList(cfg.SuperpeerURLs); err != nil {
		return Config{}, fmt.Errorf("SUPERPEER_URLS: %w", err)
	}
	if cfg.PeerAddress == "" {
		return Config{}, errors.New("PEER_ADDRESS is required (the host other peers use to reach this one)")
	}
	if cfg.PeerPort == "" {
		return Config{}, errors.New("PEER_PORT is required")
	}
	if port, err := strconv.Atoi(cfg.PeerPort); err != nil || port < 1 || port > 65535 {
		return Config{}, fmt.Errorf("PEER_PORT must be a number from 1 to 65535, got %q", cfg.PeerPort)
	}

	// Optional settings: default when empty, validate when set.
	if cfg.StoragePath == "" {
		cfg.StoragePath = defaultStoragePath
	}

	cfg.HeartbeatInterval = defaultHeartbeatInterval
	if v := get("HEARTBEAT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			return Config{}, fmt.Errorf("HEARTBEAT_INTERVAL must be a duration of at least 1s (for example 10s), got %q", v)
		}
		cfg.HeartbeatInterval = d
	}

	cfg.ChunkSize = defaultChunkSize
	if v := get("CHUNK_SIZE"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("CHUNK_SIZE must be a positive number of bytes, got %q", v)
		}
		cfg.ChunkSize = n
	}

	cfg.MaxChunkSizeMB = defaultMaxChunkSizeMB
	if v := get("MAX_CHUNK_SIZE_MB"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("MAX_CHUNK_SIZE_MB must be a positive number of MiB, got %q", v)
		}
		cfg.MaxChunkSizeMB = n
	}

	cfg.Parallelism = defaultParallelism
	if v := get("PARALLELISM"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return Config{}, fmt.Errorf("PARALLELISM must be a positive number, got %q", v)
		}
		cfg.Parallelism = n
	}

	// TLS settings. Checked here, and not when the server starts, so a typo fails before the
	// login prompt and not after it.
	if (cfg.TLSCert == "") != (cfg.TLSKey == "") {
		return Config{}, errors.New("TLS_CERT and TLS_KEY must be set together")
	}
	if cfg.TLSCert != "" && cfg.TLSCA == "" {
		return Config{}, errors.New("TLS_CA is required with TLS_CERT: the peer checks the certificate of every client that connects to it (mutual TLS)")
	}
	cfg.AllowInsecure = defaultAllowInsecure
	if v := get("ALLOW_INSECURE"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("ALLOW_INSECURE must be true or false, got %q", v)
		}
		cfg.AllowInsecure = b
	}
	if cfg.TLSCert == "" && !cfg.AllowInsecure {
		return Config{}, errors.New("TLS is required: set TLS_CERT and TLS_KEY (or ALLOW_INSECURE=true for local tests only)")
	}
	if !cfg.AllowInsecure && strings.Contains(strings.ReplaceAll(cfg.SuperpeerURLs, " ", ""), "http://") {
		return Config{}, errors.New("SUPERPEER_URLS must be https:// URLs while TLS is required (or ALLOW_INSECURE=true for local tests only)")
	}

	return cfg, nil
}

// checkURLList verifies that every entry of the comma-separated list is an
// http(s) URL with a host. The pool only trims entries; a typo such as a
// missing "http://" should fail at startup, not at the first request.
func checkURLList(list string) error {
	found := 0
	for _, raw := range strings.Split(list, ",") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return fmt.Errorf("%q is not a valid URL (expected something like http://host:8080)", raw)
		}
		found++
	}
	if found == 0 {
		return errors.New("no URL found in the list")
	}
	return nil
}

// readDotEnv parses a .env file into a map. Supported syntax, which is the
// common subset:
//
//	# comment
//	KEY=value
//	export KEY=value
//	KEY="value with spaces"     (single or double quotes are stripped)
//
// A missing file returns an empty map and no error.
func readDotEnv(path string) (map[string]string, error) {
	vars := map[string]string{}

	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return vars, nil
	}
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))

		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			return nil, fmt.Errorf("%s:%d: expected KEY=VALUE, got %q", path, lineNo, line)
		}
		vars[key] = unquote(strings.TrimSpace(value))
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}
	return vars, nil
}

// unquote strips one pair of matching surrounding quotes.
func unquote(v string) string {
	if len(v) >= 2 {
		first, last := v[0], v[len(v)-1]
		if (first == '"' || first == '\'') && first == last {
			return v[1 : len(v)-1]
		}
	}
	return v
}

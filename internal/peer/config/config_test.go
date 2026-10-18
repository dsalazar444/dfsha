package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// env builds a lookup function over a fixed map.
func env(vars map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := vars[k]; return v, ok }
}

func writeEnvFile(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimal = `SUPERPEER_URLS=http://a:8080,http://b:8080
PEER_ADDRESS=10.0.0.5
PEER_PORT=9000
`

func TestLoadMinimalUsesDefaults(t *testing.T) {
	cfg, err := load(writeEnvFile(t, minimal), env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SuperpeerURLs != "http://a:8080,http://b:8080" || cfg.PeerAddress != "10.0.0.5" || cfg.PeerPort != "9000" {
		t.Errorf("required values: %+v", cfg)
	}
	if cfg.StoragePath != "./data" || cfg.HeartbeatInterval != 10*time.Second ||
		cfg.ChunkSize != 64<<20 || cfg.MaxChunkSizeMB != 64 || cfg.Parallelism != 4 || cfg.Username != "" {
		t.Errorf("defaults: %+v", cfg)
	}
}

func TestLoadOverridesAndPrecedence(t *testing.T) {
	file := writeEnvFile(t, minimal+`STORAGE_PATH=/var/dfsha
HEARTBEAT_INTERVAL=5s
CHUNK_SIZE=1048576
MAX_CHUNK_SIZE_MB=32
PARALLELISM=8
DFSHA_USERNAME=ana
`)
	// The real environment wins over the file.
	cfg, err := load(file, env(map[string]string{"PEER_PORT": "9100", "PARALLELISM": "2"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PeerPort != "9100" || cfg.Parallelism != 2 {
		t.Errorf("environment did not override the file: %+v", cfg)
	}
	if cfg.StoragePath != "/var/dfsha" || cfg.HeartbeatInterval != 5*time.Second ||
		cfg.ChunkSize != 1<<20 || cfg.MaxChunkSizeMB != 32 || cfg.Username != "ana" {
		t.Errorf("file values: %+v", cfg)
	}
}

func TestLoadWithoutFileUsesEnvironment(t *testing.T) {
	cfg, err := load(filepath.Join(t.TempDir(), "missing.env"), env(map[string]string{
		"SUPERPEER_URLS": "http://x:1", "PEER_ADDRESS": "h", "PEER_PORT": "1",
	}))
	if err != nil || cfg.PeerPort != "1" {
		t.Fatalf("%+v, %v", cfg, err)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	cases := map[string]struct {
		content string
		want    string // part of the expected message
	}{
		"no superpeers":     {"PEER_ADDRESS=h\nPEER_PORT=1\n", "SUPERPEER_URLS is required"},
		"no scheme":         {"SUPERPEER_URLS=10.0.0.1:8080\nPEER_ADDRESS=h\nPEER_PORT=1\n", "not a valid URL"},
		"only commas":       {"SUPERPEER_URLS= , ,\nPEER_ADDRESS=h\nPEER_PORT=1\n", "SUPERPEER_URLS"},
		"no address":        {"SUPERPEER_URLS=http://a:1\nPEER_PORT=1\n", "PEER_ADDRESS is required"},
		"no port":           {"SUPERPEER_URLS=http://a:1\nPEER_ADDRESS=h\n", "PEER_PORT is required"},
		"port not a number": {minimal + "PEER_PORT=abc\n", "PEER_PORT must be"},
		"port out of range": {"SUPERPEER_URLS=http://a:1\nPEER_ADDRESS=h\nPEER_PORT=70000\n", "PEER_PORT must be"},
		"interval too low":  {minimal + "HEARTBEAT_INTERVAL=100ms\n", "HEARTBEAT_INTERVAL"},
		"interval garbage":  {minimal + "HEARTBEAT_INTERVAL=soon\n", "HEARTBEAT_INTERVAL"},
		"chunk size zero":   {minimal + "CHUNK_SIZE=0\n", "CHUNK_SIZE"},
		"max chunk zero":    {minimal + "MAX_CHUNK_SIZE_MB=0\n", "MAX_CHUNK_SIZE_MB"},
		"max chunk garbage": {minimal + "MAX_CHUNK_SIZE_MB=abc\n", "MAX_CHUNK_SIZE_MB"},
		"parallelism zero":  {minimal + "PARALLELISM=0\n", "PARALLELISM"},
		"malformed line":    {minimal + "this is not valid\n", "expected KEY=VALUE"},
	}
	for name, tc := range cases {
		_, err := load(writeEnvFile(t, tc.content), env(nil))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error containing %q", name, err, tc.want)
		}
	}
}

func TestReadDotEnvSyntax(t *testing.T) {
	file := writeEnvFile(t, `# a comment

export A=1
B = 2
C="with spaces"
D='single'
E=has=equals
F=
`)
	got, err := readDotEnv(file)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"A": "1", "B": "2", "C": "with spaces", "D": "single", "E": "has=equals", "F": ""}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
}

package peer

// End-to-end tests of the whole peer: real storage, real HTTP between
// peers, real encryption and chunking, the real command line, against a
// fake superpeer that implements the contract in memory.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"dfsha/internal/peer/config"
	"dfsha/internal/shared/model"
)

// --- fake superpeer ----------------------------------------------------

type fakeSuperpeer struct {
	mu sync.Mutex

	salt       []byte
	peers      map[string]string // peer ID -> base URL
	files      map[string]model.PutInitRequest
	acks       map[model.ChunkID]map[string]bool
	heartbeats map[string][]model.ChunkID // last chunk list per peer ID
	beatCount  map[string]int
	registers  int
	dirs       map[string]bool
}

func newFakeSuperpeer() *fakeSuperpeer {
	return &fakeSuperpeer{
		salt:       []byte("0123456789abcdef"),
		peers:      map[string]string{},
		files:      map[string]model.PutInitRequest{},
		acks:       map[model.ChunkID]map[string]bool{},
		heartbeats: map[string][]model.ChunkID{},
		beatCount:  map[string]int{},
		dirs:       map[string]bool{"/": true},
	}
}

func jsonBody(r *http.Request, v any) bool { return json.NewDecoder(r.Body).Decode(v) == nil }

func reply(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

// authorized accepts the token this fake hands out ("tok-<user>").
func authorized(w http.ResponseWriter, r *http.Request) bool {
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer tok-") {
		http.Error(w, "token missing or expired", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *fakeSuperpeer) handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /auth/login", func(w http.ResponseWriter, r *http.Request) {
		var req model.LoginRequest
		if !jsonBody(r, &req) {
			http.Error(w, "bad body", 400)
			return
		}
		if req.Username != "ana" || req.Password != "secret" {
			http.Error(w, "invalid credentials", 401)
			return
		}
		reply(w, model.LoginResponse{Token: "tok-ana", UserID: "uid-ana", Salt: base64.StdEncoding.EncodeToString(s.salt)})
	})

	mux.HandleFunc("POST /peers/register", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		var req model.RegisterPeerRequest
		if !jsonBody(r, &req) {
			http.Error(w, "bad body", 400)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.registers++
		// Idempotent: the same address and port always get the same ID,
		// which is what the peer relies on when it registers again.
		id := "peer-" + req.Port
		s.peers[id] = "http://" + req.Address + ":" + req.Port
		reply(w, model.RegisterPeerResponse{PeerID: id})
	})

	mux.HandleFunc("POST /peers/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		var req model.HeartbeatPayload
		if !jsonBody(r, &req) {
			http.Error(w, "bad body", 400)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, known := s.peers[req.PeerID]; !known {
			http.Error(w, "peer_id is not registered", 404)
			return
		}
		s.heartbeats[req.PeerID] = req.Chunks
		s.beatCount[req.PeerID]++
		reply(w, model.HeartbeatResponse{Tasks: []model.ReplicationTask{}})
	})

	mux.HandleFunc("POST /fs/mkdir", func(w http.ResponseWriter, r *http.Request) {
		var req model.PathRequest
		if !authorized(w, r) || !jsonBody(r, &req) {
			return
		}
		s.mu.Lock()
		s.dirs[req.Path] = true
		s.mu.Unlock()
		w.Write([]byte("directory created"))
	})

	mux.HandleFunc("GET /fs/ls", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		dir := r.URL.Query().Get("path")
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.dirs[dir] {
			http.Error(w, "no such directory", 404)
			return
		}
		var entries []model.DirEntry
		for d := range s.dirs {
			if d != "/" && path.Dir(d) == dir {
				entries = append(entries, model.DirEntry{Name: path.Base(d), Type: "directory"})
			}
		}
		for f := range s.files {
			if path.Dir(f) == dir {
				entries = append(entries, model.DirEntry{Name: path.Base(f), Type: "file"})
			}
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
		reply(w, model.ListDirResponse{Entries: entries})
	})

	mux.HandleFunc("POST /files/put/init", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		var req model.PutInitRequest
		if !jsonBody(r, &req) || req.FileID == "" {
			http.Error(w, "bad put/init", 400)
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.files[req.LogicalPath] = req

		// Every chunk goes to every registered peer.
		var urls []string
		for _, u := range s.peers {
			urls = append(urls, u)
		}
		sort.Strings(urls)
		resp := model.PutInitResponse{FileID: req.FileID}
		for i := 0; i < req.ChunkCount; i++ {
			resp.Assignment = append(resp.Assignment, model.ChunkAssignment{
				ChunkID: model.NewChunkID(req.FileID, i), PeerURLs: urls,
			})
		}
		reply(w, resp)
	})

	mux.HandleFunc("POST /files/put/ack", func(w http.ResponseWriter, r *http.Request) {
		var req model.PutAckRequest
		if !authorized(w, r) || !jsonBody(r, &req) {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.acks[req.ChunkID] == nil {
			s.acks[req.ChunkID] = map[string]bool{}
		}
		s.acks[req.ChunkID][req.PeerID] = true
		w.Write([]byte("ack recorded"))
	})

	mux.HandleFunc("GET /files/get/{path...}", func(w http.ResponseWriter, r *http.Request) {
		if !authorized(w, r) {
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		rec, ok := s.files["/"+r.PathValue("path")]
		if !ok {
			http.Error(w, "no such file", 404)
			return
		}
		resp := model.GetFileResponse{FileID: rec.FileID}
		for i := 0; i < rec.ChunkCount; i++ {
			id := model.NewChunkID(rec.FileID, i)
			loc := model.ChunkLocation{ChunkID: id, ExpectedHash: rec.HashPerChunk[i]}
			for peerID := range s.acks[id] { // only peers that confirmed
				loc.PeerURLs = append(loc.PeerURLs, s.peers[peerID])
			}
			sort.Strings(loc.PeerURLs)
			resp.Chunks = append(resp.Chunks, loc)
		}
		reply(w, resp)
	})

	return mux
}

func (s *fakeSuperpeer) heartbeatCount(peerID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beatCount[peerID]
}

func (s *fakeSuperpeer) reportedChunks(peerID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.heartbeats[peerID])
}

func (s *fakeSuperpeer) registerCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.registers
}

func startFakeSuperpeer(t *testing.T) (*fakeSuperpeer, *httptest.Server) {
	t.Helper()
	sp := newFakeSuperpeer()
	srv := httptest.NewServer(sp.handler())
	t.Cleanup(srv.Close)
	return sp, srv
}

// --- peer process harness ----------------------------------------------

// inputQueue is a reader whose Read blocks until text is sent or it is
// closed, so a test can type commands whenever it wants.
type inputQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    bytes.Buffer
	closed bool
}

func newInputQueue() *inputQueue {
	q := &inputQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *inputQueue) send(text string) {
	q.mu.Lock()
	q.buf.WriteString(text)
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *inputQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}

func (q *inputQueue) Read(p []byte) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.buf.Len() == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.buf.Len() == 0 {
		return 0, io.EOF
	}
	return q.buf.Read(p)
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type peerProc struct {
	t       *testing.T
	in      *inputQueue
	out     *syncBuffer
	done    chan error
	cancel  context.CancelFunc
	port    string
	peerID  string
	dataDir string
}

// startPeer runs a whole peer inside the test process.
func startPeer(t *testing.T, superpeerURLs, username, password string) *peerProc {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)

	p := &peerProc{t: t, in: newInputQueue(), out: &syncBuffer{}, done: make(chan error, 1),
		port: port, peerID: "peer-" + port, dataDir: t.TempDir()}

	a := &app{
		cfg: config.Config{
			SuperpeerURLs: superpeerURLs, PeerAddress: "127.0.0.1", PeerPort: port,
			StoragePath: p.dataDir, HeartbeatInterval: 50 * time.Millisecond,
			ChunkSize: 64, Parallelism: 3,
		},
		in: bufio.NewReader(p.in), out: p.out, terminal: false, listener: listener,
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	t.Cleanup(func() { cancel(); p.in.close() })

	go func() { p.done <- a.run(ctx) }()
	p.in.send(username + "\n" + password + "\n")
	return p
}

func (p *peerProc) say(command string) { p.in.send(command + "\n") }

func (p *peerProc) waitFor(substr string) {
	p.t.Helper()
	eventually(p.t, "peer output to contain "+fmt.Sprintf("%q", substr), func() bool {
		return strings.Contains(p.out.String(), substr)
	})
}

func (p *peerProc) waitReady() { p.t.Helper(); p.waitFor("type help for the list of commands") }

// quit types "exit" and checks that the peer shuts down cleanly.
func (p *peerProc) quit() {
	p.t.Helper()
	p.say("exit")
	select {
	case err := <-p.done:
		if err != nil {
			p.t.Errorf("peer %s stopped with an error: %v", p.port, err)
		}
	case <-time.After(10 * time.Second):
		p.t.Fatalf("peer %s did not stop after exit", p.port)
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// storedFiles lists every file under a storage directory.
func storedFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			files = append(files, p)
		}
		return nil
	})
	return files
}

// --- the tests ----------------------------------------------------------

func TestEndToEndPutThenGetFromAnotherPeer(t *testing.T) {
	sp, srv := startFakeSuperpeer(t)
	peerA := startPeer(t, srv.URL, "ana", "secret")
	peerB := startPeer(t, srv.URL, "ana", "secret")
	peerA.waitReady()
	peerB.waitReady()

	// A file of 1000 bytes with a recognizable marker, in 64-byte chunks: 16 chunks.
	original := []byte(strings.Repeat("TOP-SECRET-PAYLOAD-0123456789-", 34))[:1000]
	localFile := filepath.Join(t.TempDir(), "report.txt")
	os.WriteFile(localFile, original, 0o600)

	// Upload through peer A.
	peerA.say("mkdir docs")
	peerA.say(fmt.Sprintf("put %q docs/", localFile))
	peerA.waitFor("uploaded /docs/report.txt (1000 bytes")
	peerA.say("ls docs")
	peerA.waitFor("report.txt")

	// Both peers stored the chunks, and only as ciphertext.
	for _, p := range []*peerProc{peerA, peerB} {
		files := storedFiles(t, p.dataDir)
		if len(files) != 16 {
			t.Errorf("peer %s stores %d chunk files, want 16", p.port, len(files))
		}
		for _, f := range files {
			content, _ := os.ReadFile(f)
			if bytes.Contains(content, []byte("TOP-SECRET")) {
				t.Errorf("%s holds plaintext", f)
			}
		}
	}

	// Their heartbeats report the chunks they hold (what a superpeer uses to
	// notice lost copies).
	eventually(t, "heartbeats listing 16 chunks", func() bool {
		return sp.reportedChunks(peerA.peerID) == 16 && sp.reportedChunks(peerB.peerID) == 16
	})

	// Make B lose its copies: its server will answer 404 for every chunk,
	// so downloading through B must fall back to peer A over the network.
	for _, f := range storedFiles(t, peerB.dataDir) {
		os.Remove(f)
	}

	dest := filepath.Join(t.TempDir(), "downloaded.txt")
	peerB.say(fmt.Sprintf("get /docs/report.txt %q", dest))
	peerB.waitFor("downloaded /docs/report.txt")

	got, err := os.ReadFile(dest)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("downloaded file differs from the original (%d bytes, err %v)", len(got), err)
	}

	peerA.quit()
	peerB.quit()
}

func TestPeerStartsWhileTheFirstSuperpeerIsDown(t *testing.T) {
	sp, srv := startFakeSuperpeer(t)
	// Nothing listens on port 1: the first superpeer of the list is down.
	peer := startPeer(t, "http://127.0.0.1:1,"+srv.URL, "ana", "secret")

	peer.waitReady()
	eventually(t, "heartbeats at the backup superpeer", func() bool { return sp.heartbeatCount(peer.peerID) > 0 })
	peer.quit()
}

// func TestFailoverWhileRunning(t *testing.T) {
// 	sp1, srv1 := startFakeSuperpeer(t)
// 	sp2, srv2 := startFakeSuperpeer(t) // a backup that has never heard of this peer
// 	peer := startPeer(t, srv1.URL+","+srv2.URL, "ana", "secret")
// 	peer.waitReady()
// 	eventually(t, "first heartbeats at superpeer 1", func() bool { return sp1.heartbeatCount(peer.peerID) > 2 })

// 	srv1.Close() // the primary dies

// 	// After 3 failed heartbeats the peer moves to the backup, finds that it
// 	// is not registered there (404), registers again, and keeps reporting.
// 	eventually(t, "re-registration at the backup superpeer", func() bool { return sp2.registerCount() >= 1 })
// 	eventually(t, "heartbeats at the backup superpeer", func() bool { return sp2.heartbeatCount(peer.peerID) > 2 })
// 	peer.quit()
// }

func TestWrongPasswordStopsStartupAndReleasesThePort(t *testing.T) {
	_, srv := startFakeSuperpeer(t)
	peer := startPeer(t, srv.URL, "ana", "wrong")

	select {
	case err := <-peer.done:
		if err == nil || !strings.Contains(err.Error(), "login") {
			t.Errorf("got %v, want a login error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the peer kept running after a failed login")
	}

	if conn, err := net.DialTimeout("tcp", "127.0.0.1:"+peer.port, time.Second); err == nil {
		conn.Close()
		t.Error("the port is still open after startup failed")
	}
}

func TestCommandsAfterErrorsKeepWorking(t *testing.T) {
	_, srv := startFakeSuperpeer(t)
	peer := startPeer(t, srv.URL, "ana", "secret")
	peer.waitReady()

	peer.say("cd nowhere")
	peer.waitFor("/nowhere: no such directory")
	peer.say("get /missing.txt")
	peer.waitFor("not found")
	peer.say("mkdir ok")
	peer.say("cd ok")
	peer.waitFor("dfsha:/ok> ")
	peer.quit()
}

func TestDecodeSalt(t *testing.T) {
	raw := []byte("0123456789abcdef") // 16 bytes
	for name, enc := range map[string]string{
		"padded":   base64.StdEncoding.EncodeToString(raw),
		"unpadded": base64.RawStdEncoding.EncodeToString(raw),
	} {
		got, err := decodeSalt(enc)
		if err != nil || !bytes.Equal(got, raw) {
			t.Errorf("%s: %q, %v", name, got, err)
		}
	}
	if _, err := decodeSalt("not base64 !!"); err == nil {
		t.Error("accepted garbage")
	}
}

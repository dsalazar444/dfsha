package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dfsha/internal/peer/client"
	"dfsha/internal/peer/crypto"
	"dfsha/internal/peer/hash"
	"dfsha/internal/peer/http_client"

	"dfsha/internal/shared/model"
)

// Compile-time checks: the real clients satisfy what transfer needs.
var _ SuperpeerAPI = (*client.SuperpeerClient)(nil)
var _ PeerAPI = (*client.PeerClient)(nil)

var errRefused = fmt.Errorf("connection refused: %w", http_client.ErrUnreachable)

// --- fake peer network -------------------------------------------------

// fakeNet plays every storage peer at once: it keeps what each URL holds
// and behaves like the real receiveChunk (verifies the hash, answers 400
// on mismatch).
type fakeNet struct {
	mu     sync.Mutex
	stored map[string]map[model.ChunkID][]byte // url -> chunk -> envelope

	down    map[string]bool // url -> every call fails as unreachable
	corrupt map[string]bool // url -> Fetch returns damaged bytes

	// sendHook, if set, can fail a send; attempt counts calls per url+chunk (1-based).
	sendHook func(url string, id model.ChunkID, attempt int) error

	attempts map[string]int
	sends    int32 // total SendChunk calls
	fetches  int32 // total FetchChunk calls

	inFlight, maxInFlight int32 // concurrency of SendChunk
	sendDelay             time.Duration
}

func newFakeNet() *fakeNet {
	return &fakeNet{
		stored:   map[string]map[model.ChunkID][]byte{},
		down:     map[string]bool{},
		corrupt:  map[string]bool{},
		attempts: map[string]int{},
	}
}

func (n *fakeNet) SendChunk(ctx context.Context, url, owner string, id model.ChunkID, envelope []byte, expectedHash string) error {
	atomic.AddInt32(&n.sends, 1)
	cur := atomic.AddInt32(&n.inFlight, 1)
	defer atomic.AddInt32(&n.inFlight, -1)
	for {
		max := atomic.LoadInt32(&n.maxInFlight)
		if cur <= max || atomic.CompareAndSwapInt32(&n.maxInFlight, max, cur) {
			break
		}
	}
	if n.sendDelay > 0 {
		time.Sleep(n.sendDelay)
	}

	n.mu.Lock()
	key := url + "|" + string(id)
	n.attempts[key]++
	attempt := n.attempts[key]
	down := n.down[url]
	hook := n.sendHook
	n.mu.Unlock()

	if down {
		return errRefused
	}
	if hook != nil {
		if err := hook(url, id, attempt); err != nil {
			return err
		}
	}
	if !hash.VerifySHA256(envelope, expectedHash) {
		return &http_client.StatusError{Code: 400, Message: "hash mismatch"}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stored[url] == nil {
		n.stored[url] = map[model.ChunkID][]byte{}
	}
	n.stored[url][id] = append([]byte(nil), envelope...)
	return nil
}

func (n *fakeNet) FetchChunk(ctx context.Context, url, owner string, id model.ChunkID) ([]byte, error) {
	atomic.AddInt32(&n.fetches, 1)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[url] {
		return nil, errRefused
	}
	env, ok := n.stored[url][id]
	if !ok {
		return nil, &http_client.StatusError{Code: 404, Message: "chunk not found"}
	}
	out := append([]byte(nil), env...)
	if n.corrupt[url] {
		out[len(out)/2] ^= 0xFF
	}
	return out, nil
}

// --- fake superpeer ----------------------------------------------------

// fakeSP assigns every chunk to the same peer list and, for get, reports
// the chunks that some peer really holds (like a superpeer that got acks).
type fakeSP struct {
	net   *fakeNet
	peers []string

	mu      sync.Mutex
	files   map[string]model.PutInitRequest // logical path -> put/init request
	putInit int

	mutatePut func(*model.PutInitResponse) // inject a lying put/init answer
	mutateGet func(*model.GetFileResponse) // inject a lying get answer
}

func (s *fakeSP) PutInit(ctx context.Context, req model.PutInitRequest) (model.PutInitResponse, error) {
	s.mu.Lock()
	s.putInit++
	if s.files == nil {
		s.files = map[string]model.PutInitRequest{}
	}
	s.files[req.LogicalPath] = req
	s.mu.Unlock()

	resp := model.PutInitResponse{FileID: req.FileID}
	for i := 0; i < req.ChunkCount; i++ {
		resp.Assignment = append(resp.Assignment, model.ChunkAssignment{
			ChunkID:  model.NewChunkID(req.FileID, i),
			PeerURLs: append([]string(nil), s.peers...),
		})
	}
	if s.mutatePut != nil {
		s.mutatePut(&resp)
	}
	return resp, nil
}

func (s *fakeSP) GetFile(ctx context.Context, logicalPath string) (model.GetFileResponse, error) {
	s.mu.Lock()
	req, ok := s.files[logicalPath]
	s.mu.Unlock()
	if !ok {
		return model.GetFileResponse{}, &http_client.StatusError{Code: 404, Message: "no such file"}
	}

	resp := model.GetFileResponse{FileID: req.FileID}
	s.net.mu.Lock()
	for i := 0; i < req.ChunkCount; i++ {
		id := model.NewChunkID(req.FileID, i)
		loc := model.ChunkLocation{ChunkID: id, ExpectedHash: req.HashPerChunk[i]}
		for _, url := range s.peers {
			if _, has := s.net.stored[url][id]; has {
				loc.PeerURLs = append(loc.PeerURLs, url)
			}
		}
		resp.Chunks = append(resp.Chunks, loc)
	}
	s.net.mu.Unlock()

	if s.mutateGet != nil {
		s.mutateGet(&resp)
	}
	return resp, nil
}

// --- helpers -----------------------------------------------------------

var testKey = bytes.Repeat([]byte{0x42}, crypto.KeySize)

func newEnv(t *testing.T, chunkSize int, peers ...string) (*Transfer, *fakeSP, *fakeNet) {
	t.Helper()
	net := newFakeNet()
	sp := &fakeSP{net: net, peers: peers}
	tr, err := New(Config{UserID: "user-1", MasterKey: testKey, ChunkSize: chunkSize, Parallelism: 3}, sp, net)
	if err != nil {
		t.Fatal(err)
	}
	tr.backoff = time.Millisecond
	return tr, sp, net
}

func sampleData(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

// --- round trip --------------------------------------------------------

func TestPutGetRoundTrip(t *testing.T) {
	// Sizes around the chunk boundary (10) and many chunks.
	for _, size := range []int{1, 9, 10, 11, 20, 95, 1000} {
		tr, _, _ := newEnv(t, 10, "http://p1", "http://p2")
		data := sampleData(size)

		if _, err := tr.Put(context.Background(), "/docs/f.bin", data); err != nil {
			t.Fatalf("size %d: Put: %v", size, err)
		}
		got, err := tr.Get(context.Background(), "/docs/f.bin")
		if err != nil {
			t.Fatalf("size %d: Get: %v", size, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("size %d: downloaded data differs from the original", size)
		}
	}
}

func TestStoredDataIsEncryptedAndHashed(t *testing.T) {
	tr, sp, net := newEnv(t, 16, "http://p1")
	secret := []byte("TOP-SECRET-CONTENT-THAT-MUST-NOT-LEAK-" + strings.Repeat("x", 40))

	fileID, err := tr.Put(context.Background(), "/s.txt", secret)
	if err != nil {
		t.Fatal(err)
	}

	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(fileID) {
		t.Errorf("file ID %q is not a UUIDv4", fileID)
	}
	req := sp.files["/s.txt"]
	if req.FileID != fileID {
		t.Error("the file ID sent to put/init is not the one Put returned")
	}

	chunks := net.stored["http://p1"]
	if len(chunks) != req.ChunkCount || req.ChunkCount != len(req.HashPerChunk) {
		t.Fatalf("chunk count mismatch: stored %d, request %d/%d", len(chunks), req.ChunkCount, len(req.HashPerChunk))
	}
	for i := 0; i < req.ChunkCount; i++ {
		env := chunks[model.NewChunkID(fileID, i)]
		if bytes.Contains(env, []byte("TOP-SECRET")) || bytes.Contains(env, []byte("xxxx")) {
			t.Errorf("chunk %d holds plaintext", i)
		}
		if hash.CalculateSHA256(env) != req.HashPerChunk[i] {
			t.Errorf("hash %d sent to put/init is not the SHA-256 of the stored envelope", i)
		}
	}
}

func TestEachUploadGetsItsOwnFileIDAndKey(t *testing.T) {
	tr, _, net := newEnv(t, 100, "http://p1")
	data := sampleData(50)

	id1, _ := tr.Put(context.Background(), "/a", data)
	id2, _ := tr.Put(context.Background(), "/b", data)
	if id1 == id2 {
		t.Fatal("two uploads got the same file ID")
	}
	e1 := net.stored["http://p1"][model.NewChunkID(id1, 0)]
	e2 := net.stored["http://p1"][model.NewChunkID(id2, 0)]
	if bytes.Equal(e1, e2) {
		t.Error("the same data produced identical envelopes")
	}
}

func TestNewFileID(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 2000; i++ {
		id, err := newFileID()
		if err != nil || strings.Contains(id, "_") || seen[id] {
			t.Fatalf("bad or repeated file ID %q (%v)", id, err)
		}
		seen[id] = true
	}
}

// --- put: failures -----------------------------------------------------

func TestPutToleratesADeadReplica(t *testing.T) {
	tr, _, net := newEnv(t, 10, "http://p1", "http://p2")
	net.down["http://p1"] = true
	data := sampleData(45)

	if _, err := tr.Put(context.Background(), "/f", data); err != nil {
		t.Fatalf("Put with one dead replica: %v", err)
	}
	if len(net.stored["http://p1"]) != 0 {
		t.Error("the dead peer stored something")
	}
	got, err := tr.Get(context.Background(), "/f")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get after partial replication: %v", err)
	}
}

func TestPutFailsWhenAChunkHasNoReplica(t *testing.T) {
	tr, _, net := newEnv(t, 10, "http://p1", "http://p2")
	net.down["http://p1"] = true
	net.down["http://p2"] = true

	_, err := tr.Put(context.Background(), "/f", sampleData(25))
	if err == nil {
		t.Fatal("Put succeeded with every peer down")
	}
	if !errors.Is(err, http_client.ErrUnreachable) {
		t.Errorf("the cause was lost: %v", err)
	}
	if !strings.Contains(err.Error(), "accepted by 0 of 2 peers") {
		t.Errorf("unhelpful message: %v", err)
	}
}

func TestPutRetriesTemporaryFailures(t *testing.T) {
	tr, _, net := newEnv(t, 100, "http://p1")
	net.sendHook = func(url string, id model.ChunkID, attempt int) error {
		if attempt < 3 {
			return errRefused // fails twice, works on the 3rd try
		}
		return nil
	}

	id, err := tr.Put(context.Background(), "/f", sampleData(50))
	if err != nil {
		t.Fatal(err)
	}
	if got := net.attempts["http://p1|"+string(model.NewChunkID(id, 0))]; got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestPutGivesUpAfterMaxAttempts(t *testing.T) {
	tr, _, net := newEnv(t, 100, "http://p1")
	net.sendHook = func(string, model.ChunkID, int) error { return errRefused }

	if _, err := tr.Put(context.Background(), "/f", sampleData(50)); err == nil {
		t.Fatal("expected an error")
	}
	// One chunk, one peer: exactly maxSendAttempts sends, no more.
	if total := atomic.LoadInt32(&net.sends); total != maxSendAttempts {
		t.Errorf("sends = %d, want %d", total, maxSendAttempts)
	}
}

func TestPutDoesNotRetryRefusals(t *testing.T) {
	tr, _, net := newEnv(t, 100, "http://p1", "http://p2")
	net.sendHook = func(url string, id model.ChunkID, attempt int) error {
		if url == "http://p1" {
			return &http_client.StatusError{Code: 400, Message: "hash mismatch"}
		}
		return nil
	}

	if _, err := tr.Put(context.Background(), "/f", sampleData(50)); err != nil {
		t.Fatalf("p2 accepted it, so Put must succeed: %v", err)
	}
	// 1 refused send to p1 (no retries) + 1 successful send to p2.
	if total := atomic.LoadInt32(&net.sends); total != 2 {
		t.Errorf("sends = %d, want 2 (a 400 must not be retried)", total)
	}
}

func TestPutRejectsBadAssignments(t *testing.T) {
	cases := map[string]func(*model.PutInitResponse){
		"wrong file ID":      func(r *model.PutInitResponse) { r.FileID = "someone-elses" },
		"missing chunk":      func(r *model.PutInitResponse) { r.Assignment = r.Assignment[:len(r.Assignment)-1] },
		"chunk without peer": func(r *model.PutInitResponse) { r.Assignment[0].PeerURLs = nil },
		"extra chunk": func(r *model.PutInitResponse) {
			r.Assignment = append(r.Assignment, model.ChunkAssignment{ChunkID: "zzz_99", PeerURLs: []string{"http://p1"}})
		},
		"duplicated chunk": func(r *model.PutInitResponse) { r.Assignment[1] = r.Assignment[0] },
		"foreign chunk ID": func(r *model.PutInitResponse) { r.Assignment[0].ChunkID = "other-file_0" },
	}
	for name, mutate := range cases {
		tr, sp, net := newEnv(t, 10, "http://p1")
		sp.mutatePut = mutate

		if _, err := tr.Put(context.Background(), "/f", sampleData(35)); err == nil {
			t.Errorf("%s: Put accepted the assignment", name)
		}
		if atomic.LoadInt32(&net.sends) != 0 {
			t.Errorf("%s: chunks were sent before the assignment was validated", name)
		}
	}
}

func TestPutEmptyFile(t *testing.T) {
	tr, sp, _ := newEnv(t, 10, "http://p1")
	if _, err := tr.Put(context.Background(), "/f", nil); !errors.Is(err, ErrEmptyFile) {
		t.Errorf("got %v, want ErrEmptyFile", err)
	}
	if sp.putInit != 0 {
		t.Error("put/init was called for an empty file")
	}
}

func TestPutRespectsParallelismLimit(t *testing.T) {
	net := newFakeNet()
	net.sendDelay = 5 * time.Millisecond
	sp := &fakeSP{net: net, peers: []string{"http://p1", "http://p2"}}
	tr, _ := New(Config{UserID: "u", MasterKey: testKey, ChunkSize: 10, Parallelism: 2}, sp, net)

	if _, err := tr.Put(context.Background(), "/f", sampleData(100)); err != nil { // 10 chunks x 2 peers
		t.Fatal(err)
	}
	if max := atomic.LoadInt32(&net.maxInFlight); max > 2 || max < 1 {
		t.Errorf("max simultaneous sends = %d, want between 1 and 2", max)
	}
}

func TestPutCancelled(t *testing.T) {
	tr, _, _ := newEnv(t, 10, "http://p1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := tr.Put(ctx, "/f", sampleData(100)); !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
}

// --- get ---------------------------------------------------------------

func TestGetFallsBackFromCorruptAndDeadPeers(t *testing.T) {
	tr, _, net := newEnv(t, 10, "http://p1", "http://p2", "http://p3")
	data := sampleData(95)
	if _, err := tr.Put(context.Background(), "/f", data); err != nil {
		t.Fatal(err)
	}

	// After the upload: p1 serves damaged bytes, p2 goes down, only p3 is good.
	net.corrupt["http://p1"] = true
	net.down["http://p2"] = true

	// Repeat: peer order is random, every order must work.
	for i := 0; i < 10; i++ {
		got, err := tr.Get(context.Background(), "/f")
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("round %d: %v", i, err)
		}
	}
}

func TestGetFailsWhenEveryCopyIsCorrupt(t *testing.T) {
	tr, _, net := newEnv(t, 10, "http://p1", "http://p2")
	if _, err := tr.Put(context.Background(), "/f", sampleData(30)); err != nil {
		t.Fatal(err)
	}
	net.corrupt["http://p1"] = true
	net.corrupt["http://p2"] = true

	_, err := tr.Get(context.Background(), "/f")
	if err == nil || !strings.Contains(err.Error(), "no peer could serve a valid copy") {
		t.Errorf("got %v", err)
	}
}

func TestGetWithWrongKeyFailsCleanly(t *testing.T) {
	tr, sp, net := newEnv(t, 10, "http://p1")
	if _, err := tr.Put(context.Background(), "/f", sampleData(30)); err != nil {
		t.Fatal(err)
	}

	otherKey := bytes.Repeat([]byte{0x99}, crypto.KeySize)
	intruder, _ := New(Config{UserID: "user-1", MasterKey: otherKey, ChunkSize: 10}, sp, net)

	_, err := intruder.Get(context.Background(), "/f")
	if err == nil || !strings.Contains(err.Error(), "could not be decrypted") {
		t.Errorf("got %v, want a decryption error", err)
	}
}

func TestGetDetectsChunkAlteredBeforeUpload(t *testing.T) {
	tr, sp, net := newEnv(t, 10, "http://p1")
	id, err := tr.Put(context.Background(), "/f", sampleData(30))
	if err != nil {
		t.Fatal(err)
	}

	// Damage a stored envelope AND make the superpeer's hash match it, so
	// the hash check passes and only AES-GCM can catch it.
	chunk := model.NewChunkID(id, 1)
	net.stored["http://p1"][chunk][20] ^= 0x01
	req := sp.files["/f"]
	req.HashPerChunk[1] = hash.CalculateSHA256(net.stored["http://p1"][chunk])
	sp.files["/f"] = req

	if _, err := tr.Get(context.Background(), "/f"); err == nil || !strings.Contains(err.Error(), "could not be decrypted") {
		t.Errorf("got %v, want a decryption error", err)
	}
}

func TestGetSwappedChunksAreRejected(t *testing.T) {
	// A malicious peer serves chunk 1's (valid) envelope as chunk 0. The
	// superpeer's hash for chunk 0 is then wrong too, but even if the
	// attacker also controlled it, the AAD binds each envelope to its slot.
	tr, sp, net := newEnv(t, 10, "http://p1")
	id, _ := tr.Put(context.Background(), "/f", sampleData(30))

	c0, c1 := model.NewChunkID(id, 0), model.NewChunkID(id, 1)
	net.stored["http://p1"][c0] = net.stored["http://p1"][c1]
	req := sp.files["/f"]
	req.HashPerChunk[0] = req.HashPerChunk[1]
	sp.files["/f"] = req

	if _, err := tr.Get(context.Background(), "/f"); err == nil {
		t.Fatal("a swapped chunk was accepted")
	}
}

func TestGetReordersChunks(t *testing.T) {
	tr, sp, _ := newEnv(t, 10, "http://p1")
	data := sampleData(95)
	if _, err := tr.Put(context.Background(), "/f", data); err != nil {
		t.Fatal(err)
	}
	sp.mutateGet = func(r *model.GetFileResponse) { // reverse the list
		for i, j := 0, len(r.Chunks)-1; i < j; i, j = i+1, j-1 {
			r.Chunks[i], r.Chunks[j] = r.Chunks[j], r.Chunks[i]
		}
	}

	got, err := tr.Get(context.Background(), "/f")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("out-of-order answer not handled: %v", err)
	}
}

func TestGetRejectsBadSuperpeerAnswers(t *testing.T) {
	cases := map[string]func(*model.GetFileResponse){
		"no file ID":          func(r *model.GetFileResponse) { r.FileID = "" },
		"no chunks":           func(r *model.GetFileResponse) { r.Chunks = nil },
		"duplicated chunk":    func(r *model.GetFileResponse) { r.Chunks[1] = r.Chunks[0] },
		"chunk of other file": func(r *model.GetFileResponse) { r.Chunks[0].ChunkID = "other_0" },
		"index out of range":  func(r *model.GetFileResponse) { r.Chunks[0].ChunkID = model.NewChunkID(r.FileID, 50) },
		"malformed chunk ID":  func(r *model.GetFileResponse) { r.Chunks[0].ChunkID = "nounderscore" },
		"no hash":             func(r *model.GetFileResponse) { r.Chunks[0].ExpectedHash = "" },
		"no peers":            func(r *model.GetFileResponse) { r.Chunks[0].PeerURLs = nil },
	}
	for name, mutate := range cases {
		tr, sp, net := newEnv(t, 10, "http://p1")
		if _, err := tr.Put(context.Background(), "/f", sampleData(35)); err != nil {
			t.Fatal(err)
		}
		sp.mutateGet = mutate
		before := atomic.LoadInt32(&net.fetches)

		if _, err := tr.Get(context.Background(), "/f"); err == nil {
			t.Errorf("%s: Get accepted the answer", name)
		}
		if atomic.LoadInt32(&net.fetches) != before {
			t.Errorf("%s: chunks were fetched before the answer was validated", name)
		}
	}
}

// A superpeer that omits the last chunk (say, one with no ack yet) must
// not make Get return a truncated file. The list alone looks complete, so
// only the authenticated chunk count can catch it.
func TestGetDetectsTruncatedChunkList(t *testing.T) {
	for _, drop := range []string{"last", "first"} {
		tr, sp, _ := newEnv(t, 10, "http://p1")
		if _, err := tr.Put(context.Background(), "/f", sampleData(35)); err != nil {
			t.Fatal(err)
		}
		sp.mutateGet = func(r *model.GetFileResponse) {
			if drop == "last" {
				r.Chunks = r.Chunks[:len(r.Chunks)-1] // looks complete: only the AAD can tell
			} else {
				r.Chunks = r.Chunks[1:] // indices 1..3 with n=3: out of range, rejected by orderChunks
			}
		}

		got, err := tr.Get(context.Background(), "/f")
		if err == nil {
			t.Errorf("drop %s: accepted a truncated file (%d bytes)", drop, len(got))
		}
	}
}

func TestGetUnknownFile(t *testing.T) {
	tr, _, _ := newEnv(t, 10, "http://p1")
	_, err := tr.Get(context.Background(), "/nope")
	if !errors.Is(err, http_client.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

// --- config and helpers --------------------------------------------------

func TestNewValidatesConfig(t *testing.T) {
	good := Config{UserID: "u", MasterKey: testKey}
	if _, err := New(good, &fakeSP{}, newFakeNet()); err != nil {
		t.Fatal(err)
	}
	bad := []Config{
		{MasterKey: testKey},
		{UserID: "u"},
		{UserID: "u", MasterKey: testKey[:16]},
		{UserID: "u", MasterKey: testKey, ChunkSize: -1},
		{UserID: "u", MasterKey: testKey, Parallelism: -1},
	}
	for i, cfg := range bad {
		if _, err := New(cfg, &fakeSP{}, newFakeNet()); err == nil {
			t.Errorf("bad config %d accepted", i)
		}
	}
	tr, _ := New(good, &fakeSP{}, newFakeNet())
	if tr.chunkSize != defaultChunkSize || tr.parallelism != defaultParallelism {
		t.Errorf("defaults not applied: %d, %d", tr.chunkSize, tr.parallelism)
	}
}

func TestForEachFirstErrorCancelsTheRest(t *testing.T) {
	boom := errors.New("boom")
	var started int32

	err := forEach(context.Background(), 100, 2, func(ctx context.Context, i int) error {
		atomic.AddInt32(&started, 1)
		if i == 3 {
			return boom
		}
		select {
		case <-ctx.Done():
			return ctx.Err() // cancelled siblings must not replace the first error
		case <-time.After(20 * time.Millisecond):
			return nil
		}
	})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the first error", err)
	}
	if s := atomic.LoadInt32(&started); s >= 100 {
		t.Errorf("all %d calls started despite the error", s)
	}
}

func TestForEachRunsEverythingOnSuccess(t *testing.T) {
	var sum int64
	err := forEach(context.Background(), 50, 4, func(ctx context.Context, i int) error {
		atomic.AddInt64(&sum, int64(i))
		return nil
	})
	if err != nil || sum != 49*50/2 {
		t.Errorf("sum = %d, err = %v", sum, err)
	}
}

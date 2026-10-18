package peers_test

import (
	"bytes"
	"dfsha/internal/superpeer/auth"
	"dfsha/internal/superpeer/config"
	"dfsha/internal/superpeer/files"
	"dfsha/internal/superpeer/fs"
	"dfsha/internal/superpeer/locks"
	"dfsha/internal/superpeer/peers"
	"dfsha/internal/superpeer/server"
	"dfsha/internal/superpeer/store"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestHeartbeatRepeatsTaskUntilAck validates that:
// 1. When a chunk is under-replicated, the heartbeat gives a "replicate" task.
// 2. In subsequent heartbeats, the task is REPEATED if no ACK was sent.
// 3. Once the peer sends put/ack, subsequent heartbeats NO LONGER contain the task.
func TestHeartbeatRepeatsTaskUntilAck(t *testing.T) {
	userStore := store.NewUserStore()
	peerStore := store.NewPeerStore()
	fsStore := store.NewFsStore()
	fileStore := store.NewFileStore()
	lockStore := store.NewLockStore()
	taskStore := store.NewTaskStore()

	cfg := &config.Config{
		Port:              "8080",
		ChunkSize:         4 * 1024 * 1024,
		ReplicationFactor: 2,
		HeartbeatTimeout:  15 * time.Second,
		SessionSecret:     []byte("testsecret0123456789012345678901"),
	}

	authService := auth.NewService(userStore, cfg.SessionSecret)
	peerService := peers.NewService(peerStore, fileStore, taskStore, cfg.HeartbeatTimeout, cfg.ReplicationFactor)
	fileService := files.NewService(fsStore, fileStore, peerStore, cfg.ReplicationFactor, files.NewTaskChunkDeleter(taskStore))
	fsService := fs.NewService(fsStore, fileService)
	lockService := locks.NewService(lockStore, fsStore, 30*time.Second)

	authHandler := auth.NewHandler(authService)
	peerHandler := peers.NewHandler(peerService)
	fsHandler := fs.NewHandler(fsService)
	fileHandler := files.NewHandler(fileService)
	lockHandler := locks.NewHandler(lockService)

	router := server.NewRouter(cfg, authService, authHandler, peerHandler, fsHandler, fileHandler, lockHandler)
	ts := httptest.NewServer(router)
	defer ts.Close()

	client := ts.Client()

	// 1. Login
	loginBody, _ := json.Marshal(map[string]string{"username": "alice", "password": "alice1234"})
	resp, err := client.Post(ts.URL+"/auth/login", "application/json", bytes.NewReader(loginBody))
	if err != nil {
		t.Fatal(err)
	}
	var authRes struct {
		SessionToken string `json:"session_token"`
		UserID       string `json:"user_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&authRes)
	resp.Body.Close()
	token := authRes.SessionToken

	doAuthReq := func(method, path string, body []byte) *http.Response {
		var req *http.Request
		if body != nil {
			req, _ = http.NewRequest(method, ts.URL+path, bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
		} else {
			req, _ = http.NewRequest(method, ts.URL+path, nil)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		r, err := client.Do(req)
		if err != nil {
			t.Fatalf("Request %s %s failed: %v", method, path, err)
		}
		return r
	}

	// 2. Register Peer 1 (source) and Peer 2 (target)
	p1Body, _ := json.Marshal(map[string]string{"address": "127.0.0.1", "port": "9001"})
	r1 := doAuthReq(http.MethodPost, "/peers/register", p1Body)
	var p1Data struct{ PeerID string `json:"peer_id"` }
	_ = json.NewDecoder(r1.Body).Decode(&p1Data)
	r1.Body.Close()

	p2Body, _ := json.Marshal(map[string]string{"address": "127.0.0.1", "port": "9002"})
	r2 := doAuthReq(http.MethodPost, "/peers/register", p2Body)
	var p2Data struct{ PeerID string `json:"peer_id"` }
	_ = json.NewDecoder(r2.Body).Decode(&p2Data)
	r2.Body.Close()

	// 3. Upload a file where ONLY Peer 1 receives and ACKs the chunk
	initBody, _ := json.Marshal(map[string]interface{}{
		"logical_path":        "/data.bin",
		"chunk_count":         1,
		"hash_of_each_chunk": []string{"testhash123"},
	})
	rInit := doAuthReq(http.MethodPost, "/files/put/init", initBody)
	var initData struct {
		Assignment []struct {
			ChunkID string `json:"chunk_id"`
		} `json:"assignment"`
	}
	_ = json.NewDecoder(rInit.Body).Decode(&initData)
	rInit.Body.Close()
	chunkID := initData.Assignment[0].ChunkID

	// Peer 1 sends ACK -> It now has 1 healthy acked copy (ReplicationFactor is 2, so it's under-replicated)
	ack1Body, _ := json.Marshal(map[string]string{"chunk_id": chunkID, "peer_id": p1Data.PeerID})
	rAck1 := doAuthReq(http.MethodPost, "/files/put/ack", ack1Body)
	rAck1.Body.Close()

	// 4. Peer 2 sends first Heartbeat -> MUST receive replication task for chunkID
	hb2Body, _ := json.Marshal(map[string]interface{}{
		"peer_id":         p2Data.PeerID,
		"available_space": 5000000,
		"chunks":          []string{},
	})
	rHb1 := doAuthReq(http.MethodPost, "/peers/heartbeat", hb2Body)
	var hb1Res struct {
		Tasks []struct {
			Action        string `json:"action"`
			ChunkID       string `json:"chunk_id"`
			SourcePeerURL string `json:"source_peer_url"`
		} `json:"tasks"`
	}
	_ = json.NewDecoder(rHb1.Body).Decode(&hb1Res)
	rHb1.Body.Close()

	if len(hb1Res.Tasks) == 0 || hb1Res.Tasks[0].ChunkID != chunkID {
		t.Fatalf("Heartbeat 1: expected replication task for %s, got: %+v", chunkID, hb1Res.Tasks)
	}

	// 5. Peer 2 sends second Heartbeat (WITHOUT sending ACK yet) -> MUST REPEAT the task!
	rHb2 := doAuthReq(http.MethodPost, "/peers/heartbeat", hb2Body)
	var hb2Res struct {
		Tasks []struct {
			Action  string `json:"action"`
			ChunkID string `json:"chunk_id"`
		} `json:"tasks"`
	}
	_ = json.NewDecoder(rHb2.Body).Decode(&hb2Res)
	rHb2.Body.Close()

	if len(hb2Res.Tasks) == 0 || hb2Res.Tasks[0].ChunkID != chunkID {
		t.Fatalf("Heartbeat 2: expected task to REPEAT, but got %+v", hb2Res.Tasks)
	}

	// 6. Peer 2 finally downloads the chunk and sends put/ack
	ack2Body, _ := json.Marshal(map[string]string{"chunk_id": chunkID, "peer_id": p2Data.PeerID})
	rAck2 := doAuthReq(http.MethodPost, "/files/put/ack", ack2Body)
	if rAck2.StatusCode != http.StatusOK {
		t.Fatalf("Peer 2 ACK failed: %d", rAck2.StatusCode)
	}
	rAck2.Body.Close()

	// 7. Peer 2 sends third Heartbeat reporting the downloaded chunk -> Tasks list MUST now be EMPTY!
	hb2WithChunk, _ := json.Marshal(map[string]interface{}{
		"peer_id":         p2Data.PeerID,
		"available_space": 5000000,
		"chunks":          []string{chunkID},
	})
	rHb3 := doAuthReq(http.MethodPost, "/peers/heartbeat", hb2WithChunk)
	var hb3Res struct {
		Tasks []interface{} `json:"tasks"`
	}
	_ = json.NewDecoder(rHb3.Body).Decode(&hb3Res)
	rHb3.Body.Close()

	if len(hb3Res.Tasks) != 0 {
		t.Fatalf("Heartbeat 3: expected 0 tasks after ACK, but got %d tasks: %+v", len(hb3Res.Tasks), hb3Res.Tasks)
	}
}

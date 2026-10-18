package main_test

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

// TestSuperpeerEndToEndFlow runs a complete smoke test covering all phases:
// 1. Login (Alice)
// 2. Register Storage Peers (Peer A & Peer B)
// 3. Heartbeats
// 4. Create Directory (mkdir)
// 5. Put Init (upload file metadata)
// 6. Put Ack (peers confirm chunk storage)
// 7. Get File (download chunk coordinates and hash)
// 8. Lock & Unlock (concurrency control)
// 9. Delete File (eventual deletion)
// 10. List Directory (ls verify file removed)

func TestSuperpeerEndToEndFlow(t *testing.T) {
	// Initialize full environment
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

	// --- Step 1: Login ---
	loginPayload, _ := json.Marshal(map[string]string{
		"username": "alice",
		"password": "alice1234",
	})
	resp, err := client.Post(ts.URL+"/auth/login", "application/json", bytes.NewReader(loginPayload))
	if err != nil {
		t.Fatalf("Login failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Login status expected 200, got %d", resp.StatusCode)
	}

	var authData struct {
		SessionToken string `json:"session_token"`
		UserID       string `json:"user_id"`
		Salt         string `json:"salt"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&authData)
	resp.Body.Close()

	if authData.SessionToken == "" || authData.UserID == "" || authData.Salt == "" {
		t.Fatalf("Login response missing fields: %+v", authData)
	}
	token := authData.SessionToken

	// Helper for authenticated requests
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
			t.Fatalf("HTTP %s %s failed: %v", method, path, err)
		}
		return r
	}

	// --- Step 2: Register Peers ---
	p1Payload, _ := json.Marshal(map[string]string{"address": "127.0.0.1", "port": "9001"})
	r1 := doAuthReq(http.MethodPost, "/peers/register", p1Payload)
	if r1.StatusCode != http.StatusOK {
		t.Fatalf("Peer 1 registration failed: %d", r1.StatusCode)
	}
	var p1Data struct {
		PeerID string `json:"peer_id"`
	}
	_ = json.NewDecoder(r1.Body).Decode(&p1Data)
	r1.Body.Close()

	p2Payload, _ := json.Marshal(map[string]string{"address": "127.0.0.1", "port": "9002"})
	r2 := doAuthReq(http.MethodPost, "/peers/register", p2Payload)
	var p2Data struct {
		PeerID string `json:"peer_id"`
	}
	_ = json.NewDecoder(r2.Body).Decode(&p2Data)
	r2.Body.Close()

	// --- Step 3: Peer Heartbeats ---
	hbPayload, _ := json.Marshal(map[string]interface{}{
		"peer_id":         p1Data.PeerID,
		"available_space": 1000000000,
		"chunks":          []string{},
	})
	rHb := doAuthReq(http.MethodPost, "/peers/heartbeat", hbPayload)
	if rHb.StatusCode != http.StatusOK {
		t.Fatalf("Heartbeat failed: %d", rHb.StatusCode)
	}
	rHb.Body.Close()

	// --- Step 4: Make Directory /docs ---
	mkdirPayload, _ := json.Marshal(map[string]string{"path": "/docs"})
	rMkdir := doAuthReq(http.MethodPost, "/fs/mkdir", mkdirPayload)
	if rMkdir.StatusCode != http.StatusOK {
		t.Fatalf("Mkdir failed: %d", rMkdir.StatusCode)
	}
	rMkdir.Body.Close()

	// --- Step 5: Put Init (/docs/thesis.pdf, 1 chunk) ---
	putInitPayload, _ := json.Marshal(map[string]interface{}{
		"logical_path":        "/docs/thesis.pdf",
		"chunk_count":         1,
		"hash_of_each_chunk": []string{"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
	})
	rInit := doAuthReq(http.MethodPost, "/files/put/init", putInitPayload)
	if rInit.StatusCode != http.StatusOK {
		t.Fatalf("PutInit failed: %d", rInit.StatusCode)
	}
	var initResp struct {
		FileID     string `json:"file_id"`
		Assignment []struct {
			ChunkID          string   `json:"chunk_id"`
			DestinationPeers []string `json:"destination_peers"`
		} `json:"assignment"`
	}
	_ = json.NewDecoder(rInit.Body).Decode(&initResp)
	rInit.Body.Close()

	if len(initResp.Assignment) == 0 || len(initResp.Assignment[0].DestinationPeers) != 2 {
		t.Fatalf("Invalid assignment: %+v", initResp)
	}
	chunkID := initResp.Assignment[0].ChunkID

	// Test custom file_id with uniqueness validation
	customPutPayload, _ := json.Marshal(map[string]interface{}{
		"file_id":             "my-custom-encrypted-id-999",
		"logical_path":        "/docs/notes.txt",
		"chunk_count":         1,
		"hash_of_each_chunk": []string{"abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"},
	})
	rCustom := doAuthReq(http.MethodPost, "/files/put/init", customPutPayload)
	if rCustom.StatusCode != http.StatusOK {
		t.Fatalf("Custom file_id put/init failed: %d", rCustom.StatusCode)
	}
	var customResp struct {
		FileID string `json:"file_id"`
	}
	_ = json.NewDecoder(rCustom.Body).Decode(&customResp)
	rCustom.Body.Close()
	if customResp.FileID != "my-custom-encrypted-id-999" {
		t.Fatalf("Expected file_id to be preserved, got %s", customResp.FileID)
	}

	// Submitting the same file_id again must be rejected with 409 Conflict
	duplicateFileIDPayload, _ := json.Marshal(map[string]interface{}{
		"file_id":             "my-custom-encrypted-id-999",
		"logical_path":        "/docs/other.txt",
		"chunk_count":         1,
		"hash_of_each_chunk": []string{"abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"},
	})
	rConflict := doAuthReq(http.MethodPost, "/files/put/init", duplicateFileIDPayload)
	if rConflict.StatusCode != http.StatusConflict {
		t.Fatalf("Expected 409 Conflict for duplicate file_id, got %d", rConflict.StatusCode)
	}
	rConflict.Body.Close()

	// --- Step 6: Put Ack (from Peer 1 & Peer 2) ---
	ackPayload1, _ := json.Marshal(map[string]string{"chunk_id": chunkID, "peer_id": p1Data.PeerID})
	rAck1 := doAuthReq(http.MethodPost, "/files/put/ack", ackPayload1)
	if rAck1.StatusCode != http.StatusOK {
		t.Fatalf("Ack 1 failed: %d", rAck1.StatusCode)
	}
	rAck1.Body.Close()

	ackPayload2, _ := json.Marshal(map[string]string{"chunk_id": chunkID, "peer_id": p2Data.PeerID})
	rAck2 := doAuthReq(http.MethodPost, "/files/put/ack", ackPayload2)
	if rAck2.StatusCode != http.StatusOK {
		t.Fatalf("Ack 2 failed: %d", rAck2.StatusCode)
	}
	rAck2.Body.Close()

	// --- Step 7: Get File ---
	rGet := doAuthReq(http.MethodGet, "/files/get/docs/thesis.pdf", nil)
	if rGet.StatusCode != http.StatusOK {
		t.Fatalf("Get file failed: %d", rGet.StatusCode)
	}
	var getResp struct {
		FileID string `json:"file_id"`
		Chunks []struct {
			ChunkID string   `json:"chunk_id"`
			Hash    string   `json:"hash"`
			Peers   []string `json:"peers"`
		} `json:"chunks"`
	}
	_ = json.NewDecoder(rGet.Body).Decode(&getResp)
	rGet.Body.Close()

	if len(getResp.Chunks) == 0 || len(getResp.Chunks[0].Peers) != 2 {
		t.Fatalf("Expected 2 acked healthy peers in get, got %+v", getResp)
	}

	// --- Step 8: Lock & Unlock ---
	rLock := doAuthReq(http.MethodPost, "/files/lock/docs/thesis.pdf", nil)
	if rLock.StatusCode != http.StatusOK {
		t.Fatalf("Lock failed: %d", rLock.StatusCode)
	}
	var lockData struct {
		LockID    string `json:"lock_id"`
		ExpiresAt string `json:"expires_at"`
	}
	_ = json.NewDecoder(rLock.Body).Decode(&lockData)
	rLock.Body.Close()

	// Second lock should fail with 409 Conflict
	rLockConflict := doAuthReq(http.MethodPost, "/files/lock/docs/thesis.pdf", nil)
	if rLockConflict.StatusCode != http.StatusConflict {
		t.Fatalf("Expected 409 Conflict on double lock, got %d", rLockConflict.StatusCode)
	}
	rLockConflict.Body.Close()

	// Unlock
	unlockPayload, _ := json.Marshal(map[string]string{"lock_id": lockData.LockID})
	rUnlock := doAuthReq(http.MethodPost, "/files/unlock/docs/thesis.pdf", unlockPayload)
	if rUnlock.StatusCode != http.StatusOK {
		t.Fatalf("Unlock failed: %d", rUnlock.StatusCode)
	}
	rUnlock.Body.Close()

	// --- Step 9: Delete File ---
	rDel := doAuthReq(http.MethodDelete, "/files/docs/thesis.pdf", nil)
	if rDel.StatusCode != http.StatusOK {
		t.Fatalf("Delete file failed: %d", rDel.StatusCode)
	}
	rDel.Body.Close()

	// Also delete /docs/notes.txt
	rDelNotes := doAuthReq(http.MethodDelete, "/files/docs/notes.txt", nil)
	if rDelNotes.StatusCode != http.StatusOK {
		t.Fatalf("Delete notes failed: %d", rDelNotes.StatusCode)
	}
	rDelNotes.Body.Close()

	// Verify file is gone
	rGetGone := doAuthReq(http.MethodGet, "/files/get/docs/thesis.pdf", nil)
	if rGetGone.StatusCode != http.StatusNotFound {
		t.Fatalf("Expected 404 for deleted file, got %d", rGetGone.StatusCode)
	}
	rGetGone.Body.Close()

	// --- Step 10: List Directory ---
	rLs := doAuthReq(http.MethodGet, "/fs/ls?path=/docs", nil)
	if rLs.StatusCode != http.StatusOK {
		t.Fatalf("Ls failed: %d", rLs.StatusCode)
	}
	var lsData struct {
		Entries []interface{} `json:"entries"`
	}
	_ = json.NewDecoder(rLs.Body).Decode(&lsData)
	rLs.Body.Close()

	if len(lsData.Entries) != 0 {
		t.Fatalf("Expected empty directory after deletion, got %d entries", len(lsData.Entries))
	}
}

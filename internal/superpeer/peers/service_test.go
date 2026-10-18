package peers

import (
	"bytes"
	"dfsha/internal/superpeer/store"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// newTestService crea el servicio con dos peers registrados (p1 en host1, p2 en host2) y replicación 2
func newTestService() (*Service, *store.FileStore, *store.TaskStore, string, string) {
	fileStore, taskStore := store.NewFileStore(), store.NewTaskStore()
	s := NewService(store.NewPeerStore(), fileStore, taskStore, time.Minute, 2)
	p1 := s.Register("host1", "9000")
	p2 := s.Register("host2", "9000")
	return s, fileStore, taskStore, p1, p2
}

func TestHeartbeatDeliversDeleteTasksUntilDone(t *testing.T) {
	s, _, taskStore, p1, _ := newTestService()
	taskStore.AddDelete("http://host1:9000", "f1_0", "u1")

	// p1 todavía tiene el chunk: recibe la tarea DELETE
	tasks, err := s.Heartbeat(p1, 1000, []string{"f1_0"})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Action != ActionDelete || tasks[0].ChunkID != "f1_0" || tasks[0].OwnerUserID != "u1" {
		t.Fatalf("tasks = %+v, want one DELETE of f1_0 for u1", tasks)
	}

	// p1 ya lo borró (no lo reporta): la tarea se da por cumplida y no se reenvía
	tasks, _ = s.Heartbeat(p1, 1000, []string{})
	if len(tasks) != 0 {
		t.Fatalf("tasks = %+v, want none after the chunk is gone", tasks)
	}
}

func TestHeartbeatReplicationTaskFormat(t *testing.T) {
	s, fileStore, _, p1, p2 := newTestService()
	// El chunk solo está en p1, pero la replicación pide 2 copias
	fileStore.AddFile(&store.FileMeta{FileID: "f1", OwnerUserID: "u1", Chunks: []*store.ChunkMeta{
		{ChunkID: "f1_0", ExpectedHash: "hash0", Peers: []string{"http://host1:9000"}},
	}})
	if _, err := s.Heartbeat(p1, 1000, []string{"f1_0"}); err != nil {
		t.Fatal(err)
	}

	// p2 recibe la orden de copiarlo desde p1, con dueño y hash
	tasks, _ := s.Heartbeat(p2, 1000, []string{})
	if len(tasks) != 1 {
		t.Fatalf("tasks = %+v, want one GET", tasks)
	}
	got := tasks[0]
	if got.Action != ActionGet || got.OwnerUserID != "u1" || got.ExpectedHash != "hash0" ||
		!slices.Equal(got.SourceURLs, []string{"http://host1:9000"}) {
		t.Fatalf("task = %+v, want GET f1_0 from host1 for u1", got)
	}
}

// El JSON del heartbeat debe usar los nombres del contrato, que son los que lee el peer
func TestHeartbeatHandlerJSON(t *testing.T) {
	s, _, taskStore, p1, _ := newTestService()
	taskStore.AddDelete("http://host1:9000", "f1_0", "u1")

	body, _ := json.Marshal(map[string]any{"peer_id": p1, "available_space": 1000, "chunks": []string{"f1_0"}})
	rec := httptest.NewRecorder()
	NewHandler(s).Heartbeat(rec, httptest.NewRequest(http.MethodPost, "/peers/heartbeat", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}

	var resp struct {
		Tasks []map[string]any `json:"tasks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil || len(resp.Tasks) != 1 {
		t.Fatalf("response = %s", rec.Body)
	}
	for _, key := range []string{"action", "chunk_id", "owner_user_id", "source_urls", "expected_hash"} {
		if _, ok := resp.Tasks[0][key]; !ok {
			t.Fatalf("task without %q: %s", key, rec.Body)
		}
	}
}

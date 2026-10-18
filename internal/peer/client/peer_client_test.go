package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"dfsha/internal/peer/http_client"
	"dfsha/internal/shared/model"
)

func newTestClient() *http_client.HTTPClient { return http_client.NewHTTPClient(2 * time.Second) }

// NOTA: srv es peer que es contactado, client es el que contacta
func TestFetchChunk(t *testing.T) {
	var gotMethod, gotPath, gotOwner string

	// creamos servidor temporal, que es quien recibirá nuestras peticiones (peer al que queremos contactar), con handler HTTP
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// el handler lo unico que hace es guardar los valores de la request, y escribir en w byte[]
		gotMethod, gotPath, gotOwner = r.Method, r.URL.Path, r.URL.Query().Get("owner_user_id")
		w.Write([]byte{0, 1, 2, 255}) // binary, not text
	}))
	defer srv.Close()

	// creamos peer que contactará al server
	pc := NewPeerClient(newTestClient())

	data, err := pc.FetchChunk(context.Background(), srv.URL, "user-1", "file-9_3") //srv.URL es la dirección temporal del server
	if err != nil {
		t.Fatal(err)
	}
	// que data no este alterada
	if !bytes.Equal(data, []byte{0, 1, 2, 255}) {
		t.Errorf("binary data altered: %v", data)
	}
	// que info de peticion que llegó a server esté bien
	if gotMethod != "GET" || gotPath != "/chunks/file-9_3" || gotOwner != "user-1" {
		t.Errorf("request was %s %s owner=%q", gotMethod, gotPath, gotOwner)
	}
}

func TestFetchChunkNotFoundIsErrNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // siempre retorna not found
		http.Error(w, "chunk not found", http.StatusNotFound)
	}))
	defer srv.Close()

	// creamos peer que contactará al server
	pc := NewPeerClient(newTestClient())

	_, err := pc.FetchChunk(context.Background(), srv.URL, "u", "f_0")
	if !errors.Is(err, http_client.ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestDeleteChunk(t *testing.T) {
	var gotMethod, gotPath, gotOwner string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotOwner = r.Method, r.URL.Path, r.URL.Query().Get("owner_user_id")
	}))
	defer srv.Close()

	// creamos peer que contactará al server
	pc := NewPeerClient(newTestClient())

	if err := pc.DeleteChunk(context.Background(), srv.URL, "u1", "f_0"); err != nil { // DELETE esdempotente, si archivo no existe, no hay problema
		t.Fatal(err)
	}
	if gotMethod != "DELETE" || gotPath != "/chunks/f_0" || gotOwner != "u1" {
		t.Errorf("request was %s %s owner=%q", gotMethod, gotPath, gotOwner)
	}
}

func TestSendChunkMultipart(t *testing.T) {
	envelope := []byte{9, 8, 7, 0, 1, 2} // binary
	var fields map[string]string

	var gotFile []byte
	var gotMethod, gotPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		// Parse it exactly as the real receiveChunk handler does.
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		fields = map[string]string{
			"chunk_id":      r.FormValue("chunk_id"),
			"owner_user_id": r.FormValue("owner_user_id"),
			"expected_hash": r.FormValue("expected_hash"),
		}
		f, _, err := r.FormFile("chunk")
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		gotFile, _ = io.ReadAll(f)
	}))
	defer srv.Close()

	err := NewPeerClient(newTestClient()).SendChunk(context.Background(), srv.URL, "user-1", "file-9_3", envelope, "abc123")
	if err != nil {
		t.Fatal(err)
	}
	if gotMethod != "POST" || gotPath != "/chunks/receive" {
		t.Errorf("request was %s %s", gotMethod, gotPath)
	}
	if fields["chunk_id"] != "file-9_3" || fields["owner_user_id"] != "user-1" || fields["expected_hash"] != "abc123" {
		t.Errorf("form fields: %v", fields)
	}
	if !bytes.Equal(gotFile, envelope) {
		t.Errorf("envelope altered in transit: %v", gotFile)
	}
}

func TestSendChunkHashRejectedIsBadRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "hash mismatch", http.StatusBadRequest)
	}))
	defer srv.Close()

	err := NewPeerClient(newTestClient()).SendChunk(context.Background(), srv.URL, "u", "f_0", []byte("x"), "h")
	if !errors.Is(err, http_client.ErrBadRequest) {
		t.Errorf("got %v, want ErrBadRequest", err)
	}
}

func TestChunkEndpointValidation(t *testing.T) {
	bad := []string{"", "localhost:9000", "ftp://host", "http://", "://x", "file:///etc"}
	for _, peerURL := range bad {
		if _, err := chunkEndpoint(peerURL, "f_0", "u"); !errors.Is(err, http_client.ErrInvalidInput) {
			t.Errorf("accepted peer URL %q (err=%v)", peerURL, err)
		}
	}
	for _, seg := range []string{"", ".", ".."} {
		if _, err := chunkEndpoint("http://h:1", seg, "u"); !errors.Is(err, http_client.ErrInvalidInput) {
			t.Errorf("accepted segment %q", seg)
		}
	}
}

func TestChunkEndpointCannotChangeTarget(t *testing.T) {
	// Path in the peer URL is ignored; the chunk ID is escaped as ONE segment.
	got, err := chunkEndpoint("http://h:1/some/path?x=1", "a/../b?c=d", "o&x=1")
	if err != nil {
		t.Fatal(err)
	}
	want := "http://h:1/chunks/a%2F..%2Fb%3Fc=d?owner_user_id=o%26x%3D1"
	if got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}

func TestChunkRequestRejectsEmptyOwner(t *testing.T) {
	_, err := NewPeerClient(newTestClient()).FetchChunk(context.Background(), "http://h:1", "", model.ChunkID("f_0"))
	if !errors.Is(err, http_client.ErrInvalidInput) {
		t.Errorf("got %v", err)
	}
}

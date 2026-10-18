package server

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dfsha/internal/peer/hash"
	"dfsha/internal/peer/storage"
	"dfsha/internal/shared/model"
)

// Porque no tenemos implementado SuperPeerClient
type fakeSuperpeerClient struct{}

func (f *fakeSuperpeerClient) PutAck(chunkID model.ChunkID, peerID string) error {
	return nil
}

// Solo con que un struct defina las funciones requeridas por interface, la implementa

func newTestServer(t *testing.T) *Server {
	t.Helper() // Le indica a Go que la función actual es una función auxiliar de un test.

	store, err := storage.NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("storage.NewStore() error = %v", err)
	}

	return &Server{
		store:           store,
		superpeerClient: &fakeSuperpeerClient{},
		peerID:          "peer-test",
		maxChunkSizeMB:  64,
	}
}

// ------------------- POST receive ------------------------------
func TestReceiveChunk_InvalidHashReturnsBadRequest(t *testing.T) {
	s := newTestServer(t)

	var body bytes.Buffer                // contendrá body de petición simulada
	writer := multipart.NewWriter(&body) // Creamos un writer que construye contenido en formato multipart (porque es receive), y lo escribe en body
	// escritor que se utiliza en el test para construir la petición.

	if err := writer.WriteField("chunk_id", "file_0"); err != nil {
		t.Fatalf("WriteField chunk_id: %v", err)
	}
	if err := writer.WriteField("owner_user_id", "user1"); err != nil {
		t.Fatalf("WriteField owner_user_id: %v", err)
	}
	if err := writer.WriteField("expected_hash", "invalid-hash"); err != nil {
		t.Fatalf("WriteField expected_hash: %v", err)
	}

	part, err := writer.CreateFormFile("chunk", "chunk.bin") // Creamos nuevo campo multipart destinado al bin
	// part es un io.Writer que permite escribir bytes en ese "nuevo campo"
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write([]byte("payload")); err != nil { // con part, escribimos "payload" dentro de bin.
		t.Fatalf("Write chunk data: %v", err)
	}
	if err := writer.Close(); err != nil { // Para indicar que finalizamos modificación de multipart
		t.Fatalf("writer.Close: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/chunks/receive", &body) // construimos nuevo req post a ruta, con ese body
	req.Header.Set("Content-Type", writer.FormDataContentType())

	res := httptest.NewRecorder() // Crea un objeto que simula la respuesta HTTP y guarda lo que el handler escriba en ella
	s.receiveChunk(res, req)

	// Como handler escribe en req, verificamos
	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusBadRequest)
	}

	if !strings.Contains(res.Body.String(), "hash") && !strings.Contains(res.Body.String(), "corrupt") {
		t.Fatalf("body = %q, want hash/corrupt message", res.Body.String())
	}
}

func TestReceiveChunk_ValidHashStoresChunk(t *testing.T) {
	s := newTestServer(t)

	payload := []byte("real-chunk-bytes")
	expectedHash := hash.CalculateSHA256(payload)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("chunk_id", "file1_0"); err != nil {
		t.Fatalf("WriteField chunk_id: %v", err)
	}
	if err := writer.WriteField("owner_user_id", "user1"); err != nil {
		t.Fatalf("WriteField owner_user_id: %v", err)
	}
	if err := writer.WriteField("expected_hash", expectedHash); err != nil {
		t.Fatalf("WriteField expected_hash: %v", err)
	}
	part, err := writer.CreateFormFile("chunk", "chunk.bin")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write(payload); err != nil {
		t.Fatalf("Write chunk data: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/chunks/receive", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	res := httptest.NewRecorder()

	s.receiveChunk(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), "Chunk received") {
		t.Fatalf("body = %q, want success message", res.Body.String())
	}

	// verificamos si sí se guardó
	stored, err := s.store.Load("user1", model.NewChunkID("file1", 0))
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	// Verificamos que si se haya guardado el contenido mandado
	if !bytes.Equal(stored, payload) {
		t.Fatalf("stored chunk = %q, want %q", stored, payload)
	}
}

// ------------------- GET chunk ------------------------------
func TestGetChunk_ExistingChunkReturnsBytes(t *testing.T) {
	s := newTestServer(t)

	chunkID := model.NewChunkID("file1", 0)
	payload := []byte("chunk-data")
	// Creamos archivo para que devuelva OK
	if err := s.store.Save("user1", chunkID, payload); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/chunks/file1_0?owner_user_id=user1", nil)
	req.SetPathValue("chunk_id", string(chunkID))
	res := httptest.NewRecorder()

	s.getChunk(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if got := res.Body.Bytes(); !bytes.Equal(got, payload) { // Verificamos que contenido en respuesta a GET, sea mismo que el que tiene el archivo "local" (o sea, que si se haya escrito bien)
		t.Fatalf("body = %q, want %q", got, payload)
	}
	if got := res.Header().Get("Content-Type"); got != "application/octet-stream" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/octet-stream")
	}
}

func TestGetChunk_NotFoundReturns404(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/chunks/file1_0?owner_user_id=user1", nil)
	req.SetPathValue("chunk_id", "file1_0")
	res := httptest.NewRecorder()

	s.getChunk(res, req)

	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusNotFound)
	}
	if !strings.Contains(res.Body.String(), "not found") {
		t.Fatalf("body = %q, want not found message", res.Body.String())
	}
}

func TestDeleteChunk_IsIdempotentAndReturns200(t *testing.T) {
	s := newTestServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/chunks/file1_0?owner_user_id=user1", nil)
	req.SetPathValue("chunk_id", "file1_0")
	res := httptest.NewRecorder()

	s.deleteChunk(res, req) // Chunk no existe. No deberia haber problema

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}
	if !strings.Contains(res.Body.String(), "Chunk deleted") {
		t.Fatalf("body = %q, want delete confirmation", res.Body.String())
	}
}

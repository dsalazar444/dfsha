// Package files  cliente HTTP que usa el superpeer para hablar con los peers

package files

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// ChunkDeleter es "algo que sabe pedirle a un peer que borre un chunk"
// Es una interfaz para que Service no dependa de HTTP: en producción se usa
// HTTPChunkDeleter, y en las pruebas se usa uno falso que solo anota las llamadas
type ChunkDeleter interface {
	DeleteChunk(peerURL, chunkID, ownerUserID string) error
}

// HTTPChunkDeleter es el ChunkDeleter real: le habla a los peers por HTTP
type HTTPChunkDeleter struct {
	client *http.Client
}

// NewHTTPChunkDeleter crea el cliente con un timeout corto (5 s),
// para que un peer caído no deje la petición colgada mucho tiempo
func NewHTTPChunkDeleter() *HTTPChunkDeleter {
	return &HTTPChunkDeleter{client: &http.Client{Timeout: 5 * time.Second}}
}

// DeleteChunk llama a DELETE {peerURL}/chunks/{chunkID}?owner_user_id={ownerUserID}
// El peer necesita el dueño porque guarda los chunks en disco como dueño/archivo/índice
func (d *HTTPChunkDeleter) DeleteChunk(peerURL, chunkID, ownerUserID string) error {
	// PathEscape y QueryEscape evitan que caracteres raros rompan la URL
	target := fmt.Sprintf("%s/chunks/%s?owner_user_id=%s", peerURL, url.PathEscape(chunkID), url.QueryEscape(ownerUserID))
	req, err := http.NewRequest(http.MethodDelete, target, nil)
	if err != nil {
		return err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err // el peer no respondió (caído, timeout, etc.)
	}
	defer resp.Body.Close()

	// El peer responde 200 también si ya no tenía el chunk (borrado idempotente)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("peer answered %d", resp.StatusCode)
	}
	return nil
}

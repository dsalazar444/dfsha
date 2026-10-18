// Package model define los tipos de datos que viajan en las peticiones
// y respuestas HTTP entre el cliente, el peer y el superpeer, y y las
// funciones puras mínimas para construirlos/interpretarlos correctamente. Vive en
// shared porque tanto el código del peer como el del superpeer necesitan
// coincidir exactamente en estos tipos para que el JSON se (de)serialice
// igual en ambos lados.
package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ChunkID identifica un chunk de forma única en todo el sistema.
// Es un string compuesto: "{fileID}_{indice}", por ejemplo "abc123_0".
// Se modela como un tipo propio (en vez de usar string a secas) para que
// el compilador distinga "esto es un ChunkID" de "esto es un string
// cualquiera" en las firmas de función — evita pasar, por ejemplo, un
// fileID donde se esperaba un chunkID.
type ChunkID string

// NewChunkID arma un ChunkID a partir de un fileID y la posición del
// chunk dentro del archivo. Se usa al generar los chunks (en put/init
// del lado superpeer, y al partir el archivo del lado peer).
// "thesis", 0 -> thesis_0
func NewChunkID(fileID string, index int) ChunkID {
	return ChunkID(fmt.Sprintf("%s_%d", fileID, index))
}

// FileID extrae el identificador de archivo desde un ChunkID ya armado.
// Se necesita, por ejemplo, para construir la ruta de almacenamiento
// local (userID/fileID/chunk_n), o para que el superpeer sepa a qué
// archivo pertenece un chunk_id recibido en put/ack.
func (c ChunkID) FileID() (string, error) { // getFileID
	parts := strings.SplitN(string(c), "_", 2)

	if len(parts) != 2 {
		return "", fmt.Errorf("invalid ChunkID: %q", c)
	}

	return parts[0], nil
}

// Index extrae la posición del chunk dentro del archivo. Se necesita
// para reconstruir el archivo en el orden correcto al hacer get, o
// para que el superpeer devuelva los chunks ordenados.
func (c ChunkID) Index() (int, error) { // getIndex
	parts := strings.SplitN(string(c), "_", 2)

	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid ChunkID: %q", c)
	}

	i, err := strconv.Atoi(parts[1])
	if err != nil || i < 0 {
		return 0, fmt.Errorf("invalid chunk index in %q: %w", c, err)
	}

	return i, nil
}

// ChunkAssignment es la INSTRUCCIÓN que da el superpeer en put/init:
// "este chunk debe enviarse a estos peers". En el momento en que el
// cliente recibe esto, los peers listados AÚN NO TIENEN el chunk —
// es una asignación a futuro, no una confirmación.
type ChunkAssignment struct {
	ChunkID ChunkID `json:"chunk_id"`
	// PeerURLs son los destinos elegidos por el superpeer según el
	// factor de replicación (ej. 2 URLs si la replicación es 2).
	PeerURLs []string `json:"destination_peers"`
}

// Request para POST files/put/init del SP
type PutInitRequest struct {
	FileID       string   `json:"file_id"`
	LogicalPath  string   `json:"logical_path"`
	ChunkCount   int      `json:"chunk_count"`
	HashPerChunk []string `json:"hash_per_chunk"`
}

// Es la respuesta completa de POST /files/put/init del SP. Contiene los chunkAssig. de un file
type PutInitResponse struct {
	// FileID es generado por Peer, pero es retornado en response si es váldio, si no, retorna ""
	FileID     string            `json:"file_id"`
	Assignment []ChunkAssignment `json:"assignment"`
}

// ChunkLocation es el HECHO CONFIRMADO que devuelve el superpeer en
// /get: "este chunk YA ESTÁ guardado en estos peers" (solo peers que
// mandaron su put/ack para ese chunk aparecen aquí). A diferencia de
// ChunkAssignment, esto no es una instrucción, es el estado actual.
type ChunkLocation struct {
	ChunkID      ChunkID  `json:"chunk_id"`
	PeerURLs     []string `json:"peers"`
	ExpectedHash string   `json:"hash"` // hash declarado por el dueño en put/init; fuente confiable, no viene del peer que sirve el chunk
}

// Es la respuesta completa de GET /files/get/{ruta}. Contiene todas las ubicaciones de todos
// los chunks de un FileID
type GetFileResponse struct {
	FileID string          `json:"file_id"`
	Chunks []ChunkLocation `json:"chunks"`
}

// es lo que el peer manda periódicamente a POST /peers/heartbeat para reportar
// que sigue vivo y en qué estado.
type HeartbeatPayload struct {
	PeerID string `json:"peer_id"`
	// FreeSpace en bytes; el superpeer lo usa para decidir a qué peer
	// asignar nuevos chunks (prioriza el que tenga más espacio libre).
	FreeSpace int64 `json:"available_space"`
	// Chunks: todos los ChunkID que este peer tiene almacenados
	// actualmente. Le sirve al superpeer para detectar inconsistencias
	// (ej. un chunk que el superpeer cree perdido pero el peer sí tiene).
	Chunks []ChunkID `json:"chunks"`
}

type HeartbeatResponse struct {
	Tasks []ReplicationTask `json:"tasks"`
}

// ReplicationAction identifies what the peer must do for a task.
type ReplicationAction string

const (
	ReplicationActionGet    ReplicationAction = "GET"
	ReplicationActionDelete ReplicationAction = "DELETE"
)

type ReplicationTask struct {
	Action       ReplicationAction `json:"action"`        // Indicates if it is GET or DELETE
	ChunkID      ChunkID           `json:"chunk_id"`      // chunkID a solicitar
	OwnerUserID  string            `json:"owner_user_id"` // owner para armar path de chunk
	SourceURLs   []string          `json:"source_urls"`   // urls de peers a pedir chunk
	ExpectedHash string            `json:"expected_hash"` // hash confiable de chunk
}

// Es la respuesta de POST /files/lock/{ruta} de SP
type LockResponse struct {
	LockID string `json:"lock_id"`
	// ExpiresAt es el TTL del lock: si el cliente no hace unlock antes
	// de esta hora, el superpeer lo libera automáticamente (evita locks
	// huérfanos si el cliente se cae a mitad de una edición).
	ExpiresAt time.Time `json:"expires_at"`
}

// Es el request de POST /files/unlock/{ruta}.
type UnlockRequest struct {
	LockID string `json:"lock_id"`
}

// Es el request para el POST /auth/login.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse es la respuesta de POST /auth/login.
type LoginResponse struct {
	Token  string `json:"session_token"`
	UserID string `json:"user_id"`
	Salt   string `json:"salt"` // salt de Argon2id, para derivar la clave maestra localmente
}

// --- Tipos que faltaban: payloads JSON entre el peer y el superpeer ---

// Es el request de POST /peers/register.
type RegisterPeerRequest struct {
	Address string `json:"address"`
	Port    string `json:"port"`
}

// Es la respuesta de POST /peers/register.
type RegisterPeerResponse struct {
	PeerID string `json:"peer_id"`
}

// Es el request de POST /files/put/ack: quién confirma (PeerID) qué chunk
// ya guardó (ChunkID).
type PutAckRequest struct {
	ChunkID ChunkID `json:"chunk_id"`
	PeerID  string  `json:"peer_id"`
}

// Es el request de POST /fs/mkdir y de DELETE /fs/rmdir.
type PathRequest struct {
	Path string `json:"path"`
}

// DirEntry es una entrada de un listado de directorio. Type distingue
// archivo de directorio con los valores que defina el contrato del SP.
type DirEntry struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Es la respuesta de GET /fs/ls.
type ListDirResponse struct {
	Entries []DirEntry `json:"entries"`
}

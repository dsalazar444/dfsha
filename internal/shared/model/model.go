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
	ChunkID ChunkID
	// PeerURLs son los destinos elegidos por el superpeer según el
	// factor de replicación (ej. 2 URLs si la replicación es 2).
	PeerURLs []string
}

// Request para POST files/put/init del SP
type PutInitRequest struct {
	LogicalPath  string
	ChunkCount   int
	HashPerChunk []string
}

// Es la respuesta completa de POST /files/put/init del SP. Contiene los chunkAssig. de un file
type PutInitResponse struct {
	// FileID es generado por el superpeer; el peer lo necesita para
	// construir cada ChunkID al subir y para el ack posterior.
	FileID     string
	Assignment []ChunkAssignment
}

// ChunkLocation es el HECHO CONFIRMADO que devuelve el superpeer en
// /get: "este chunk YA ESTÁ guardado en estos peers" (solo peers que
// mandaron su put/ack para ese chunk aparecen aquí). A diferencia de
// ChunkAssignment, esto no es una instrucción, es el estado actual.
type ChunkLocation struct {
	ChunkID      ChunkID
	PeerURLs     []string
	ExpectedHash string // hash declarado por el dueño en put/init; fuente confiable, no viene del peer que sirve el chunk
}

// Es la respuesta completa de GET /files/get/{ruta}. Contiene todas las ubicaciones de todos
// los chunks de un FileID
type GetFileResponse struct {
	FileID string
	Chunks []ChunkLocation
}

// es lo que el peer manda periódicamente a POST /peers/heartbeat para reportar
// que sigue vivo y en qué estado.
type HeartbeatPayload struct {
	PeerID string
	// FreeSpace en bytes; el superpeer lo usa para decidir a qué peer
	// asignar nuevos chunks (prioriza el que tenga más espacio libre).
	FreeSpace int64
	// Chunks: todos los ChunkID que este peer tiene almacenados
	// actualmente. Le sirve al superpeer para detectar inconsistencias
	// (ej. un chunk que el superpeer cree perdido pero el peer sí tiene).
	Chunks []ChunkID
}

// Es la respuesta de POST /files/lock/{ruta} de SP
type LockResponse struct {
	LockID string
	// ExpiresAt es el TTL del lock: si el cliente no hace unlock antes
	// de esta hora, el superpeer lo libera automáticamente (evita locks
	// huérfanos si el cliente se cae a mitad de una edición).
	ExpiresAt time.Time
}

// Es el request para el POST /auth/login.
type LoginRequest struct {
	Username string
	Password string
}

// LoginResponse es la respuesta de POST /auth/login.
type LoginResponse struct {
	Token  string
	UserID string
	Salt   string // salt de Argon2id, para derivar la clave maestra localmente
}

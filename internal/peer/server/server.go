// Package server implementa el rol SERVIDOR de un peer: los handlers
// HTTP que responden cuando otro peer o cliente llama a este peer. No
// decide a quién mandar nada ni inicia llamadas por su cuenta — eso
// es trabajo de internal/peer/client. Aquí solo se valida lo que
// llega, se delega a storage, y se responde.
package server

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"dfsha/internal/peer/hash"
	"dfsha/internal/peer/storage"
	"dfsha/internal/shared/model"
)

const defaultMaxChunkSizeMB = 64

// TODO: Si interface crece, sacarla a archivo aparte
// superpeerClient define la mínima operación que el peer necesita del
// superpeer para confirmar que aceptó el chunk.
type superpeerClient interface {
	PutAck(chunkID model.ChunkID, peerID string) error
}

// Server agrupa las dependencias que los handlers necesitan.
type Server struct {
	store           *storage.Store // Objeto de pckg storage que permite usar sus funciones
	superpeerClient superpeerClient
	// peerID identifica a ESTE peer ante el superpeer (el que se
	// obtuvo en /peers/register). Se necesita para el put/ack: el
	// superpeer debe saber QUIÉN confirma haber recibido el chunk.
	peerID string

	maxChunkSizeMB int
}

// NewServer crea un Server con sus dependencias ya resueltas.
func NewServer(store *storage.Store, superpeerClient superpeerClient, peerID string, maxChunkSizeMB int) *Server {
	if maxChunkSizeMB <= 0 {
		maxChunkSizeMB = defaultMaxChunkSizeMB
	}
	return &Server{store: store, superpeerClient: superpeerClient, peerID: peerID, maxChunkSizeMB: maxChunkSizeMB}
}

// Routes registra los handlers de este paquete en un mux. Se separa
// de NewServer para que main.go decida cuándo y con qué envolver las
// rutas (ej. agregar logging o recovery middleware más adelante).
//
// Aclaraciones:
// Un mux (multiplexer) es un componente que distribuye las peticiones HTTP al
// handler correspondiente según la ruta y el método HTTP.
func (s *Server) Routes() http.Handler { // Ese tipo de retorno porque es interfaz implementada por mux
	mux := http.NewServeMux()                              // crea enrutador vacio (mux)
	mux.HandleFunc("POST /chunks/receive", s.receiveChunk) // le agregamos las rutas
	mux.HandleFunc("GET /chunks/{chunk_id}", s.getChunk)
	mux.HandleFunc("DELETE /chunks/{chunk_id}", s.deleteChunk)

	return mux
}

// receiveChunk implementa POST /chunks/receive.
//
// Flujo:
//  1. Parsear el multipart/form-data con los campos acordados en el
//     OpenAPI: chunk_id, owner_user_id, chunk (binario = envelope
//     nonce+ciphertext+tag ya armado por crypto.Seal del lado del
//     peer dueño), hash_esperado.
//  2. Verificar integridad: recalcular SHA-256 sobre el envelope
//     recibido y compararlo contra hash_esperado. Esto detecta
//     corrupción de transporte SIN necesitar descifrar nada — este
//     peer nunca tiene ni necesita la clave de cifrado.
//  3. Si el hash no coincide, responder 400 y NO guardar nada — un
//     chunk corrupto no debe quedar en disco disfrazado de válido.
//  4. Si coincide, guardar con storage.Save (que ya maneja la
//     escritura atómica vía archivo temporal + rename).
//  5. Confirmar al superpeer con PutAck — ESTE peer es quien avisa
//     que ya tiene el chunk, no el que lo subió (el ack lo manda
//     quien recibe, no quien envía).
func (s *Server) receiveChunk(w http.ResponseWriter, r *http.Request) {
	// Con esto limpiamos archivos temporales que pueden haber quedado por multipartform.
	defer func() {
		if r.MultipartForm != nil {
			r.MultipartForm.RemoveAll()
		}
	}()

	maxChunkSize := int64(s.maxChunkSizeMB) << 20
	// ponemos maximo recibido para proteger contra ataques DoS
	r.Body = http.MaxBytesReader(w, r.Body, maxChunkSize+(1<<20)) // ponemos 1 MiB de más para el tag y el nonce

	// 32 MB de límite para las partes que van en memoria RAM (campos de
	// texto); el chunk binario puede ser más grande, ParseMultipartForm
	// igual lo acepta, solo que lo que exceda este límite se vuelca a
	// disco como archivo temporal por la propia librería estándar. Estos
	// temps se limpian con el defer.
	if err := r.ParseMultipartForm(32 << 20); err != nil { // parsemultipartForm procesa los datos del cuerpo HTTP y permite mantener en memoria hasta un umbral de aproximadamente 32 MiB
		writeError(w, fmt.Errorf("%w: invalid multipart: %v", storage.ErrBadRequest, err))
		return
	}
	// Obtenemos valores y veriicamos
	chunkIDRaw := r.FormValue("chunk_id")
	ownerUserID := r.FormValue("owner_user_id")
	expectedHash := r.FormValue("expected_hash")

	if chunkIDRaw == "" || ownerUserID == "" || expectedHash == "" {
		writeError(w, fmt.Errorf("%w: required fields are missing: chunk_id, owner_user_id, expected_hash", storage.ErrBadRequest))
		return
	}
	chunkID := model.ChunkID(chunkIDRaw)

	// Obtenemos "File" chunk de multipart. Devuelve un objeto que implementa io.Reader (no un []byte),
	// por lo que podmeos leer sus bytes progresivamente, con readAll
	file, _, err := r.FormFile("chunk")
	if err != nil {
		writeError(w, fmt.Errorf("%w: 'chunk' field missing or invalid: %v", storage.ErrBadRequest, err))
		return
	}
	defer file.Close()

	// Leemos chunk (envelope), obteniendo un []byte
	envelope, err := io.ReadAll(file)
	if err != nil {
		writeError(w, fmt.Errorf("%w: reading chunk: %v", storage.ErrServer, err))
		return
	}

	// Verificación de integridad ANTES de tocar disco. El hash es
	// sobre el envelope completo (nonce + ciphertext+tag), nunca
	// sobre contenido descifrado — este peer no tiene la clave.
	if !hash.VerifySHA256(envelope, expectedHash) {
		// 400: el chunk llegó corrupto o alterado. El cliente que
		// subió debe reintentar con OTRO peer de la lista de
		// réplicas, no con este mismo (ya quedó documentado en el
		// OpenAPI).
		writeError(w, fmt.Errorf("%w: the hash does not match: corrupt chunk", storage.ErrBadRequest))
		return
	}

	if err := s.store.Save(ownerUserID, chunkID, envelope); err != nil {
		writeError(w, err) // en Save se manejan errores y respuestas a path transversal.
		return
	}

	// El chunk ya quedó guardado en disco en este punto. Si el ack
	// al superpeer falla, no revertimos el guardado — es un detalle
	// de sincronización de metadata, no invalida que este peer sí
	// tenga el chunk. Se loggea para investigar después, pero no
	// bloquea la respuesta 200 que ya le corresponde a quien subió.
	if err := s.superpeerClient.PutAck(chunkID, s.peerID); err != nil {
		log.Printf("WARNING: put/ack failed for chunk %s: %v", chunkID, err)
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Chunk received and verified successfully"))
}

// getChunk implements GET /chunks/{chunk_id}?owner_user_id=...
//
// A GET has no body, so the request carries only identifiers: chunk_id
// in the path and owner_user_id as a query param. The response body is
// the raw envelope (nonce + ciphertext+tag) exactly as it was stored.
// This peer never decrypts it: the client verifies its integrity against
// the hash the superpeer gave it in /files/get, not against anything
// this peer says.
//
// Flujo:
// 1. Obtenemos datos en path y query
// 2. Cargamos, con storage, el chunk con ese userID y chunkID
// 3. Escribimos headers en el HTTP response
// 4. Escribimos chunk en HTTP response
// NOTA: El peer que recibe la respuesta de GET es el encargado de enviar ACK
func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) {
	// Get urls params
	ownerUserID, chunkID, err := chunkParams(r)

	// Explicit check only to give a clear message for a missing param.
	// The format of both identifiers is validated inside Load.
	if err != nil {
		writeError(w, err)
		return
	}

	// Load validates both identifiers (path traversal included) before
	// touching the disk, so there is no extra validation here.
	envelope, err := s.store.Load(ownerUserID, chunkID)
	if err != nil {
		writeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(envelope)))

	if _, err := w.Write(envelope); err != nil {
		// The status line (e.g. 200 OK) is already sent; all that is left is to log.
		log.Printf("writing chunk %s: %v", chunkID, err)
	}
}

func (s *Server) deleteChunk(w http.ResponseWriter, r *http.Request) {
	// Get urls params
	ownerUserID, chunkID, err := chunkParams(r)

	// err for missing params
	if err != nil {
		writeError(w, err)
		return
	}

	if err := s.store.Delete(ownerUserID, chunkID); err != nil {
		writeError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK) // No es necesario, proque si añadimos body,
	// sin haberlo setteado, se asigna automaticamente 200, pero por explicidad.
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("Chunk deleted. Idempotent: also 200 if this peer did not have it."))
}

// chunkParams reads the identifiers shared by GET and DELETE on
// /chunks/{chunk_id}. The format of both is validated later, inside
// the storage layer; here we only guarantee the query param is there
// so the caller gets a clear message.
func chunkParams(r *http.Request) (ownerUserID string, chunkID model.ChunkID, err error) {
	ownerUserID = r.URL.Query().Get("owner_user_id") // No se verifica que se haya pasado porque en ruta de handler especificamos chunks/{chunk_id}, entonces si no coindiden con esa estructura, no llegan hasta acá

	// Explicit check only to give a clear message for a missing param.
	// The format of both identifiers is validated inside Load.
	if ownerUserID == "" {
		return "", "", fmt.Errorf("%w: missing owner_user_id", storage.ErrBadRequest)
	}
	return ownerUserID, model.ChunkID(r.PathValue("chunk_id")), nil
}

// writeError maps the error category to an HTTP status. The category
// (ErrBadRequest, ErrNotFound, ErrServer) decides the code; the wrapped
// message carries the context.
func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrBadRequest):
		http.Error(w, err.Error(), http.StatusBadRequest) // escribe err.Error en body de respuesta
	case errors.Is(err, storage.ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	default:
		// ErrServer, or any error with no category. The detail is only
		// logged, never sent to the caller: OS errors usually include
		// absolute local paths, which should not leave this peer.
		log.Printf("internal error: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

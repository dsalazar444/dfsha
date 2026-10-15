// Package server implementa el rol SERVIDOR de un peer: los handlers
// HTTP que responden cuando otro peer o cliente llama a este peer. No
// decide a quién mandar nada ni inicia llamadas por su cuenta — eso
// es trabajo de internal/peer/client. Aquí solo se valida lo que
// llega, se delega a storage, y se responde.
package server

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"dfsha/internal/peer/client"
	"dfsha/internal/peer/hash"
	"dfsha/internal/peer/storage"
	"dfsha/internal/shared/model"
)

// Server agrupa las dependencias que los handlers necesitan.
type Server struct {
	store           *storage.Store // Objeto de pckg storage que permite usar sus funciones
	superpeerClient *client.SuperpeerClient
	// peerID identifica a ESTE peer ante el superpeer (el que se
	// obtuvo en /peers/register). Se necesita para el put/ack: el
	// superpeer debe saber QUIÉN confirma haber recibido el chunk.
	peerID string
}

// NewServer crea un Server con sus dependencias ya resueltas.
func NewServer(store *storage.Store, superpeerClient *client.SuperpeerClient, peerID string) *Server {
	return &Server{store: store, superpeerClient: superpeerClient, peerID: peerID}
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

	maxChunkSizeMB := int64(64)

	// Es parametrizable en .env
	if v := os.Getenv("MAX_CHUNK_SIZE_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxChunkSizeMB = n
		}
	}

	maxChunkSize := maxChunkSizeMB << 20
	// ponemos maximo recibido para proteger contra ataques DoS
	r.Body = http.MaxBytesReader(w, r.Body, maxChunkSize+(1<<20)) // ponemos 1 MiB de más para el tag y el nonce

	// 32 MB de límite para las partes que van en memoria RAM (campos de
	// texto); el chunk binario puede ser más grande, ParseMultipartForm
	// igual lo acepta, solo que lo que exceda este límite se vuelca a
	// disco como archivo temporal por la propia librería estándar. Estos
	// temps se limpian con el defer.
	if err := r.ParseMultipartForm(32 << 20); err != nil { // parsemultipartForm procesa los datos del cuerpo HTTP y permite mantener en memoria hasta un umbral de aproximadamente 32 MiB
		http.Error(w, fmt.Sprintf("invalid multipart: %v", err), http.StatusBadRequest)
		return
	}
	// Obtenemos valores y veriicamos
	chunkIDRaw := r.FormValue("chunk_id")
	ownerUserID := r.FormValue("owner_user_id")
	hashEsperado := r.FormValue("expected_hash")

	if chunkIDRaw == "" || ownerUserID == "" || hashEsperado == "" {
		http.Error(w, "required fields are missing: chunk_id, owner_user_id, expected_hash", http.StatusBadRequest)
		return
	}
	chunkID := model.ChunkID(chunkIDRaw)

	// Obtenemos "File" chunk de multipart. Devuelve un objeto que implementa io.Reader (no un []byte),
	// por lo que podmeos leer sus bytes progresivamente, con readAll
	file, _, err := r.FormFile("chunk")
	if err != nil {
		http.Error(w, fmt.Sprintf("'chunk' field missing or invalid: %v", err), http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Leemos chunk (envelope), obteniendo un []byte
	envelope, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, fmt.Sprintf("reading chunk: %v", err), http.StatusInternalServerError)
		return
	}

	// Verificación de integridad ANTES de tocar disco. El hash es
	// sobre el envelope completo (nonce + ciphertext+tag), nunca
	// sobre contenido descifrado — este peer no tiene la clave.
	if !hash.VerifySHA256(envelope, hashEsperado) {
		// 400: el chunk llegó corrupto o alterado. El cliente que
		// subió debe reintentar con OTRO peer de la lista de
		// réplicas, no con este mismo (ya quedó documentado en el
		// OpenAPI).
		http.Error(w, "the hash does not match: corrupt chunk", http.StatusBadRequest)
		return
	}

	if err := s.store.Save(ownerUserID, chunkID, envelope); err != nil {
		http.Error(w, fmt.Sprintf("saving chunk: %v", err), http.StatusInternalServerError)
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
}

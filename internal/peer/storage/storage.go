// Package storage implementa el almacenamiento local plano de un peer:
// escribe/lee/borra chunks bajo un directorio base, con la ruta
// basePath/ownerUserID/fileID/index. Protege contra path traversal
// porque ownerUserID y ChunkID llegan desde requests HTTP (de otros
// peers o del cliente) y nunca deben usarse para construir rutas de
// filesystem sin validar primero.
package storage

import (
	"dfsha/internal/shared/model"

	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// struct para funcion ListChunkIDs(). Usado para representar un chunk almacenado.
type StoredChunk struct {
	UserID  string
	ChunkID model.ChunkID
}

// safeComponent es la whitelist de caracteres permitidos en un solo
// segmento de ruta (ownerUserID, o el fileID extraído de un ChunkID).
// Es la defensa real contra path traversal: con este patrón no hay
// forma de codificar "..", "/", ni un byte nulo, así que no hace
// falta "limpiar" el input después — lo que no cumple se rechaza
// directo, en vez de intentar sanearlo.
var safeComponent = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Errores centinela: permiten a quien llama (los handlers) distinguir
// el tipo de fallo con errors.Is y responder el status HTTP correcto,
// sin comparar textos de error.
var (
	ErrBadRequest = errors.New("bad request")
	ErrNotFound   = errors.New("not found")
	ErrServer     = errors.New("internal server error")
)

// Store es el almacenamiento local de un peer.
type Store struct {
	basePath string
	mu       sync.RWMutex
}

// NewStore crea un Store sobre basePath. Crea el directorio base aquí
// mismo si no existe; cada operación crea las carpetas que necesita bajo demanda.
func NewStore(basePath string) (*Store, error) {
	if err := os.MkdirAll(basePath, 0o700); err != nil {
		return nil, fmt.Errorf("%w: creating basePath %q: %w", ErrServer, basePath, err)
	}
	return &Store{basePath: basePath}, nil
}

// chunkPath calcula la ruta final de un chunk en disco, validando
// cada componente ANTES de construir cualquier ruta. Es el único
// punto del paquete que sabe cómo se arma la ruta — todas las demás
// funciones pasan por aquí, para no duplicar la validación en cada
// una (y correr el riesgo de que a alguna se le olvide).
// string -> path, error -> mensaje de error, si lo hay, path es vacio.
func (s *Store) chunkPath(ownerUserID string, chunkID model.ChunkID) (string, error) {
	// Validate chunkID
	fileID, err := chunkID.FileID()
	if err != nil {
		return "", fmt.Errorf("%w: invalid chunk_id %q: %v", ErrBadRequest, chunkID, err)
	}
	index, err := chunkID.Index()
	if err != nil {
		return "", fmt.Errorf("%w: invalid chunk index in %q: %v", ErrBadRequest, chunkID, err)
	}

	// Validate each part regex
	if !safeComponent.MatchString(ownerUserID) {
		return "", fmt.Errorf("%w: ownerUserID does not match required structure: %q", ErrBadRequest, ownerUserID)
	}
	if !safeComponent.MatchString(fileID) {
		return "", fmt.Errorf("%w: fileID does not match required structure: %q", ErrBadRequest, fileID)
	}

	path := filepath.Join(s.basePath, ownerUserID, fileID, strconv.Itoa(index))

	// Defensa en profundidad: aunque la whitelist de arriba ya
	// debería hacer esto imposible, verificamos que la ruta final
	// siga dentro de basePath después de Join+Clean. Si el día de
	// mañana alguien relaja safeComponent sin pensar en esto, esta
	// comprobación sigue frenando un path traversal.
	absBase, err := filepath.Abs(s.basePath) // obtenemos ruta absoluta base (por ejm: user/dfsha/storage/)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrServer, err)
	}
	absPath, err := filepath.Abs(path) // Obtenemos ruta absoluta del path construido acá
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrServer, err)
	}

	// Obtenemos ruta relativa absBase para generar un path relativo, y visualizar de forma clara
	// si quieren salir de directorio.
	relPath, err := filepath.Rel(absBase, absPath)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrServer, err)
	}
	// Arriba sí 500 porque son errores al construir o pedir rutas

	if relPath == ".." || strings.HasPrefix(relPath, ".."+string(os.PathSeparator)) {
		return "", fmt.Errorf("%w: resulted path outside of basePath: %q", ErrBadRequest, absPath)
	}

	return absPath, nil
}

// Save guarda el envelope de un chunk (nonce + ciphertext+tag, ya
// armado por crypto.Seal) en disco.
//
// Escribe primero a un archivo temporal y hace os.Rename al final,
// en vez de escribir directo al destino, por dos razones:
//  1. Atomicidad: si el proceso se cae a mitad de la escritura, el
//     archivo final nunca queda truncado — o está completo, o no
//     existe. Un Load concurrente nunca ve un estado a medias.
//  2. os.Rename solo es atómico cuando origen y destino están en el
//     MISMO filesystem — por eso el temporal se crea en el mismo
//     directorio del destino final, no en /tmp del sistema (que
//     puede ser un filesystem distinto, ej. tmpfs vs disco).
func (s *Store) Save(ownerUserID string, chunkID model.ChunkID, envelope []byte) error {
	finalPath, err := s.chunkPath(ownerUserID, chunkID)
	if err != nil {
		return err // ya viene formateado de chunkPath
	}

	dir := filepath.Dir(finalPath) // obtiene directorio padre de final path

	s.mu.Lock()
	defer s.mu.Unlock()

	// creamos dir con permisos, inicializando err, y luego miramos si no es vacio.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("%w: creating directory %q: %w", ErrServer, dir, err)
	}

	tmp, err := os.CreateTemp(dir, "chunk-*.tmp") // Os reemplaza * por numero aleatorio
	if err != nil {
		return fmt.Errorf("%w: creating temporal file: %w", ErrServer, err)
	}
	tmpPath := tmp.Name()
	// Si algo falla antes del rename exitoso, se borra el temporal
	// para no dejar basura acumulándose en disco. Si el rename sí
	// tuvo éxito, tmpPath ya no existe y este Remove no hace nada.
	defer os.Remove(tmpPath)

	if _, err := tmp.Write(envelope); err != nil {
		tmp.Close()
		return fmt.Errorf("%w: writing temporal chunk: %w", ErrServer, err)
	}
	// Sync fuerza el flush a disco antes del rename. Sin esto, el
	// rename podría completarse mientras los datos siguen en el
	// buffer del SO, y una caída justo ahí dejaría el archivo final
	// "renombrado" pero vacío o incompleto en algunos filesystems.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("%w: synchronizing temporal chunk: %w", ErrServer, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("%w: closing temporal chunk: %w", ErrServer, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("%w: moving chunk to final destination: %w", ErrServer, err)
	}
	return nil
}

// Load lee el envelope completo de un chunk ya guardado.
func (s *Store) Load(ownerUserID string, chunkID model.ChunkID) ([]byte, error) {
	path, err := s.chunkPath(ownerUserID, chunkID)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: chunk %s not found: %w", ErrNotFound, chunkID, err)
	}

	return data, err
}

// Delete elimina el chunk del disco. No es error si ya no existía —
// coherente con el borrado "relajado" que se define en la API: el
// superpeer puede reintentar un delete sin que falle por no-encontrado.
func (s *Store) Delete(ownerUserID string, chunkID model.ChunkID) error {
	path, err := s.chunkPath(ownerUserID, chunkID)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	err = os.Remove(path)
	if err == nil || errors.Is(err, fs.ErrNotExist) {
		return nil
	}

	return fmt.Errorf("%w: removing chunk %q: %v", ErrServer, path, err)
}

// Exists indica si el chunk está almacenado localmente.
func (s *Store) Exists(ownerUserID string, chunkID model.ChunkID) bool {
	path, err := s.chunkPath(ownerUserID, chunkID)
	if err != nil {
		return false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, err = os.Stat(path)
	return err == nil
}

// FreeSpace devuelve el espacio libre del filesystem donde vive
// basePath, en bytes — lo usa heartbeat para reportar al superpeer.
//
// Nota de portabilidad: syscall.Statfs es específico de Linux (y la
// mayoría de Unix).
func (s *Store) FreeSpace() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var stat syscall.Statfs_t
	// consultamos info de basePath y lo guardamos en stat
	if err := syscall.Statfs(s.basePath, &stat); err != nil {
		return 0, fmt.Errorf("%w: consulting free space of %q: %w", ErrServer, s.basePath, err)
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// ListChunkIDs recorre basePath y reconstruye el ChunkID de cada
// chunk almacenado, a partir de la estructura de carpetas
// (ownerUserID/fileID/index). Se usa para armar el payload del
// heartbeat periódico.
func (s *Store) ListChunkIDs() ([]StoredChunk, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var chunks []StoredChunk

	err := filepath.WalkDir(s.basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		// La ruta esperada es basePath/ownerUserID/fileID/index —
		// cualquier archivo que no respete esa profundidad se ignora
		// en vez de asumir que es un chunk válido.
		rel, err := filepath.Rel(s.basePath, path)
		if err != nil {
			return err
		}

		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 3 {
			return nil
		}

		userID := parts[0]
		fileID := parts[1]

		index, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil // nombre de archivo que no es un índice numérico, se ignora
		}

		chunks = append(chunks, StoredChunk{
			UserID:  userID,
			ChunkID: model.NewChunkID(fileID, index),
		})
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("%w: listing chunks in %q: %v", ErrServer, s.basePath, err)
	}
	return chunks, nil
}

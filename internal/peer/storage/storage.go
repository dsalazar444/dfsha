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
	"syscall"
)

// safeComponent es la whitelist de caracteres permitidos en un solo
// segmento de ruta (ownerUserID, o el fileID extraído de un ChunkID).
// Es la defensa real contra path traversal: con este patrón no hay
// forma de codificar "..", "/", ni un byte nulo, así que no hace
// falta "limpiar" el input después — lo que no cumple se rechaza
// directo, en vez de intentar sanearlo.
var safeComponent = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// Store es el almacenamiento local de un peer.
type Store struct {
	basePath string
}

// NewStore crea un Store sobre basePath. No crea el directorio aquí
// mismo; cada operación crea las carpetas que necesita bajo demanda.
func NewStore(basePath string) *Store {
	return &Store{basePath: basePath}
}

// chunkPath calcula la ruta final de un chunk en disco, validando
// cada componente ANTES de construir cualquier ruta. Es el único
// punto del paquete que sabe cómo se arma la ruta — todas las demás
// funciones pasan por aquí, para no duplicar la validación en cada
// una (y correr el riesgo de que a alguna se le olvide).
func (s *Store) chunkPath(ownerUserID string, chunkID model.ChunkID) (string, error) {
	// Validate chunkID
	fileID, err := chunkID.FileID()
	if err != nil {
		return "", err
	}
	index, err := chunkID.Index()
	if err != nil {
		return "", err
	}

	// Validate each part regexs
	if !safeComponent.MatchString(ownerUserID) {
		return "", fmt.Errorf("ownerUserID contains invalid characters: %q", ownerUserID)
	}
	if !safeComponent.MatchString(fileID) {
		return "", fmt.Errorf("fileID contains invalid characters: %q", fileID)
	}

	path := filepath.Join(s.basePath, ownerUserID, fileID, strconv.Itoa(index))

	// Defensa en profundidad: aunque la whitelist de arriba ya
	// debería hacer esto imposible, verificamos que la ruta final
	// siga dentro de basePath después de Join+Clean. Si el día de
	// mañana alguien relaja safeComponent sin pensar en esto, esta
	// comprobación sigue frenando un path traversal.
	absBase, err := filepath.Abs(s.basePath) // obtenemos ruta absoluta base (por ejm: user/dfsha/storage/)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(path) // Obtenemos ruta absoluta del path construido acá
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absPath, absBase+string(os.PathSeparator)) { // si absPath NO comienza con ruta base + /, retornamos error
		return "", fmt.Errorf("resulted path outside of basePath: %q", absPath)
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
		return err
	}

	dir := filepath.Dir(finalPath)
	// creamos dir con permisos, inicializando err, y luego miramos si no es vacio.
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("creating directory %q: %w", dir, err)
	}

	tmp, err := os.CreateTemp(dir, "chunk-*.tmp") // Os reemplaza * por numero aleatorio
	if err != nil {
		return fmt.Errorf("creating temporal file: %w", err)
	}
	tmpPath := tmp.Name()
	// Si algo falla antes del rename exitoso, se borra el temporal
	// para no dejar basura acumulándose en disco. Si el rename sí
	// tuvo éxito, tmpPath ya no existe y este Remove no hace nada.
	defer os.Remove(tmpPath) // con defer "programamos" ejecución cuando se termine la función.

	if _, err := tmp.Write(envelope); err != nil {
		tmp.Close()
		return fmt.Errorf("writing temporal chunk: %w", err)
	}
	// Sync fuerza el flush a disco antes del rename. Sin esto, el
	// rename podría completarse mientras los datos siguen en el
	// buffer del SO, y una caída justo ahí dejaría el archivo final
	// "renombrado" pero vacío o incompleto en algunos filesystems.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("synchronizing temporal chunk: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temporal chunk: %w", err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("moving chunk to final destination: %w", err)
	}
	return nil
}

// Load lee el envelope completo de un chunk ya guardado.
func (s *Store) Load(ownerUserID string, chunkID model.ChunkID) ([]byte, error) {
	path, err := s.chunkPath(ownerUserID, chunkID)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("chunk %s not found: %w", chunkID, err)
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
	err = os.Remove(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// Exists indica si el chunk está almacenado localmente.
func (s *Store) Exists(ownerUserID string, chunkID model.ChunkID) bool {
	path, err := s.chunkPath(ownerUserID, chunkID)
	if err != nil {
		return false
	}

	_, err = os.Stat(path)
	return err == nil
}

// FreeSpace devuelve el espacio libre del filesystem donde vive
// basePath, en bytes — lo usa heartbeat para reportar al superpeer.
//
// Nota de portabilidad: syscall.Statfs es específico de Linux (y la
// mayoría de Unix).
func (s *Store) FreeSpace() (int64, error) {
	var stat syscall.Statfs_t
	// consultamos info de basePath y lo guardamos es stat
	if err := syscall.Statfs(s.basePath, &stat); err != nil {
		return 0, fmt.Errorf("consulting free space of %q: %w", s.basePath, err)
	}
	return int64(stat.Bavail) * int64(stat.Bsize), nil
}

// ListChunkIDs recorre basePath y reconstruye el ChunkID de cada
// chunk almacenado, a partir de la estructura de carpetas
// (ownerUserID/fileID/index). Se usa para armar el payload del
// heartbeat periódico.
func (s *Store) ListChunkIDs() (map[string][]model.ChunkID, error) {
	ids := make(map[string][]model.ChunkID)

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
		ids[userID] = append(ids[userID], model.NewChunkID(fileID, index))

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("listing chunks in %q: %w", s.basePath, err)
	}
	return ids, nil
}

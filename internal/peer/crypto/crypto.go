//package crypto

// func Encrypt(data, key []byte) ([]byte, error)
// func Decrypt(data, key []byte) ([]byte, error)

// Package crypto implements the client-side cryptography of a peer:
// key derivation (Argon2id + HKDF) and chunk encryption (AES-256-GCM).
//
// Only the peer acting as the file owner (upload) or reader (download)
// calls this package. Storage peers and the superpeer never do: they
// move envelopes around without being able to read them, so they never
// hold any key.
//
// Key hierarchy:
//
//	password + salt (from the superpeer) --Argon2id--> master key
//	master key + file ID                 --HKDF-----> file key
//	file key                             --AES-GCM--> chunk envelopes
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/hkdf"
)

const (
	// KeySize is the size in bytes of every key in this package
	// (a 32-byte key selects AES-256).
	KeySize = 32

	// MinSaltSize is the minimum accepted Argon2id salt length.
	MinSaltSize = 16

	// nonceSize is the standard AES-GCM nonce length (96 bits).
	nonceSize = 12

	// Argon2id parameters (RFC 9106, second recommended option).
	// They are the same for every peer and part of the key derivation:
	// changing any of them changes every master key, which makes all
	// existing files undecryptable. Treat them as a format constant.
	argonTime    = 3         // número de iteraciones (pasadas) que realiza Argon2id sobre su memoria
	argonMemory  = 64 * 1024 // in KiB, so 64 MiB
	argonThreads = 4

	// Aclaración: Argon2id realiza un cálculo que utiliza aproximadamente 64 MiB de memoria y tres pasadas de trabajo sobre ella, distribuyendo el procesamiento en cuatro lanes.
)

// fileKeyInfoPrefix labels the purpose of the derived key (HKDF "info").
// It keeps this key from being reused for anything other than file
// encryption if the master key ever gets other uses.
const fileKeyInfoPrefix = "file-encryption:"

// DeriveMasterKey derives the user's master key from the login password
// and the salt the superpeer stored for that user. It runs once per
// login and the result is kept in memory only, never on disk or network.
//
// It is deliberately slow and memory-hungry (that is what Argon2id is
// for), so it must not be called per chunk or per file.
func DeriveMasterKey(password string, salt []byte) ([]byte, error) {
	if password == "" {
		return nil, errors.New("password must not be empty")
	}
	if len(salt) < MinSaltSize {
		return nil, fmt.Errorf("salt too short: need at least %d bytes, got %d", MinSaltSize, len(salt))
	}
	return argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, KeySize), nil
}

// DeriveFileKey derives the independent key of one file from the master
// key and the file ID. Each file gets its own key, so a leaked file key
// exposes one file only, and the random-nonce budget of AES-GCM is spent
// per file instead of across the whole account.
//
// The HKDF salt is omitted on purpose: the input is already a uniformly
// random master key, so a salt adds nothing.
func DeriveFileKey(masterKey []byte, fileID string) ([]byte, error) {
	if len(masterKey) != KeySize {
		return nil, fmt.Errorf("master key must be %d bytes, got %d", KeySize, len(masterKey))
	}
	if fileID == "" {
		return nil, errors.New("file ID must not be empty")
	}
	// creamos lector de hkdf. Argumentos:
	// 1. La función hash que HKDF utilizará internamente: SHA-256.
	// 2. El material secreto del que se derivará la nueva clave.
	// 3. El salt de HKDF. Aquí no se proporciona uno explícito.
	// 4. El contexto que identifica para qué se utilizará la clave derivada.
	reader := hkdf.New(sha256.New, masterKey, nil, []byte(fileKeyInfoPrefix+fileID)) // lector es un objeto io.reader, con el que generaremos la clave byte por byte, hasta llenar fileKey
	fileKey := make([]byte, KeySize)                                                 // Declara y reserva espacio explicitamente para fileKey, proque necesitamos que sea de tamaño espec

	if _, err := io.ReadFull(reader, fileKey); err != nil { // Intentamos llenar slice fileKey con reader, que es un generador.
		return nil, fmt.Errorf("deriving file key: %w", err)
	}
	return fileKey, nil
}

// Seal encrypts one chunk and returns its envelope: nonce || ciphertext || tag.
//
// The envelope is a single blob on purpose. The same bytes are what gets
// hashed, sent, stored and served, so no other component needs to know
// about nonces or tags. (AES-GCM appends the authentication tag to the
// ciphertext by itself; the only thing added here is the nonce prefix.)
//
// associatedData is authenticated (es decir, es usado para encriptar,
// y por tanto, necesario para la autenticación y poder abrir archivo)
// but not encrypted and not stored in
// the envelope: Open must receive the exact same value or it fails.
// Callers pass the chunk ID, which binds the envelope to its file and
// position, so a storage peer cannot serve a valid chunk of the same
// file in the wrong place. Pass nil to skip this binding.
// associetadData queda vinculado criptográficamente al cifrado, o sea, para poder
// descifrarlo, se necesita. permite comprobar que ese contenido corresponde al chunk que esperabas recibir.
// porque si pediste file1_1, y te mandan file1_2, mandarás eso como associatedData
// y no lo podrás abrir.
// Sin una vinculación al identificador esperado, dos chunks cifrados correctamente podrían intercambiarse y seguir siendo válidos individualmente.
func Seal(plaintext, fileKey, associatedData []byte) ([]byte, error) {
	aead, err := newAEAD(fileKey) // Obtenemos aead que es con lo que cifraremos
	if err != nil {
		return nil, err
	}

	// Fresh random nonce on every call. Reusing a nonce with the same
	// key breaks AES-GCM completely, so it is never derived or cached.
	// The envelope is allocated once, with room for ciphertext and tag.
	// Reservamos espacio para envelope.
	// make([]byte, longitud, capacidad) -> []byte es el tipo. Longitud es cuantos bytes
	// contiene inicialmente (needed para nonce). Y capacidad es hasta dónde puede crecer (espacio que tiene
	// reservado) -> Serán 12 para el nonce, y cap 12 + len + 16
	envelope := make([]byte, nonceSize, nonceSize+len(plaintext)+aead.Overhead())
	if _, err := io.ReadFull(rand.Reader, envelope); err != nil { // Generamos nonce hasta llenar envelope, que de momento es de 12 bytes
		return nil, fmt.Errorf("generating nonce: %w", err)
	}

	// aead.Seal appends ciphertext+tag after the nonce already in envelope.
	// Ciframos y construimos envelope.
	// aead.Seal(dst, nonce, plaintext, additionalData)
	// dst es slice al que se añadirá el resultado (así terminaremos con envelope con nonce + ciphertext + tag)
	// nonce -> por eso pasamos envelope hasta index de nonceSize
	// plaintext -> contenido a cifrar
	// adittionalData -> Datos que se autentican sin cifrarse
	return aead.Seal(envelope, envelope[:nonceSize], plaintext, associatedData), nil
}

// Open reverses Seal: it splits the nonce off the envelope, then decrypts
// and authenticates the rest. Any alteration of the nonce, ciphertext or
// tag, a wrong key, or a different associatedData makes it fail.
//
// This is the cryptographic integrity check. The SHA-256 comparison done
// earlier against the superpeer's hash is a separate, earlier check that
// can reject a bad replica without needing the key.
func Open(envelope, fileKey, associatedData []byte) ([]byte, error) {
	aead, err := newAEAD(fileKey)
	if err != nil {
		return nil, err
	}

	// The envelope comes from the network: check its length before
	// slicing, or a truncated one would panic instead of failing when obtaining
	// nonce and sealed
	if len(envelope) < nonceSize+aead.Overhead() {
		return nil, fmt.Errorf("envelope too short: %d bytes", len(envelope))
	}

	nonce, sealed := envelope[:nonceSize], envelope[nonceSize:] // separamos nonce de texto cifrado y tag
	plaintext, err := aead.Open(nil, nonce, sealed, associatedData)
	if err != nil {
		return nil, fmt.Errorf("decrypting chunk: %w", err)
	}
	return plaintext, nil
}

// newAEAD builds the AES-256-GCM cipher for a file key. It insists on a
// 32-byte key because aes.NewCipher would silently accept 16 or 24 bytes
// and fall back to AES-128 or AES-192.
func newAEAD(fileKey []byte) (cipher.AEAD, error) {
	if len(fileKey) != KeySize {
		return nil, fmt.Errorf("file key must be %d bytes, got %d", KeySize, len(fileKey))
	}
	block, err := aes.NewCipher(fileKey) // crea el cifrador AES. Como la clave tiene 32 bytes, utiliza AES-256.
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}
	// configura AES en modo GCM
	aead, err := cipher.NewGCM(block) // standard 12-byte nonce, matches nonceSize.
	if err != nil {
		return nil, fmt.Errorf("creating GCM: %w", err)
	}
	return aead, nil
}

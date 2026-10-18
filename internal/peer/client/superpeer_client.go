package client

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"sync"

	"dfsha/internal/peer/http_client"
)

// ErrNotLoggedIn is returned, without sending anything, when an endpoint
// that needs a session token is called before Login succeeded.
var ErrNotLoggedIn = errors.New("not logged in")

// SuperpeerClient makes this peer's OUTBOUND calls to the superpeer.
// Its methods live in several files by topic (superpeer_auth.go,
// superpeer_peers.go, superpeer_fs.go, superpeer_files.go); this file
// holds what they share.
//
// Two things matter here:
//   - It never stores a superpeer URL. Every request asks the pool for the
//     active one, so when the heartbeat switches superpeers, all
//     operations follow without anyone telling the client.
//   - Every error that means "superpeer down" leaves here tagged with
//     ErrSuperpeerUnavailable (see AsSuperpeerError), which is what the
//     heartbeat counts.
type SuperpeerClient struct {
	pool *SuperpeerPool
	http *http_client.HTTPClient

	// mu protects token: Login writes it while the heartbeat goroutine and
	// the HTTP handlers (put/ack) read it.
	mu    sync.RWMutex
	token string
}

// NewSuperpeerClient builds a SuperpeerClient. The session starts empty:
// call Login first.
func NewSuperpeerClient(pool *SuperpeerPool, h *http_client.HTTPClient) *SuperpeerClient {
	return &SuperpeerClient{pool: pool, http: h}
}

func (c *SuperpeerClient) setToken(token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.token = token
}

func (c *SuperpeerClient) getToken() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.token
}

// call sends an authenticated JSON request (exige que user haya iniciado sesión). endpoint is the path on the
// superpeer, already escaped (for example "/fs/mkdir").
// in:	Valor Go que se enviará como JSON, o nil.
// out:	Puntero donde se decodificará la respuesta, o nil para ignorarla.
func (c *SuperpeerClient) call(ctx context.Context, method, endpoint string, in, out any) error {
	token := c.getToken()
	if token == "" {
		return ErrNotLoggedIn
	}
	return c.send(ctx, method, endpoint, token, in, out)
}

// send is the single place where a request leaves toward the superpeer.
// Login uses it directly with an empty token (the only public endpoint).
func (c *SuperpeerClient) send(ctx context.Context, method, endpoint, token string, in, out any) error {
	target := c.pool.Active() + endpoint
	return http_client.AsSuperpeerError(c.http.DoJSON(ctx, method, target, token, in, out))
}

// cleanLogicalPath turns a logical path typed by the user into a clean
// absolute one: "docs//x/../y" -> "/docs/y". Cleaning happens here, on the
// client, so ".." can never climb above the user's root and the superpeer
// always receives absolute, normalized paths (cd is purely client-side).
func cleanLogicalPath(logicalPath string) (string, error) {
	if logicalPath == "" || strings.ContainsRune(logicalPath, 0) {
		return "", fmt.Errorf("%w: invalid logical path %q", http_client.ErrInvalidInput, logicalPath)
	}
	return path.Clean("/" + logicalPath), nil
}

// filesEndpoint builds "{prefix}/{ruta}" for the endpoints that carry the
// file's logical path inside the URL (get, lock, unlock, delete -> The user will use logical path).
// The path is cleaned, must name a file (not the root), and each segment is
// escaped, so spaces or "?" in a file name cannot change the request.
// prefix: endpoint prefix -> "/fs/get"
// logicalPath: ruta lógica del archivo, por ejemplo "/documentos/informe final.txt"
func filesEndpoint(prefix, logicalPath string) (string, error) {
	clean, err := cleanLogicalPath(logicalPath)
	if err != nil {
		return "", err
	}
	if clean == "/" {
		return "", fmt.Errorf("%w: a file path is required, got the root", http_client.ErrInvalidInput)
	}

	segments := strings.Split(strings.TrimPrefix(clean, "/"), "/") // elimina / inicial, y hace split por / -> "/documentos/informe final.txt" -> "documentos/informe final.txt" -> [documentos, informe..]
	for i, segment := range segments {
		segments[i] = url.PathEscape(segment) // limpiamos y codificamos (por ejm: espacios a %20 )
	}
	return prefix + "/" + strings.Join(segments, "/"), nil
}

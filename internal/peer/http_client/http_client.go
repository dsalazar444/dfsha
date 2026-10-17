package http_client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// NOTA: Do se encarga de la comunicación HTTP y DoJSON de convertir datos entre Go y JSON.

func getMaxChunkSize() int64 {
	// maxChunkSize caps how much of a successful response is read into
	// memory. A chunk envelope is a few MiB; this leaves a wide margin and
	// still stops a misbehaving server from filling this peer's RAM.
	// cantidad de datos que el cliente lee del cuerpo de una respuesta HTTP exitosa
	maxChunkSizeMB := int64(64)

	if v := os.Getenv("MAX_CHUNK_SIZE_MB"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxChunkSizeMB = n
		}
	}
	maxChunkSize := maxChunkSizeMB << 20

	return maxChunkSize
}

const (
	// maxErrorBody caps how much of an error response is kept as message.
	maxErrorBody = 1 << 10 // Tamaño máximo del mensaje de una respuesta HTTP no exitosa que el cliente lee.
)

// HTTPClient is the thin layer over net/http shared by SuperpeerClient and
// PeerClient. It knows nothing about the DFSha API: it only sends a
// request and turns the outcome into one of three things: the body of a
// 2xx answer, a *StatusError for any other status, or an error wrapping
// ErrUnreachable when no answer arrived.
//
// It never retries. Whether to retry, switch replica or give up is a
// decision of the caller (transfer, heartbeat).
type HTTPClient struct {
	client  *http.Client // puntero al cliente HTTP de la biblioteca estándar de Go. Este objeto se encarga de ejecutar las peticiones.
	maxBody int64        // maxBody de respuesta exitosa
}

// NewHTTPClient builds an HTTPClient. timeout is the total budget of one
// request, including reading the body; callers can set a tighter one per
// request with the context.
func NewHTTPClient(timeout time.Duration) *HTTPClient { // timeout es tiempo máximo que puede consumir una petición ejecutada por el cliente (cubre la petición completa, incluida la lectura del cuerpo de la respuesta.)
	return &HTTPClient{
		client:  &http.Client{Timeout: timeout},
		maxBody: getMaxChunkSize() + (1 << 20),
	}
}

// Do sends req and returns the response body when the status is 2xx.
func (c *HTTPClient) Do(req *http.Request) ([]byte, error) { // Recibe una petición ya construida y decide qué retornar según el resultado de la comunicación.
	// Ejecutamos petición
	resp, err := c.client.Do(req) // el error que retorna Do, son errores de comunicación, si esta falla

	if err != nil {
		// Both %w keep the cause inspectable: errors.Is(err, ErrUnreachable)
		// for the failover logic, and errors.Is(err, context.Canceled) for
		// callers that need to tell shutdown apart from a failure.
		return nil, fmt.Errorf("%w: %w", ErrUnreachable, err) // Como fue un error de comunicación, es un error unreachable
	}
	// Si la petición tuvo éxito a nivel de transporte, resp contiene la respuesta y resp.Body permite leer su cuerpo.
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 { // Son errores
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))                            // Crea un lector que permite consumir como máximo la cantidad de bytes indicada
		return nil, &StatusError{Code: resp.StatusCode, Message: strings.TrimSpace(string(msg))} // retornamos struct statusError creada en errors.go
	}

	// Read one byte more than the cap to tell "exactly the cap" from
	// "too large".
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxBody+1)) // Leer el cuerpo de una respuesta exitosa. Más uno, para ver si sigue endline o no, y detectar exceso
	if err != nil {                                                 // lectura falla -> Aunque el servidor haya comenzado a responder correctamente, podría interrumpir la conexión antes de completar el cuerpo.
		// The connection dropped halfway through the body.
		return nil, fmt.Errorf("%w: reading response: %w", ErrUnreachable, err)
	}
	if int64(len(body)) > c.maxBody { // No es unreachable, porque server respondió, solo que excedió limite
		return nil, fmt.Errorf("response larger than %d bytes", c.maxBody)
	}
	// Si no hubo error de lectura y el cuerpo no supera el límite, devuelve los bytes y nil como error
	return body, nil
}

// DoJSON sends in as a JSON body (nil for no body) and decodes the answer
// into out (nil to ignore it: several endpoints reply with just a text
// message). A non-empty token is sent as a Bearer token.

// A diferencia de Do, este método no recibe una petición ya construida. Recibe los datos necesarios para construirla y procesar una respuesta JSON
// ctx: Controlar cancelación y tiempo límite por petición.
// method:	Método HTTP, como GET o POST.
// url:	Dirección a la que se enviará la petición.
// token:	Token opcional de autenticación.
// in:	Valor Go que se enviará como JSON, o nil.
// out:	Puntero donde se decodificará la respuesta, o nil para ignorarla.
func (c *HTTPClient) DoJSON(ctx context.Context, method, url, token string, in, out any) error {
	var body io.Reader

	if in != nil { // si in es nil, es porque no tendrá contenido en body.
		payload, err := json.Marshal(in) // convierte el valor Go en bytes JSON.
		if err != nil {                  // si falla conversión, se devuelve error
			return fmt.Errorf("encoding request: %w", err)
		}
		body = bytes.NewReader(payload) // si se transforma bien, crea un lector sobre los bytes JSON. Ese lector se puede utilizar como cuerpo de la petición HTTP.
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body) // creamos request
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	// Agregamos headers
	if in != nil { // si habia in, que es donde se especificaba el "json", el content será json
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	respBody, err := c.Do(req) // COmo ya creamos req a partir de json, la mandamos con DO.
	if err != nil {
		return err
	}
	if out == nil { // Si no se proporcionó una variable de salida, la función termina sin intentar interpretar el cuerpo
		return nil
	}
	if err := json.Unmarshal(respBody, out); err != nil { // interpreta los bytes JSON y coloca los valores en la variable de destino.
		return fmt.Errorf("decoding response: %w", err)
	}
	return nil
}

// errInvalidInput is returned for malformed URLs or empty required arguments.
var errInvalidInput = errors.New("invalid input")

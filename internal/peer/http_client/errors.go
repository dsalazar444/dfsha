package http_client

import (
	"errors"
	"fmt"
	"net/http"
)

// Un error centinela es un error identificable por una variable concreta. En lugar de comparar el texto del error, el código puede preguntar si un error corresponde a uno de estos valores mediante errors.Is.

// ErrSuperpeerUnavailable marks a failure that means "this superpeer
// cannot be reached or is broken", as opposed to "it answered and said
// no". The heartbeat counts only these toward switching superpeers.
// SuperpeerClient adds it (through asSuperpeerError) whenever
// IsUnavailable(err) is true.
var ErrSuperpeerUnavailable = errors.New("superpeer unavailable")

// ErrUnreachable means no valid HTTP answer was obtained: connection
// refused, DNS failure, timeout, or the connection dropped mid-response.
// It is the generic "transport failure" used for peers and superpeers.
var ErrUnreachable = errors.New("unreachable")

// Sentinels for the answers callers need to react to. They are matched
// with errors.Is against the *StatusError that httpclient returns, which
// keeps the server's message available for the user.
var (
	ErrBadRequest   = errors.New("bad request")  // 400
	ErrUnauthorized = errors.New("unauthorized") // 401: missing or expired token -> log in again
	ErrForbidden    = errors.New("forbidden")    // 403
	ErrNotFound     = errors.New("not found")    // 404
	ErrConflict     = errors.New("conflict")     // 409: e.g. file locked by someone else or UIDD alredy exists
)

// StatusError is a non-2xx HTTP answer. The peer and the superpeer send a
// plain-text message as body, which ends up in Message.
type StatusError struct {
	Code    int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.Code, e.Message)
}

// Is lets errors.Is(err, client.ErrNotFound) work on a *StatusError
// without losing the message or the numeric code.
func (e *StatusError) Is(target error) bool {
	switch target {
	case ErrBadRequest:
		return e.Code == http.StatusBadRequest
	case ErrUnauthorized:
		return e.Code == http.StatusUnauthorized
	case ErrForbidden:
		return e.Code == http.StatusForbidden
	case ErrNotFound:
		return e.Code == http.StatusNotFound
	case ErrConflict:
		return e.Code == http.StatusConflict
	}
	return false
}

// StatusMeansUnavailable reports whether an HTTP status code should be
// treated as "server down". 5xx counts, except 503: a server that answers
// 503 is alive but not ready to serve (for example a standby that has not
// been promoted yet), and switching away would just make the peers bounce
// between superpeers.
func StatusMeansUnavailable(statusCode int) bool {
	return (statusCode >= 500 || statusCode == 421) && statusCode != http.StatusServiceUnavailable // 503
}

// IsUnavailable reports whether err means the other side is down, as
// opposed to having answered with a refusal.
// Este error indica que el otro servidor no está disponible?
func IsUnavailable(err error) bool {
	if errors.Is(err, ErrUnreachable) {
		return true
	}
	var statusErr *StatusError
	return errors.As(err, &statusErr) && StatusMeansUnavailable(statusErr.Code)
}

// asSuperpeerError tags errors that mean "superpeer down" with
// ErrSuperpeerUnavailable, keeping the original error wrapped too. Every
// SuperpeerClient method passes its error through here.
func AsSuperpeerError(err error) error {
	if err != nil && IsUnavailable(err) {
		return fmt.Errorf("%w: %w", ErrSuperpeerUnavailable, err)
	}
	return err
}

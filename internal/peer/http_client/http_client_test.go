package http_client

import (
	"encoding/pem"
	"os"
	"path/filepath"

	"context"
	"dfsha/internal/shared/tlsconf"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newTestClient() *HTTPClient { return NewHTTPClient(2*time.Second, 64) }

// Construye una petición GET y la ejecuta usando c.Do(req).
func get(t *testing.T, c *HTTPClient, url string) ([]byte, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c.Do(req)
}

func TestDoReturnsBodyOn2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello"))
	}))
	defer srv.Close()

	body, err := get(t, newTestClient(), srv.URL)
	if err != nil || string(body) != "hello" {
		t.Fatalf("got %q, %v", body, err)
	}
}

func TestDoMapsStatusCodes(t *testing.T) {
	cases := []struct {
		code        int
		sentinel    error // expected errors.Is match, nil if none
		unavailable bool
	}{
		{400, ErrBadRequest, false},
		{401, ErrUnauthorized, false},
		{403, ErrForbidden, false},
		{404, ErrNotFound, false},
		{409, ErrConflict, false},
		{500, nil, true},
		{502, nil, true},
		{503, nil, false}, // alive but not ready: must NOT count as down
		{504, nil, true},
	}
	for _, tc := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "  the reason \n", tc.code)
		}))
		_, err := get(t, newTestClient(), srv.URL)
		srv.Close()

		var statusErr *StatusError
		if !errors.As(err, &statusErr) || statusErr.Code != tc.code {
			t.Fatalf("status %d: got %v, want *StatusError", tc.code, err)
		}
		if statusErr.Message != "the reason" {
			t.Errorf("status %d: message %q not trimmed", tc.code, statusErr.Message)
		}
		if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
			t.Errorf("status %d: errors.Is(%v) false", tc.code, tc.sentinel)
		}
		if IsUnavailable(err) != tc.unavailable {
			t.Errorf("status %d: IsUnavailable = %v, want %v", tc.code, IsUnavailable(err), tc.unavailable)
		}
	}
}

func TestDoConnectionRefusedIsUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // nobody listens any more

	_, err := get(t, newTestClient(), url)
	if !errors.Is(err, ErrUnreachable) || !IsUnavailable(err) {
		t.Errorf("got %v, want ErrUnreachable", err)
	}
}

func TestDoTimeoutIsUnreachableAndKeepsCause(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(time.Second):
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	_, err := newTestClient().Do(req)

	if !errors.Is(err, ErrUnreachable) {
		t.Errorf("timeout is not ErrUnreachable: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("the cause was lost: %v", err)
	}
}

func TestDoRejectsOversizedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", 100)))
	}))
	defer srv.Close()

	c := newTestClient()
	c.maxBody = 99
	_, err := get(t, c, srv.URL)
	if err == nil || IsUnavailable(err) {
		t.Errorf("got %v, want a non-unavailable 'too large' error", err)
	}

	c.maxBody = 100 // exactly the cap is fine
	if _, err := get(t, c, srv.URL); err != nil {
		t.Errorf("body of exactly maxBody bytes rejected: %v", err)
	}
}

func TestDoJSON(t *testing.T) {
	var gotAuth, gotType, gotBody, gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotType = r.Header.Get("Content-Type")
		gotMethod = r.Method
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Write([]byte(`{"name":"bob"}`))
	}))
	defer srv.Close()

	var out struct{ Name string }
	in := map[string]string{"path": "/docs"}
	err := newTestClient().DoJSON(context.Background(), http.MethodPost, srv.URL, "tok123", in, &out)
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer tok123" || gotType != "application/json" || gotMethod != "POST" {
		t.Errorf("headers/method: %q %q %q", gotAuth, gotType, gotMethod)
	}
	if gotBody != `{"path":"/docs"}` {
		t.Errorf("body = %s", gotBody)
	}
	if out.Name != "bob" {
		t.Errorf("decoded %+v", out)
	}
}

func TestDoJSONWithoutTokenOrBodyOrOutput(t *testing.T) {
	var gotAuth, gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotType = r.Header.Get("Authorization"), r.Header.Get("Content-Type")
		w.Write([]byte("plain text message, not JSON"))
	}))
	defer srv.Close()

	// out == nil: a text answer must be ignored, not decoded.
	if err := newTestClient().DoJSON(context.Background(), http.MethodGet, srv.URL, "", nil, nil); err != nil {
		t.Fatal(err)
	}
	if gotAuth != "" || gotType != "" {
		t.Errorf("unexpected headers: auth=%q type=%q", gotAuth, gotType)
	}
}

func TestDoJSONBadResponseJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("not json"))
	}))
	defer srv.Close()

	var out map[string]string
	err := newTestClient().DoJSON(context.Background(), http.MethodGet, srv.URL, "", nil, &out)
	if err == nil || IsUnavailable(err) {
		t.Errorf("got %v, want a decoding error that is not 'unavailable'", err)
	}
}

func TestAsSuperpeerError(t *testing.T) {
	down := AsSuperpeerError(&StatusError{Code: 500, Message: "boom"})
	if !errors.Is(down, ErrSuperpeerUnavailable) {
		t.Error("500 was not tagged as superpeer unavailable")
	}
	var statusErr *StatusError
	if !errors.As(down, &statusErr) {
		t.Error("original *StatusError was lost")
	}

	if !errors.Is(AsSuperpeerError(ErrUnreachable), ErrSuperpeerUnavailable) {
		t.Error("transport failure was not tagged")
	}

	refused := AsSuperpeerError(&StatusError{Code: 401, Message: "expired"})
	if errors.Is(refused, ErrSuperpeerUnavailable) || !errors.Is(refused, ErrUnauthorized) {
		t.Error("a 401 must stay a 401 and not count as down")
	}
	if AsSuperpeerError(nil) != nil {
		t.Error("nil must stay nil")
	}
}

// A client built with tlsconf trusts a server only if its certificate comes from the CA file.
func TestDoOverTLSTrustsOnlyTheGivenCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("secure"))
	}))
	defer srv.Close()

	// The test server's own certificate plays the role of the CA.
	caFile := filepath.Join(t.TempDir(), "ca.crt")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(caFile, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}

	trusting, err := tlsconf.NewClient(caFile, "", "", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	body, err := get(t, NewHTTPClientWith(trusting, 64), srv.URL)
	if err != nil || string(body) != "secure" {
		t.Fatalf("with the CA: got %q, %v", body, err)
	}

	// Without the CA the same server is rejected, and reported as unreachable.
	if _, err := get(t, newTestClient(), srv.URL); !errors.Is(err, ErrUnreachable) {
		t.Errorf("without the CA: err = %v, want ErrUnreachable", err)
	}
}

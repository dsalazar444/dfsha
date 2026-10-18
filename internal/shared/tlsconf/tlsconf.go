// Package tlsconf is the HTTPS setup shared by the superpeer, the peer and the CLI.
// It has two parts, one for each side of a connection:
//   - ListenAndServe: a server answers over HTTPS when it has a certificate
//   - NewClient: a client trusts the servers whose certificate was signed by the DFSha CA
//
// The certificates come from deploy/certs/gen-certs.sh. Only one side checks the other (the client
// checks the server); who is allowed to talk to the superpeer is a separate matter (tokens, API keys)

package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// ListenAndServe starts srv over HTTPS when certFile and keyFile are given
//
// Without certificates it falls back to plain HTTP, but only if allowPlain is true (for tests and local
// development); with allowPlain false it refuses to start. Every component calls it with the same flag,
// so making TLS mandatory at the end of the project is changing that one value, not the code
//
// Like http.Server.ListenAndServe, it blocks until the server stops
func ListenAndServe(srv *http.Server, certFile, keyFile string, allowPlain bool) error {
	switch {
	case certFile != "" && keyFile != "":
		log.Printf("[tls] serving HTTPS on %s", srv.Addr)
		return srv.ListenAndServeTLS(certFile, keyFile)

	case certFile != "" || keyFile != "":
		return errors.New("TLS needs both the certificate and the key (TLS_CERT and TLS_KEY), only one was given")

	case !allowPlain:
		return errors.New("TLS is required: set TLS_CERT and TLS_KEY (or ALLOW_INSECURE=true for local tests only)")

	default:
		log.Printf("[tls] WARNING: no certificate, serving plain HTTP on %s — traffic is not encrypted", srv.Addr)
		return srv.ListenAndServe()
	}
}

// NewClient returns an http.Client that trusts the servers whose certificate was signed by the CA in caFile
// Only that CA is trusted (not the ones of the system): in this project every server is signed by ours
// With an empty caFile it is a plain client, which works for http:// addresses (development)
// timeout is the total time one request may take
func NewClient(caFile string, timeout time.Duration) (*http.Client, error) {
	if caFile == "" {
		return &http.Client{Timeout: timeout}, nil
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%q has no valid certificate", caFile)
	}

	// Clone keeps the defaults of the standard transport (connection reuse, proxy, timeouts)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &http.Client{Timeout: timeout, Transport: transport}, nil
}

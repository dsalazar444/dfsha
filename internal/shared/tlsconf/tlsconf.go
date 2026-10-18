// Package tlsconf is the HTTPS setup shared by the superpeer, the peer and the CLI.
// It has two parts, one for each side of a connection:
//   - ListenAndServe / Serve: a server answers over HTTPS when it has a certificate
//   - NewClient: a client trusts the servers whose certificate was signed by the DFSha CA
//
// The certificates come from deploy/certs/gen-certs.sh. By default only one side checks the other (the client
// checks the server); who is allowed to talk to the superpeer is a separate matter (tokens).
// RequireClientCert turns a server into a mutual-TLS one: it also demands a certificate from the client and
// checks it against the CA. The peers use it on their data port, so only nodes of the system can send, fetch or
// delete chunks. A client presents its own certificate when NewClient gets one

package tlsconf

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
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
	useTLS, err := mode(certFile, keyFile, allowPlain)
	if err != nil {
		return err
	}
	if useTLS {
		log.Printf("[tls] serving HTTPS on %s", srv.Addr)
		return srv.ListenAndServeTLS(certFile, keyFile)
	}
	log.Printf("[tls] WARNING: no certificate, serving plain HTTP on %s — traffic is not encrypted", srv.Addr)
	return srv.ListenAndServe()
}

// Serve is ListenAndServe for a listener that is already open (the peer opens its port before
// it registers with the superpeer, and only starts answering afterwards)
// It does not close ln when the server fails to start
func Serve(srv *http.Server, ln net.Listener, certFile, keyFile string, allowPlain bool) error {
	useTLS, err := mode(certFile, keyFile, allowPlain)
	if err != nil {
		return err
	}
	if useTLS {
		log.Printf("[tls] serving HTTPS on %s", ln.Addr())
		return srv.ServeTLS(ln, certFile, keyFile)
	}
	log.Printf("[tls] WARNING: no certificate, serving plain HTTP on %s — traffic is not encrypted", ln.Addr())
	return srv.Serve(ln)
}

// mode decides between HTTPS (true) and plain HTTP (false), or refuses: the single place with the
// rules that ListenAndServe and Serve share
func mode(certFile, keyFile string, allowPlain bool) (useTLS bool, err error) {
	switch {
	case certFile != "" && keyFile != "":
		return true, nil
	case certFile != "" || keyFile != "":
		return false, errors.New("TLS needs both the certificate and the key (TLS_CERT and TLS_KEY), only one was given")
	case !allowPlain:
		return false, errors.New("TLS is required: set TLS_CERT and TLS_KEY (or ALLOW_INSECURE=true for local tests only)")
	}
	return false, nil
}

// NewClient returns an http.Client that trusts the servers whose certificate was signed by the CA in caFile
// Only that CA is trusted (not the ones of the system): in this project every server is signed by ours
// With an empty caFile it is a plain client, which works for http:// addresses (development)
// certFile and keyFile are the client's own certificate and key, which a server with RequireClientCert asks for
// (empty: none is presented). A server that does not ask for one never sees it
// timeout is the total time one request may take
func NewClient(caFile, certFile, keyFile string, timeout time.Duration) (*http.Client, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("a client certificate needs both the certificate and the key")
	}
	if caFile == "" && certFile == "" {
		return &http.Client{Timeout: timeout}, nil
	}

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pool, err := loadPool(caFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("loading the client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Clone keeps the defaults of the standard transport (connection reuse, proxy, timeouts)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return &http.Client{Timeout: timeout, Transport: transport}, nil
}

// RequireClientCert makes srv demand a certificate from every client, signed by the CA in caFile
// (mutual TLS). A client without one, or with one from another CA, fails in the handshake and never
// reaches the handlers. It only has effect when the server serves HTTPS (see Serve)
func RequireClientCert(srv *http.Server, caFile string) error {
	pool, err := loadPool(caFile)
	if err != nil {
		return err
	}
	srv.TLSConfig = &tls.Config{ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	return nil
}

// loadPool reads a PEM file with the CA certificate
func loadPool(caFile string) (*x509.CertPool, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading CA certificate: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("%q has no valid certificate", caFile)
	}
	return pool, nil
}

package tlsconf

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// pemFile writes a PEM block to dir/name and returns the path
func pemFile(t *testing.T, dir, name, blockType string, der []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// makeCA creates a CA and returns the path of its certificate and a function that issues
// server certificates signed by it, valid only for the IP 127.0.0.1 (not for the name "localhost")
func makeCA(t *testing.T, dir, name string) (caFile string, issue func() (certFile, keyFile string)) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	caFile = pemFile(t, dir, name+".crt", "CERTIFICATE", caDER)

	issue = func() (string, string) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(2),
			Subject:      pkix.Name{CommonName: "server"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
			IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, _ := x509.MarshalECPrivateKey(key)
		return pemFile(t, dir, name+"-server.crt", "CERTIFICATE", der), pemFile(t, dir, name+"-server.key", "EC PRIVATE KEY", keyDER)
	}
	return caFile, issue
}

// startServer runs ListenAndServe on a free port and returns the port
// The server answers "hello" to every request and is left running until the test process ends
func startServer(t *testing.T, certFile, keyFile string, allowPlain bool) (port string, errCh chan error) {
	t.Helper()
	ln, err := net.Listen("tcp", ":0") // all interfaces, so both 127.0.0.1 and "localhost" reach it
	if err != nil {
		t.Fatal(err)
	}
	port = strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
	ln.Close() // the port is free again for the server (there is a tiny window where another program could take it)

	srv := &http.Server{Addr: ":" + port, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("hello"))
	})}
	t.Cleanup(func() { srv.Close() })

	errCh = make(chan error, 1)
	go func() { errCh <- ListenAndServe(srv, certFile, keyFile, allowPlain) }()
	waitUntilListening(t, port, errCh)
	return port, errCh
}

// waitUntilListening waits for the port to accept connections (or for ListenAndServe to fail)
func waitUntilListening(t *testing.T, port string, errCh chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			errCh <- err // leave it for the test to read
			return
		default:
		}
		if c, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 100*time.Millisecond); err == nil {
			c.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the server did not start")
}

// get returns the body of GET url, or the error
func get(client *http.Client, url string) (string, error) {
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body), nil
}

func newClient(t *testing.T, caFile string) *http.Client {
	t.Helper()
	client, err := NewClient(caFile, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestHTTPSWorksWhenClientTrustsTheCA(t *testing.T) {
	dir := t.TempDir()
	caFile, issue := makeCA(t, dir, "ca")
	cert, key := issue()
	port, _ := startServer(t, cert, key, false)

	body, err := get(newClient(t, caFile), "https://127.0.0.1:"+port)
	if err != nil || body != "hello" {
		t.Fatalf("body = %q, err = %v; want hello", body, err)
	}
}

func TestHTTPSIsRejectedWhenTheClientDoesNotTrustTheCA(t *testing.T) {
	dir := t.TempDir()
	_, issue := makeCA(t, dir, "ca")
	otherCA, _ := makeCA(t, dir, "other")
	cert, key := issue()
	port, _ := startServer(t, cert, key, false)

	cases := map[string]*http.Client{
		"client without CA":      newClient(t, ""),
		"client with another CA": newClient(t, otherCA),
	}
	for name, client := range cases {
		if body, err := get(client, "https://127.0.0.1:"+port); err == nil {
			t.Errorf("%s: connected (body %q), want a certificate error", name, body)
		}
	}
}

func TestHTTPSIsRejectedWhenTheNameIsNotInTheCertificate(t *testing.T) {
	dir := t.TempDir()
	caFile, issue := makeCA(t, dir, "ca")
	cert, key := issue() // valid for the IP 127.0.0.1 only
	port, _ := startServer(t, cert, key, false)

	_, err := get(newClient(t, caFile), "https://localhost:"+port)
	if err == nil || !strings.Contains(err.Error(), "not localhost") && !strings.Contains(err.Error(), "valid for") {
		t.Fatalf("err = %v; want a name mismatch error", err)
	}
}

func TestPlainHTTPOnlyWhenAllowed(t *testing.T) {
	// allowPlain = true: no certificates, plain HTTP (development and tests)
	port, _ := startServer(t, "", "", true)
	if body, err := get(newClient(t, ""), "http://127.0.0.1:"+port); err != nil || body != "hello" {
		t.Fatalf("plain HTTP: body = %q, err = %v", body, err)
	}

	// allowPlain = false: it must refuse to start (this is what makes TLS mandatory at the end)
	srv := &http.Server{Addr: "127.0.0.1:0"}
	if err := ListenAndServe(srv, "", "", false); err == nil {
		t.Fatal("started without certificates although plain HTTP is not allowed")
	}
}

func TestOnlyOneOfCertAndKeyIsAnError(t *testing.T) {
	srv := &http.Server{Addr: "127.0.0.1:0"}
	if err := ListenAndServe(srv, "server.crt", "", true); err == nil {
		t.Fatal("accepted a certificate without a key")
	}
	if err := ListenAndServe(srv, "", "server.key", true); err == nil {
		t.Fatal("accepted a key without a certificate")
	}
}

func TestNewClientRejectsBadCAFile(t *testing.T) {
	if _, err := NewClient(filepath.Join(t.TempDir(), "no-existe.crt"), time.Second); err == nil {
		t.Error("accepted a CA file that does not exist")
	}
	notACert := filepath.Join(t.TempDir(), "x.crt")
	_ = os.WriteFile(notACert, []byte("esto no es un certificado"), 0o600)
	if _, err := NewClient(notACert, time.Second); err == nil {
		t.Error("accepted a file that is not a certificate")
	}
}

// The certificates made by deploy/certs/gen-certs.sh (with openssl) must work with Go too
// The test is skipped when they have not been generated
func TestCertificatesFromTheScript(t *testing.T) {
	out := filepath.Join("..", "..", "..", "deploy", "certs", "out")
	cert, key, ca := filepath.Join(out, "sp1.crt"), filepath.Join(out, "sp1.key"), filepath.Join(out, "ca.crt")
	for _, f := range []string{cert, key, ca} {
		if _, err := os.Stat(f); err != nil {
			t.Skipf("%s not found: run bash deploy/certs/gen-certs.sh first", f)
		}
	}

	// The folder may hold certificates made for other names (for example AWS addresses): only the default ones apply here
	if !certHasName(t, cert, "localhost") {
		t.Skip("sp1.crt was not made with the default names: run bash deploy/certs/gen-certs.sh without arguments")
	}

	port, _ := startServer(t, cert, key, false)
	client := newClient(t, ca)

	// sp1's certificate has the names sp1, localhost and 127.0.0.1
	for _, host := range []string{"localhost", "127.0.0.1"} {
		if body, err := get(client, "https://"+host+":"+port); err != nil || body != "hello" {
			t.Errorf("https://%s: body = %q, err = %v", host, body, err)
		}
	}
}

// certHasName reports whether the certificate in certFile lists name among its DNS names
func certHasName(t *testing.T, certFile, name string) bool {
	t.Helper()
	data, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("%s has no PEM block", certFile)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cert.VerifyHostname(name) == nil
}

// Serve works on a listener that is already open (what the peer does)
func TestServeUsesAnOpenListener(t *testing.T) {
	dir := t.TempDir()
	caFile, issue := makeCA(t, dir, "ca")
	certFile, keyFile := issue()
	client, err := NewClient(caFile, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name, cert, key, scheme string
	}{
		{"with certificate", certFile, keyFile, "https"},
		{"without certificate", "", "", "http"},
	} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("hello")) })}
		t.Cleanup(func() { srv.Close() })
		go Serve(srv, ln, tc.cert, tc.key, true)

		url := tc.scheme + "://" + ln.Addr().String()
		var body string
		for i := 0; i < 50; i++ { // the goroutine needs a moment to start
			if body, err = get(client, url); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil || body != "hello" {
			t.Errorf("%s: got %q, %v", tc.name, body, err)
		}
	}

	// The rules are the same as ListenAndServe's.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	if err := Serve(&http.Server{}, ln, "", "", false); err == nil {
		t.Error("Serve started without TLS and without ALLOW_INSECURE")
	}
	if err := Serve(&http.Server{}, ln, "only.crt", "", true); err == nil {
		t.Error("Serve accepted a certificate without a key")
	}
}

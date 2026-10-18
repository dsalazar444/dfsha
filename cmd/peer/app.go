package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"dfsha/internal/peer/cli"
	"dfsha/internal/peer/client"
	"dfsha/internal/peer/config"
	"dfsha/internal/peer/crypto"
	"dfsha/internal/peer/heartbeat"
	"dfsha/internal/peer/http_client"
	"dfsha/internal/peer/server"
	"dfsha/internal/peer/storage"
	"dfsha/internal/peer/transfer"
	"dfsha/internal/shared/model"
	"dfsha/internal/shared/tlsconf"
)

const (
	// httpTimeout is the total budget of one outgoing HTTP request. Tighter
	// limits (the 3 s heartbeat, the 10 s ack) are set with contexts.
	httpTimeout = 60 * time.Second

	// shutdownGrace is how long in-flight chunk transfers get to finish
	// when the peer stops.
	shutdownGrace = 5 * time.Second
)

// app holds what the peer process needs from the outside world, so the
// whole startup can be run by a test with its own input, output, config
// and listener.
type app struct {
	cfg      config.Config
	in       *bufio.Reader // standard input, shared by the prompts and the command line
	out      io.Writer
	terminal bool // input is a terminal: passwords are read without echo

	// listener is the socket for the HTTP server. Nil means "listen on
	// cfg.PeerPort"; tests pass one they already opened.
	listener net.Listener
}

// session is what a successful login produces.
type session struct {
	username  string
	userID    string
	masterKey []byte // memory only, never written anywhere
}

// run starts every part of the peer, in an order that matters, and blocks
// until the user leaves or ctx is cancelled.
//
//  1. Open the TCP port. Before registering, because the moment the
//     superpeer knows this peer it may assign it chunks, and other peers
//     will connect. A socket that is open but not served yet is fine: the
//     operating system queues those connections until Serve starts.
//  2. Log in, derive the master key.
//  3. Register with the superpeer (this gives the peer ID the HTTP server
//     needs to send acks).
//  4. Serve HTTP, start the heartbeat.
//  5. Run the command line.
//  6. On exit, stop everything.
func (a *app) run(ctx context.Context) error {
	// A cause lets a background part (the HTTP server) end the whole peer
	// with a reason, while Ctrl-C and "exit" end it normally.
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	store, err := storage.NewStore(a.cfg.StoragePath)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	pool, err := client.NewSuperpeerPool(a.cfg.SuperpeerURLs)
	if err != nil {
		return err
	}
	tlsClient, err := tlsconf.NewClient(a.cfg.TLSCA, httpTimeout) // trusts the DFSha CA for https superpeers and peers
	if err != nil {
		return fmt.Errorf("TLS: %w", err)
	}
	httpClient := http_client.NewHTTPClientWith(tlsClient, a.cfg.MaxChunkSizeMB)
	superpeer := client.NewSuperpeerClient(pool, httpClient)
	peers := client.NewPeerClient(httpClient)

	// Abrimos puerto
	listener := a.listener
	if listener == nil {
		listener, err = net.Listen("tcp", ":"+a.cfg.PeerPort)
		if err != nil {
			return fmt.Errorf("listening on port %s: %w", a.cfg.PeerPort, err)
		}
	}
	defer listener.Close()

	// Hacer login
	sess, err := a.login(ctx, superpeer, pool)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.out, "logged in as %s\n", sess.username)

	// registra peer
	peerID, err := superpeer.RegisterPeer(ctx, a.cfg.PeerAddress, a.cfg.PeerPort)
	if err != nil {
		return fmt.Errorf("registering with the superpeer: %w", err)
	}
	fmt.Fprintf(a.out, "registered as %s, serving chunks on port %s\n", peerID, a.cfg.PeerPort)

	// levanatamos server
	httpServer := &http.Server{
		Handler:           server.NewServer(store, superpeer, peerID, a.cfg.MaxChunkSizeMB).Routes(),
		ReadHeaderTimeout: 10 * time.Second, // do not let a client hold a connection open without sending headers
	}
	go func() {
		if err := tlsconf.Serve(httpServer, listener, a.cfg.TLSCert, a.cfg.TLSKey, a.cfg.AllowInsecure); !errors.Is(err, http.ErrServerClosed) {
			cancel(fmt.Errorf("HTTP server stopped: %w", err))
		}
	}()

	// realizamos heartbeat
	sender := heartbeat.NewSender(peerID, store, superpeer, peers, a.cfg.HeartbeatInterval, pool)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		sender.Run(ctx)
	}()

	transferer, err := transfer.New(transfer.Config{
		UserID:      sess.userID,
		MasterKey:   sess.masterKey,
		ChunkSize:   a.cfg.ChunkSize,
		Parallelism: a.cfg.Parallelism,
	}, superpeer, peers)
	if err != nil {
		return err
	}

	fmt.Fprintln(a.out, "type help for the list of commands")
	cliErr := cli.New(superpeer, transferer, a.relogin(superpeer, sess)).Run(ctx, a.in, a.out)

	// Shutdown. Remember why the context ended before cancelling it
	// ourselves: a dead HTTP server is an error, "exit" and Ctrl-C are not.
	cause := context.Cause(ctx)
	cancel(nil)

	shutdownCtx, stopShutdown := context.WithTimeout(context.Background(), shutdownGrace)
	defer stopShutdown()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("stopping the HTTP server: %v", err)
	}
	select {
	case <-heartbeatDone:
	case <-time.After(shutdownGrace):
		log.Printf("heartbeat did not stop in time")
	}

	if cliErr != nil {
		return fmt.Errorf("command line: %w", cliErr)
	}
	if cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}
	return nil
}

// login asks for the credentials, authenticates, and derives the master
// key from the password and the salt the superpeer stored for the user.
func (a *app) login(ctx context.Context, superpeer *client.SuperpeerClient, pool *client.SuperpeerPool) (session, error) {
	username := a.cfg.Username
	if username == "" {
		var err error
		if username, err = a.readLine("username: "); err != nil {
			return session{}, fmt.Errorf("reading username: %w", err)
		}
	}
	password, err := a.readPassword("password: ")
	if err != nil {
		return session{}, fmt.Errorf("reading password: %w", err)
	}
	defer wipe(password)

	resp, err := loginAnySuperpeer(ctx, superpeer, pool, username, string(password))
	if err != nil {
		return session{}, fmt.Errorf("login: %w", err)
	}

	salt, err := decodeSalt(resp.Salt)
	if err != nil {
		return session{}, err
	}
	// Deliberately slow (Argon2id): this is the cost an attacker pays per
	// password guess. It runs once per login.
	masterKey, err := crypto.DeriveMasterKey(string(password), salt)
	if err != nil {
		return session{}, fmt.Errorf("deriving the encryption key: %w", err)
	}
	return session{username: username, userID: resp.UserID, masterKey: masterKey}, nil
}

// relogin builds the function behind the "login" command: it asks for the
// password again and gets a fresh token. The master key does not change
// (same password, same salt), so the rest of the peer keeps working.
func (a *app) relogin(superpeer *client.SuperpeerClient, sess session) func(context.Context) error {
	return func(ctx context.Context) error {
		password, err := a.readPassword("password: ")
		if err != nil {
			return fmt.Errorf("reading password: %w", err)
		}
		defer wipe(password)

		resp, err := superpeer.Login(ctx, sess.username, string(password))
		if err != nil {
			return err
		}
		if resp.UserID != sess.userID {
			return errors.New("the superpeer returned a different user ID; restart the peer")
		}
		return nil
	}
}

// loginAnySuperpeer logs in, moving on to the next superpeer of the list
// while the current one is unreachable. Without this, a peer could not
// start while the first superpeer is down, even with a healthy backup
// configured: the heartbeat that normally does the switching is not
// running yet.
func loginAnySuperpeer(ctx context.Context, superpeer *client.SuperpeerClient, pool *client.SuperpeerPool, username, password string) (model.LoginResponse, error) {
	first := pool.Active()
	for {
		current := pool.Active()
		resp, err := superpeer.Login(ctx, username, password)
		if err == nil || !errors.Is(err, http_client.ErrSuperpeerUnavailable) {
			return resp, err // success, or an answer such as "wrong password"
		}

		next := pool.Rotate(current)
		if next == first {
			return model.LoginResponse{}, err // every superpeer was tried
		}
		log.Printf("superpeer %s is unreachable, trying %s", current, next)
	}
}

// decodeSalt decodes the salt the superpeer sends as base64 text (padded
// or not) into the raw bytes Argon2id needs.
func decodeSalt(encoded string) ([]byte, error) {
	if salt, err := base64.StdEncoding.DecodeString(encoded); err == nil {
		return salt, nil
	}
	salt, err := base64.RawStdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("the salt from the superpeer is not valid base64: %w", err)
	}
	return salt, nil
}

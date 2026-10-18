// Package cli is the interactive command line of a peer: the part the
// user types into. It only parses commands and prints results; every real
// operation is delegated to the superpeer client (namespace operations)
// and to transfer (file content).
//
// The working directory ("cd") lives here, on the client side. The
// superpeer never sees relative paths: every command is resolved to an
// absolute, clean logical path before it is sent.
package cli

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"dfsha/internal/peer/client"
	"dfsha/internal/peer/http_client"
	"dfsha/internal/shared/model"
)

// maxUploadSize bounds the files `put` accepts. Upload keeps the whole
// file and its encrypted copy in memory, so this protects the peer from
// being asked for something that cannot fit.
const maxUploadSize = 200 << 20

// Superpeer is the namespace part of the superpeer client.
// client.SuperpeerClient satisfies it.
type Superpeer interface {
	Mkdir(ctx context.Context, logicalPath string) error
	Rmdir(ctx context.Context, logicalPath string) error
	Ls(ctx context.Context, logicalPath string) ([]model.DirEntry, error)
	DeleteFile(ctx context.Context, logicalPath string) error
	Lock(ctx context.Context, logicalPath string) (model.LockResponse, error)
	Unlock(ctx context.Context, logicalPath, lockID string) error
}

// FileTransfer moves file content. transfer.Transfer satisfies it.
type FileTransfer interface {
	Put(ctx context.Context, logicalPath string, data []byte) (string, error)
	Get(ctx context.Context, logicalPath string) ([]byte, error)
}

// CLI is one interactive session.
type CLI struct {
	superpeer Superpeer
	transfer  FileTransfer

	// relogin asks for the password again and refreshes the session token.
	// nil disables the "login" command.
	relogin func(ctx context.Context) error

	out io.Writer
	cwd string // current logical directory, always absolute
}

// New builds a CLI. relogin may be nil.
func New(superpeer Superpeer, transfer FileTransfer, relogin func(ctx context.Context) error) *CLI {
	return &CLI{superpeer: superpeer, transfer: transfer, relogin: relogin, cwd: "/"}
}

// lineResult is one answer of the input reader goroutine.
type lineResult struct {
	text string
	eof  bool  // the input ended
	err  error // why it ended, if it was an error
}

// Run reads commands from in and writes results to out until the user types
// "exit", the input ends, or ctx is cancelled. A failing command prints its
// error and the session goes on: only I/O problems end Run with an error.
//
// Input is read ONE LINE AT A TIME, and only when the session is ready for
// the next command. That matters: commands such as "login" ask for a
// password on the same terminal, and a reader that had already gone ahead
// to wait for the next line would swallow it and run it as a command,
// printing the password in the error message.
func (c *CLI) Run(ctx context.Context, in io.Reader, out io.Writer) error {
	c.out = out

	// Reading blocks and cannot be interrupted, so it happens in its own
	// goroutine, and Run waits on both its answer and the context. The
	// goroutine reads a line only when it receives a request.
	requests := make(chan struct{})
	results := make(chan lineResult, 1) // buffered: it must not block if Run already left
	defer close(requests)
	go func() {
		scanner := bufio.NewScanner(in)
		for range requests {
			if scanner.Scan() {
				results <- lineResult{text: scanner.Text()}
				continue
			}
			results <- lineResult{eof: true, err: scanner.Err()}
			return
		}
	}()

	for {
		c.prompt()
		requests <- struct{}{}

		select {
		case <-ctx.Done():
			return nil
		case res := <-results:
			if res.eof {
				return res.err
			}
			if quit := c.execute(ctx, res.text); quit {
				return nil
			}
		}
	}
}

func (c *CLI) prompt() { fmt.Fprintf(c.out, "dfsha:%s> ", c.cwd) }

// execute runs one input line and reports whether the session must end.
func (c *CLI) execute(ctx context.Context, line string) (quit bool) {
	args, err := splitArgs(line)
	if err != nil {
		c.printf("error: %v\n", err)
		return false
	}
	if len(args) == 0 {
		return false
	}

	command, args := args[0], args[1:]
	switch command {
	case "exit", "quit":
		return true
	case "help":
		c.help()
	case "pwd":
		c.printf("%s\n", c.cwd)
	case "cd":
		err = c.cd(ctx, args)
	case "ls":
		err = c.ls(ctx, args)
	case "mkdir":
		err = c.onePath(args, "mkdir <path>", func(p string) error { return c.superpeer.Mkdir(ctx, p) })
	case "rmdir":
		err = c.onePath(args, "rmdir <path>", func(p string) error { return c.superpeer.Rmdir(ctx, p) })
	case "rm":
		err = c.onePath(args, "rm <path>", func(p string) error { return c.superpeer.DeleteFile(ctx, p) })
	case "put":
		err = c.put(ctx, args)
	case "get":
		err = c.get(ctx, args)
	case "mdf":
		err = c.mdf(ctx, args)
	case "login":
		err = c.login(ctx, args)
	default:
		c.printf("unknown command %q (type help)\n", command)
	}

	if err != nil {
		c.printf("error: %s\n", friendly(err))
	}
	return false
}

func (c *CLI) printf(format string, a ...any) { fmt.Fprintf(c.out, format, a...) }

func (c *CLI) help() {
	c.printf(`commands:
  pwd                        show the current directory
  cd [path]                  change directory (no argument: go to /)
  ls [path]                  list a directory
  mkdir <path>               create a directory
  rmdir <path>               remove a directory
  rm <path>                  delete a file
  put <local> [remote]       upload a local file, remote is where you want to save it remotely
  get <remote> [local]       download a file (never overwrites), local is where you want to save it locally
  mdf <remote>               edit a file in $EDITOR (locks it, uploads only if changed)
  login                      authenticate again (after "session expired")
  exit                       leave
Paths with spaces go in quotes. Relative paths start at the current directory.
`)
}

// onePath runs a command that takes exactly one logical path.
func (c *CLI) onePath(args []string, usage string, do func(absPath string) error) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: %s", usage)
	}
	return do(resolve(c.cwd, args[0]))
}

func (c *CLI) cd(ctx context.Context, args []string) error {
	if len(args) > 1 {
		return errors.New("usage: cd [path]")
	}
	target := "/"
	if len(args) == 1 {
		target = resolve(c.cwd, args[0])
	}

	// cd itself needs no request, but entering a directory that does not
	// exist would only move the error to the next command, so check it.
	if _, err := c.superpeer.Ls(ctx, target); err != nil {
		if errors.Is(err, http_client.ErrNotFound) {
			return fmt.Errorf("%s: no such directory", target)
		}
		return err
	}
	c.cwd = target
	return nil
}

func (c *CLI) ls(ctx context.Context, args []string) error {
	if len(args) > 1 {
		return errors.New("usage: ls [path]")
	}
	target := c.cwd
	if len(args) == 1 {
		target = resolve(c.cwd, args[0])
	}

	entries, err := c.superpeer.Ls(ctx, target)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool {
		// Directories first, then by name.
		di, dj := isDirectory(entries[i]), isDirectory(entries[j])
		if di != dj {
			return di
		}
		return entries[i].Name < entries[j].Name
	})
	for _, e := range entries {
		if isDirectory(e) {
			c.printf("%s/\n", e.Name)
		} else {
			c.printf("%s\n", e.Name)
		}
	}
	return nil
}

// isDirectory interprets the entry type of an ls answer. The set of values
// is defined by the superpeer contract, so a few spellings are accepted.
func isDirectory(e model.DirEntry) bool {
	switch strings.ToLower(e.Type) {
	case "directory", "dir", "folder", "directorio":
		return true
	}
	return false
}

func (c *CLI) put(ctx context.Context, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: put <local> [remote]")
	}
	local := args[0]

	remote := resolve(c.cwd, filepath.Base(local))
	if len(args) == 2 {
		remote = resolve(c.cwd, args[1])
		// "put f.txt docs/" means "into docs, with the same name".
		if strings.HasSuffix(args[1], "/") {
			remote = path.Join(remote, filepath.Base(local))
		}
	}

	// Check the size before reading: reading a huge file just to refuse it
	// would defeat the purpose of the limit.
	info, err := os.Stat(local)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", local)
	}
	if info.Size() > maxUploadSize {
		return fmt.Errorf("%s is %d MiB; the limit is %d MiB", local, info.Size()>>20, maxUploadSize>>20)
	}
	data, err := os.ReadFile(local)
	if err != nil {
		return err
	}

	fileID, err := c.transfer.Put(ctx, remote, data)
	if err != nil {
		return err
	}
	c.printf("uploaded %s (%d bytes, id %s)\n", remote, len(data), fileID)
	return nil
}

func (c *CLI) get(ctx context.Context, args []string) error {
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: get <remote> [local]")
	}
	remote := resolve(c.cwd, args[0])
	local := path.Base(remote)
	if len(args) == 2 {
		local = args[1]
	}

	data, err := c.transfer.Get(ctx, remote)
	if err != nil {
		return err
	}

	// O_EXCL: refuse to overwrite an existing local file. The check and
	// the creation are one atomic step, so there is no window in which
	// another process could create the file in between.
	f, err := os.OpenFile(local, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%s already exists; choose another name: get %s <local>", local, args[0])
	}
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(local) // do not leave a half-written file behind
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	c.printf("downloaded %s -> %s (%d bytes)\n", remote, local, len(data))
	return nil
}

// editor opens file in the user's editor and returns when it is closed.
// It is a variable so tests can replace it.
var editor = func(file string) error {
	name := os.Getenv("EDITOR")
	if name == "" {
		name = "vi"
		if runtime.GOOS == "windows" {
			name = "notepad"
		}
	}
	parts := strings.Fields(name) // allows EDITOR="code --wait"
	cmd := exec.Command(parts[0], append(parts[1:], file)...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run()
}

// mdf edits a remote file: lock it, download it, open it in the editor and,
// only if its content changed, upload the new version. The lock is held
// (and renewed) the whole time.
//
// The superpeer refuses put/init on a path that already exists, so the new
// version is uploaded as delete + put (with a new file ID). If the upload
// fails after the delete, the edited copy stays in a temporary file whose
// path is printed, so the work is not lost.
func (c *CLI) mdf(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return errors.New("usage: mdf <remote>")
	}
	remote := resolve(c.cwd, args[0])

	release, err := c.holdLock(ctx, remote)
	if err != nil {
		return err
	}
	defer release()

	original, err := c.transfer.Get(ctx, remote)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp("", "dfsha-*"+path.Ext(remote))
	if err != nil {
		return err
	}
	tmp.Close() // closed first: on Windows the editor cannot open a file we hold
	if err := os.WriteFile(tmp.Name(), original, 0o600); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	if err := editor(tmp.Name()); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("editor: %w", err)
	}
	edited, err := os.ReadFile(tmp.Name())
	if err != nil {
		return err
	}

	switch {
	case bytes.Equal(edited, original):
		os.Remove(tmp.Name())
		c.printf("no changes, nothing uploaded\n")
		return nil
	case len(edited) == 0:
		os.Remove(tmp.Name())
		return errors.New("the edited file is empty; use rm to delete it")
	case len(edited) > maxUploadSize:
		return fmt.Errorf("the edited file is over the %d MiB limit; your copy is in %s", maxUploadSize>>20, tmp.Name())
	}

	if err := c.superpeer.DeleteFile(ctx, remote); err != nil {
		return fmt.Errorf("%w (your edited copy is in %s)", err, tmp.Name())
	}
	fileID, err := c.transfer.Put(ctx, remote, edited)
	if err != nil {
		return fmt.Errorf("%w (the old version was deleted; your edited copy is in %s)", err, tmp.Name())
	}
	os.Remove(tmp.Name())
	c.printf("modified %s (%d bytes, id %s)\n", remote, len(edited), fileID)
	return nil
}

// holdLock takes the write lock of a file and keeps it until release is
// called. The superpeer has no "renew": a lock lasts a short TTL and asking
// again while it is active is a conflict, so the lock is renewed by
// unlock + lock at half of its life.
func (c *CLI) holdLock(ctx context.Context, remote string) (release func(), err error) {
	l, err := c.superpeer.Lock(ctx, remote)
	if err != nil {
		return nil, err
	}

	stop := make(chan struct{})
	lastID := make(chan string)
	go func() {
		id, expires := l.LockID, l.ExpiresAt
		for {
			select {
			case <-stop:
				lastID <- id
				return
			case <-time.After(max(time.Until(expires)/2, time.Second)):
				// On failure keep the old lock and try again in a second.
				if c.superpeer.Unlock(ctx, remote, id) == nil {
					if n, err := c.superpeer.Lock(ctx, remote); err == nil {
						id, expires = n.LockID, n.ExpiresAt
					}
				}
			}
		}
	}()

	return func() {
		close(stop)
		// Fresh context: ctx may already be cancelled (Ctrl+C).
		bg, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c.superpeer.Unlock(bg, remote, <-lastID) // ErrNotFound = already expired: fine
	}, nil
}

func (c *CLI) login(ctx context.Context, args []string) error {
	if len(args) != 0 {
		return errors.New("usage: login")
	}
	if c.relogin == nil {
		return errors.New("login is not available in this session")
	}
	if err := c.relogin(ctx); err != nil {
		return err
	}
	c.printf("logged in again\n")
	return nil
}

// resolve turns what the user typed into an absolute, clean logical path:
// relative paths start at cwd, and "." and ".." are resolved here (".."
// stops at the root).
func resolve(cwd, arg string) string {
	if !path.IsAbs(arg) {
		arg = path.Join(cwd, arg)
	}
	return path.Clean(arg)
}

// friendly turns the typed errors of the client package into messages a
// user can act on; anything else is shown as is.
func friendly(err error) string {
	switch {
	case errors.Is(err, client.ErrNotLoggedIn), errors.Is(err, http_client.ErrUnauthorized):
		return "session expired or not logged in; type login"
	case errors.Is(err, http_client.ErrSuperpeerUnavailable):
		return "superpeer unreachable; try again in a few seconds (after about 30 s the peer switches to the backup superpeer)"
	case errors.Is(err, http_client.ErrNotFound):
		return "not found (" + err.Error() + ")"
	case errors.Is(err, http_client.ErrConflict):
		return "conflict: it already exists or is locked by someone else (" + err.Error() + ")"
	case errors.Is(err, http_client.ErrForbidden):
		return "not allowed (" + err.Error() + ")"
	}
	return err.Error()
}

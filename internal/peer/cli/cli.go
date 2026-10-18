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
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"dfsha/internal/peer/client"
	"dfsha/internal/peer/http_client"
	"dfsha/internal/shared/model"
)

// maxUploadSize bounds the files `put` accepts. Upload keeps the whole
// file and its encrypted copy in memory, so this protects the peer from
// being asked for something that cannot fit.
const maxUploadSize = 512 << 20

// Superpeer is the namespace part of the superpeer client.
// client.SuperpeerClient satisfies it.
type Superpeer interface {
	Mkdir(ctx context.Context, logicalPath string) error
	Rmdir(ctx context.Context, logicalPath string) error
	Ls(ctx context.Context, logicalPath string) ([]model.DirEntry, error)
	DeleteFile(ctx context.Context, logicalPath string) error
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
  put <local> [remote]       upload a local file
  get <remote> [local]       download a file (never overwrites)
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

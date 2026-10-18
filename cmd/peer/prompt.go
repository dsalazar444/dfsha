package peer

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"golang.org/x/term"
)

// readLine prints a prompt and reads one line of input.
func (a *app) readLine(prompt string) (string, error) {
	fmt.Fprint(a.out, prompt)
	line, err := a.in.ReadString('\n')
	if err != nil && !(errors.Is(err, io.EOF) && line != "") {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// readPassword reads a password. On a terminal it does not echo the typed
// characters. When input is not a terminal (a script, a pipe) it reads a
// plain line, which is what makes the peer testable and scriptable; there
// is nothing to hide in that case.
func (a *app) readPassword(prompt string) ([]byte, error) {
	if !a.terminal {
		line, err := a.readLine(prompt)
		return []byte(line), err
	}

	fmt.Fprint(a.out, prompt)
	password, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Fprintln(a.out) // the Enter key was not echoed either
	return password, err
}

// wipe overwrites a secret in memory once it is no longer needed.
func wipe(secret []byte) {
	for i := range secret {
		secret[i] = 0
	}
}

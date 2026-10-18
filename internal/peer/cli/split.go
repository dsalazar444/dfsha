package cli

import (
	"errors"
	"strings"
)

// splitArgs splits a command line into words. Whitespace separates words,
// except inside single or double quotes, so paths with spaces work:
//
//	put "my report.pdf" 'docs/final version.pdf'
//
// There are no escape sequences or expansions: it is not a shell.
func splitArgs(line string) ([]string, error) {
	var (
		args    []string
		current strings.Builder
		quote   rune // 0 when outside quotes, otherwise the open quote
		started bool // distinguishes an empty quoted word ("") from no word
	)

	for _, r := range line {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				current.WriteRune(r)
			}
		case r == '"' || r == '\'':
			quote = r
			started = true
		case r == ' ' || r == '\t':
			if started {
				args = append(args, current.String())
				current.Reset()
				started = false
			}
		default:
			current.WriteRune(r)
			started = true
		}
	}
	if quote != 0 {
		return nil, errors.New("unclosed quote")
	}
	if started {
		args = append(args, current.String())
	}
	return args, nil
}

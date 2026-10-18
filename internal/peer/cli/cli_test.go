package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"dfsha/internal/peer/client"
	"dfsha/internal/peer/http_client"
	"dfsha/internal/shared/model"
)

// Compile-time checks: the real implementations satisfy the interfaces.
var _ Superpeer = (*client.SuperpeerClient)(nil)

// fakeSuperpeer records the namespace calls and serves a fixed tree.
type fakeSuperpeer struct {
	calls []string
	tree  map[string][]model.DirEntry // dir -> entries; a missing dir is "not found"
	err   error                       // returned by every call if set
}

func (f *fakeSuperpeer) Mkdir(_ context.Context, p string) error {
	f.calls = append(f.calls, "mkdir "+p)
	return f.err
}
func (f *fakeSuperpeer) Rmdir(_ context.Context, p string) error {
	f.calls = append(f.calls, "rmdir "+p)
	return f.err
}
func (f *fakeSuperpeer) DeleteFile(_ context.Context, p string) error {
	f.calls = append(f.calls, "rm "+p)
	return f.err
}
func (f *fakeSuperpeer) Ls(_ context.Context, p string) ([]model.DirEntry, error) {
	f.calls = append(f.calls, "ls "+p)
	if f.err != nil {
		return nil, f.err
	}
	entries, ok := f.tree[p]
	if !ok {
		return nil, &http_client.StatusError{Code: 404, Message: "no such directory"}
	}
	return entries, nil
}

// fakeTransfer keeps uploaded files in memory.
type fakeTransfer struct {
	files map[string][]byte
	calls []string
	err   error
}

func (f *fakeTransfer) Put(_ context.Context, p string, data []byte) (string, error) {
	f.calls = append(f.calls, "put "+p)
	if f.err != nil {
		return "", f.err
	}
	if f.files == nil {
		f.files = map[string][]byte{}
	}
	f.files[p] = data
	return "file-id-1", nil
}
func (f *fakeTransfer) Get(_ context.Context, p string) ([]byte, error) {
	f.calls = append(f.calls, "get "+p)
	if f.err != nil {
		return nil, f.err
	}
	data, ok := f.files[p]
	if !ok {
		return nil, &http_client.StatusError{Code: 404, Message: "no such file"}
	}
	return data, nil
}

// run feeds script to a fresh CLI and returns everything it printed.
func run(t *testing.T, sp *fakeSuperpeer, tr *fakeTransfer, relogin func(context.Context) error, script string) string {
	t.Helper()
	var out bytes.Buffer
	c := New(sp, tr, relogin)
	if err := c.Run(context.Background(), strings.NewReader(script), &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out.String()
}

func tree() map[string][]model.DirEntry {
	return map[string][]model.DirEntry{
		"/":           {{Name: "docs", Type: "directory"}, {Name: "b.txt", Type: "file"}, {Name: "a.txt", Type: "file"}, {Name: "zeta", Type: "directory"}},
		"/docs":       {{Name: "tesis", Type: "directory"}},
		"/docs/tesis": nil,
	}
}

func TestSplitArgs(t *testing.T) {
	cases := []struct {
		in   string
		want []string
		bad  bool
	}{
		{"", nil, false},
		{"   ", nil, false},
		{"ls", []string{"ls"}, false},
		{"  put   a.txt   b.txt ", []string{"put", "a.txt", "b.txt"}, false},
		{`put "my file.pdf" 'x y/z w'`, []string{"put", "my file.pdf", "x y/z w"}, false},
		{`mkdir ""`, []string{"mkdir", ""}, false},
		{`a"b c"d`, []string{"ab cd"}, false},
		{`put "unclosed`, nil, true},
	}
	for _, tc := range cases {
		got, err := splitArgs(tc.in)
		if (err != nil) != tc.bad {
			t.Errorf("%q: err = %v", tc.in, err)
			continue
		}
		if !tc.bad && !reflect.DeepEqual(got, tc.want) && !(len(got) == 0 && len(tc.want) == 0) {
			t.Errorf("%q: got %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestResolve(t *testing.T) {
	cases := []struct{ cwd, arg, want string }{
		{"/", "docs", "/docs"},
		{"/docs", "tesis", "/docs/tesis"},
		{"/docs", "/abs/x", "/abs/x"},
		{"/docs/tesis", "..", "/docs"},
		{"/docs", "../..", "/"},
		{"/", "../../etc", "/etc"}, // cannot climb above the root
		{"/docs", ".", "/docs"},
		{"/docs", "a//b/./c", "/docs/a/b/c"},
	}
	for _, tc := range cases {
		if got := resolve(tc.cwd, tc.arg); got != tc.want {
			t.Errorf("resolve(%q, %q) = %q, want %q", tc.cwd, tc.arg, got, tc.want)
		}
	}
}

func TestNavigation(t *testing.T) {
	sp := &fakeSuperpeer{tree: tree()}
	out := run(t, sp, &fakeTransfer{}, nil, "pwd\ncd docs\npwd\ncd tesis\npwd\ncd ../..\npwd\ncd docs\ncd\npwd\nexit\n")

	for _, want := range []string{"dfsha:/docs> ", "dfsha:/docs/tesis> "} {
		if !strings.Contains(out, want) {
			t.Errorf("prompt %q never shown:\n%s", want, out)
		}
	}
	// The script ends with `cd docs`, `cd` (no argument), `pwd`: the last
	// pwd must print the root.
	if !strings.HasSuffix(out, "dfsha:/docs> dfsha:/> /\ndfsha:/> ") {
		t.Errorf("cd without argument did not return to /:\n%s", out)
	}
}

func TestCdToMissingDirectoryStaysPut(t *testing.T) {
	sp := &fakeSuperpeer{tree: tree()}
	out := run(t, sp, &fakeTransfer{}, nil, "cd nope\npwd\n")

	if !strings.Contains(out, "/nope: no such directory") {
		t.Errorf("no friendly error:\n%s", out)
	}
	if strings.Contains(out, "dfsha:/nope>") {
		t.Error("cd moved into a directory that does not exist")
	}
}

func TestLsOrdersDirectoriesFirst(t *testing.T) {
	sp := &fakeSuperpeer{tree: tree()}
	out := run(t, sp, &fakeTransfer{}, nil, "ls\n")

	body := strings.TrimPrefix(out, "dfsha:/> ")
	body = body[:strings.Index(body, "dfsha:")]
	if body != "docs/\nzeta/\na.txt\nb.txt\n" {
		t.Errorf("ls output = %q", body)
	}
}

func TestNamespaceCommandsSendAbsolutePaths(t *testing.T) {
	sp := &fakeSuperpeer{tree: tree()}
	run(t, sp, &fakeTransfer{}, nil, "cd docs\nmkdir new\nmkdir ../other\nrmdir tesis\nrm tesis/a.pdf\nls tesis\nmkdir \"with space\"\n")

	want := []string{
		"ls /docs", // from cd
		"mkdir /docs/new", "mkdir /other", "rmdir /docs/tesis", "rm /docs/tesis/a.pdf", "ls /docs/tesis",
		"mkdir /docs/with space",
	}
	if !reflect.DeepEqual(sp.calls, want) {
		t.Errorf("calls:\n got %q\nwant %q", sp.calls, want)
	}
}

func TestWrongArgumentCountsPrintUsage(t *testing.T) {
	sp := &fakeSuperpeer{tree: tree()}
	tr := &fakeTransfer{}
	out := run(t, sp, tr, nil, "mkdir\nmkdir a b\nrm\nput\nget\nget a b c\ncd a b\nls a b\nlogin x\n")

	for _, want := range []string{"usage: mkdir <path>", "usage: rm <path>", "usage: put <local> [remote]", "usage: get <remote> [local]", "usage: cd [path]", "usage: ls [path]"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if len(sp.calls) != 0 || len(tr.calls) != 0 {
		t.Errorf("a malformed command reached the backend: %v %v", sp.calls, tr.calls)
	}
}

func TestPutAndGetRoundTripThroughRealFiles(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "report.txt")
	os.WriteFile(local, []byte("hello distributed world"), 0o600)

	sp := &fakeSuperpeer{tree: tree()}
	tr := &fakeTransfer{}

	// put with the default remote name (current logical dir + base name).
	out := run(t, sp, tr, nil, fmt.Sprintf("cd docs\nput %q\n", local))
	if !strings.Contains(out, "uploaded /docs/report.txt (23 bytes, id file-id-1)") {
		t.Errorf("put output:\n%s", out)
	}
	if string(tr.files["/docs/report.txt"]) != "hello distributed world" {
		t.Error("uploaded content differs")
	}

	// get into an explicit local path.
	dest := filepath.Join(dir, "copy.txt")
	out = run(t, sp, tr, nil, fmt.Sprintf("get /docs/report.txt %q\n", dest))
	if !strings.Contains(out, "downloaded /docs/report.txt") {
		t.Errorf("get output:\n%s", out)
	}
	got, _ := os.ReadFile(dest)
	if string(got) != "hello distributed world" {
		t.Errorf("downloaded content = %q", got)
	}
	if info, _ := os.Stat(dest); info.Mode().Perm() != 0o600 {
		t.Errorf("downloaded file permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestPutRemoteNaming(t *testing.T) {
	dir := t.TempDir()
	local := filepath.Join(dir, "f.bin")
	os.WriteFile(local, []byte("x"), 0o600)

	cases := map[string]string{
		fmt.Sprintf("put %q\n", local):                "/f.bin",
		fmt.Sprintf("put %q renamed.bin\n", local):    "/renamed.bin",
		fmt.Sprintf("put %q docs/\n", local):          "/docs/f.bin",
		fmt.Sprintf("put %q /abs/dir/other\n", local): "/abs/dir/other",
	}
	for script, wantRemote := range cases {
		tr := &fakeTransfer{}
		run(t, &fakeSuperpeer{tree: tree()}, tr, nil, script)
		if len(tr.calls) != 1 || tr.calls[0] != "put "+wantRemote {
			t.Errorf("%q: calls = %v, want put %s", script, tr.calls, wantRemote)
		}
	}
}

func TestGetNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "keep.txt")
	os.WriteFile(existing, []byte("PRECIOUS"), 0o600)

	tr := &fakeTransfer{files: map[string][]byte{"/keep.txt": []byte("remote")}}
	out := run(t, &fakeSuperpeer{}, tr, nil, fmt.Sprintf("get /keep.txt %q\n", existing))

	if !strings.Contains(out, "already exists") {
		t.Errorf("no refusal:\n%s", out)
	}
	if got, _ := os.ReadFile(existing); string(got) != "PRECIOUS" {
		t.Errorf("existing file was overwritten: %q", got)
	}
}

func TestPutRejectsBadLocalFiles(t *testing.T) {
	dir := t.TempDir()
	tr := &fakeTransfer{}
	out := run(t, &fakeSuperpeer{}, tr, nil,
		fmt.Sprintf("put %q\nput %q\n", filepath.Join(dir, "missing.txt"), dir))

	if len(tr.calls) != 0 {
		t.Errorf("transfer was called: %v", tr.calls)
	}
	if !strings.Contains(out, "no such file") || !strings.Contains(out, "is not a regular file") {
		t.Errorf("output:\n%s", out)
	}
}

func TestErrorsAreFriendlyAndTheSessionContinues(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"expired":     {&http_client.StatusError{Code: 401, Message: "token expired"}, "type login"},
		"not logged":  {client.ErrNotLoggedIn, "type login"},
		"down":        {fmt.Errorf("%w: %w", http_client.ErrSuperpeerUnavailable, http_client.ErrUnreachable), "superpeer unreachable"},
		"conflict":    {&http_client.StatusError{Code: 409, Message: "locked"}, "conflict"},
		"not found":   {&http_client.StatusError{Code: 404, Message: "nothing"}, "not found"},
		"forbidden":   {&http_client.StatusError{Code: 403, Message: "no"}, "not allowed"},
		"other error": {errors.New("disk on fire"), "disk on fire"},
	}
	for name, tc := range cases {
		sp := &fakeSuperpeer{err: tc.err}
		out := run(t, sp, &fakeTransfer{}, nil, "mkdir x\npwd\n")
		if !strings.Contains(out, "error: ") || !strings.Contains(out, tc.want) {
			t.Errorf("%s: output %q does not contain %q", name, out, tc.want)
		}
		if strings.Count(out, "dfsha:/> ") != 3 { // initial prompt + after each of the 2 commands
			t.Errorf("%s: the session did not continue after the error:\n%s", name, out)
		}
	}
}

func TestLogin(t *testing.T) {
	called := 0
	out := run(t, &fakeSuperpeer{}, &fakeTransfer{}, func(context.Context) error { called++; return nil }, "login\n")
	if called != 1 || !strings.Contains(out, "logged in again") {
		t.Errorf("called=%d out=%s", called, out)
	}

	out = run(t, &fakeSuperpeer{}, &fakeTransfer{}, func(context.Context) error { return errors.New("wrong password") }, "login\n")
	if !strings.Contains(out, "wrong password") {
		t.Errorf("relogin error not shown:\n%s", out)
	}

	out = run(t, &fakeSuperpeer{}, &fakeTransfer{}, nil, "login\n")
	if !strings.Contains(out, "not available") {
		t.Errorf("login without relogin function:\n%s", out)
	}
}

func TestUnknownCommandHelpAndBlankLines(t *testing.T) {
	out := run(t, &fakeSuperpeer{}, &fakeTransfer{}, nil, "\n   \nfrobnicate\nhelp\n")
	if !strings.Contains(out, `unknown command "frobnicate"`) || !strings.Contains(out, "commands:") {
		t.Errorf("output:\n%s", out)
	}
	if out := run(t, &fakeSuperpeer{}, &fakeTransfer{}, nil, `mkdir "oops`+"\n"); !strings.Contains(out, "unclosed quote") {
		t.Errorf("unclosed quote not reported:\n%s", out)
	}
}

func TestExitStopsReadingCommands(t *testing.T) {
	sp := &fakeSuperpeer{}
	run(t, sp, &fakeTransfer{}, nil, "mkdir a\nexit\nmkdir b\n")
	if !reflect.DeepEqual(sp.calls, []string{"mkdir /a"}) {
		t.Errorf("commands after exit were executed: %v", sp.calls)
	}
}

func TestRunStopsWhenContextIsCancelled(t *testing.T) {
	pr, pw := io.Pipe() // an input that never ends and never delivers anything
	defer pw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(&fakeSuperpeer{}, &fakeTransfer{}, nil).Run(ctx, pr, io.Discard) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after cancel while waiting for input")
	}
}

func TestPutRefusesHugeFiles(t *testing.T) {
	dir := t.TempDir()
	big := filepath.Join(dir, "big.bin")
	f, _ := os.Create(big)
	f.Truncate(maxUploadSize + 1) // sparse file: no real disk or RAM used
	f.Close()

	tr := &fakeTransfer{}
	out := run(t, &fakeSuperpeer{}, tr, nil, fmt.Sprintf("put %q\n", big))
	if len(tr.calls) != 0 || !strings.Contains(out, "the limit is") {
		t.Errorf("huge file not refused:\n%s", out)
	}
}

// chunkedReader returns one line per Read call and counts the calls.
type chunkedReader struct {
	lines []string
	reads int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.reads >= len(r.lines) {
		return 0, io.EOF
	}
	n := copy(p, r.lines[r.reads])
	r.reads++
	return n, nil
}

// While a command runs (for example "login", which asks for a password on
// the same terminal) nothing may be reading the input ahead.
func TestInputIsNotReadAheadWhileACommandRuns(t *testing.T) {
	in := &chunkedReader{lines: []string{"login\n", "mkdir never-run\n"}}
	readsDuringLogin := -1

	c := New(&fakeSuperpeer{}, &fakeTransfer{}, func(context.Context) error {
		time.Sleep(30 * time.Millisecond) // give a read-ahead goroutine time to misbehave
		readsDuringLogin = in.reads
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.Run(ctx, in, io.Discard)

	if readsDuringLogin != 1 {
		t.Errorf("input was read %d times while login ran, want 1 (no read-ahead)", readsDuringLogin)
	}
}

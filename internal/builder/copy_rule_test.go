package builder

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
)

// TestCopyRule covers the COPY (src,dst)+context-entries → flatten extract
// rule mapping for the single-source forms the e2b SDK emits (arcnames rooted
// at the build context).
func TestCopyRule(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		dst     string
		workdir string
		entries []string
		want    string
		wantErr bool
	}{
		{name: "whole-context-to-dir", src: ".", dst: "/app",
			entries: []string{"main.py", "lib/x.py"}, want: ":/app/"},
		{name: "glob-whole", src: "*", dst: "/app",
			entries: []string{"a.txt", "b.txt"}, want: ":/app/"},
		{name: "subdir", src: "app", dst: "/srv",
			entries: []string{"app/main.py", "app/lib/x.py"}, want: "app/:/srv/"},
		{name: "subdir-trailing", src: "app/", dst: "/srv",
			entries: []string{"app/main.py"}, want: "app/:/srv/"},
		{name: "single-file-to-file", src: "main.py", dst: "/app/main.py",
			entries: []string{"main.py"}, want: "main.py:/app/main.py"},
		{name: "single-file-to-dir", src: "main.py", dst: "/app/",
			entries: []string{"main.py"}, want: "main.py:/app/main.py"},
		{name: "nested-file", src: "pkg/v.txt", dst: "/etc/v.txt",
			entries: []string{"pkg/v.txt"}, want: "pkg/v.txt:/etc/v.txt"},
		{name: "relative-dst-against-workdir", src: ".", dst: "src", workdir: "/home/user",
			entries: []string{"a"}, want: ":/home/user/src/"},
		{name: "relative-dst-no-workdir", src: ".", dst: "x",
			entries: []string{"a"}, want: ":/x/"},
		{name: "missing-source", src: "nope", dst: "/app",
			entries: []string{"main.py"}, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := copyRule(c.src, c.dst, c.workdir, c.entries)
			if c.wantErr {
				if err == nil {
					t.Fatalf("copyRule(%q,%q) = %q, want error", c.src, c.dst, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("copyRule(%q,%q): %v", c.src, c.dst, err)
			}
			if got != c.want {
				t.Errorf("copyRule(%q,%q,wd=%q) = %q, want %q", c.src, c.dst, c.workdir, got, c.want)
			}
		})
	}
}

func TestNormalizeCopySrc(t *testing.T) {
	cases := map[string]string{
		".": "", "./": "", "*": "", "*.py": "", "app": "app", "./app": "app",
		"app/": "app", "/app/": "app", "app/*.py": "app", "pkg/v.txt": "pkg/v.txt",
	}
	for in, want := range cases {
		if got := normalizeCopySrc(in); got != want {
			t.Errorf("normalizeCopySrc(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestApplyCopyPreservesGuestStderr(t *testing.T) {
	var contextArchive bytes.Buffer
	gz := gzip.NewWriter(&contextArchive)
	tw := tar.NewWriter(gz)
	body := []byte("hello\n")
	if err := tw.WriteHeader(&tar.Header{Name: "hello.txt", Mode: 0o644, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(contextArchive.Bytes())
	}))
	defer server.Close()

	workdir := t.TempDir()
	sandboxCtl := filepath.Join(workdir, "sandbox-ctl")
	const diagnostic = "tar: chown /opt/ct2/hello.txt -> 1000:1000: operation not permitted"
	script := `#!/bin/sh
while [ "$#" -gt 0 ]; do
    if [ "$1" = "--stderr-to" ]; then
        shift
        printf '%s\n' '` + diagnostic + `' > "$1"
        break
    fi
    shift
done
exit 1
`
	if err := os.WriteFile(sandboxCtl, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	p := &buildPipeline{
		ctx: context.Background(),
		spec: &configsock.BuildSpec{
			Workdir:  workdir,
			Paths:    configsock.BuildPaths{SandboxCtl: sandboxCtl},
			Timeouts: configsock.BuildTimeouts{PullSec: 5, StepSec: 5},
		},
	}
	sb := &phaseSandbox{p: p, sid: "copy-test", runRoot: workdir}
	err := p.applyCopy(sb, &stepCtx{}, 1, configsock.BuildStep{
		Args:      []string{"hello.txt", "/opt/ct2/", "1000:1000"},
		FilesHash: "context",
		FilesURL:  server.URL,
	}, func(v string) string { return v })
	if err == nil || !strings.Contains(err.Error(), "guest stderr: "+diagnostic) {
		t.Fatalf("applyCopy error = %v", err)
	}
}

package builder

import "testing"

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

package configresolve

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateComponentExecutableMetadata(t *testing.T) {
	dir := t.TempDir()
	nodeCtl := filepath.Join(dir, "node-ctl")
	custom := filepath.Join(dir, "xconductor")
	for _, path := range []string{nodeCtl, custom} {
		writeFileWithMode(t, path, []byte("binary"), 0o755)
	}
	if err := ValidateComponentExecutableMetadata(custom, nodeCtl); err != nil {
		t.Fatalf("valid executable: %v", err)
	}

	tests := []struct {
		name string
		path string
		prep func() string
		want string
	}{
		{name: "relative", path: "xconductor", want: "absolute"},
		{name: "missing", path: filepath.Join(dir, "missing"), want: "open component"},
		{name: "directory", path: dir, want: "regular file"},
		{name: "not executable", prep: func() string {
			path := filepath.Join(dir, "plain")
			writeFileWithMode(t, path, nil, 0o644)
			return path
		}, want: "not executable"},
		{name: "writable", prep: func() string {
			path := filepath.Join(dir, "writable")
			writeFileWithMode(t, path, nil, 0o775)
			return path
		}, want: "group/world writable"},
		{name: "same file", prep: func() string {
			path := filepath.Join(dir, "node-ctl-link")
			if err := os.Link(nodeCtl, path); err != nil {
				t.Fatal(err)
			}
			return path
		}, want: "must not be the node-ctl"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := test.path
			if test.prep != nil {
				path = test.prep()
			}
			err := ValidateComponentExecutableMetadata(path, nodeCtl)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestOpenComponentExecutableKeepsValidatedIdentityAcrossReplacement(t *testing.T) {
	dir := t.TempDir()
	nodeCtl := filepath.Join(dir, "node-ctl")
	component := filepath.Join(dir, "xconductor")
	replacement := filepath.Join(dir, "replacement")
	for path, contents := range map[string]string{
		nodeCtl: "node", component: "validated", replacement: "replacement",
	} {
		writeFileWithMode(t, path, []byte(contents), 0o500)
	}
	opened, err := OpenComponentExecutable(component, nodeCtl)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	openedInfo, err := opened.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, component); err != nil {
		t.Fatal(err)
	}
	pathInfo, err := os.Stat(component)
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(openedInfo, pathInfo) {
		t.Fatal("opened component followed a pathname replacement")
	}
}

func TestValidateComponentOwnerPolicy(t *testing.T) {
	for _, test := range []struct {
		name       string
		owner      uint32
		euid       uint32
		wantAccept bool
	}{
		{name: "root service root owned", owner: 0, euid: 0, wantAccept: true},
		{name: "root service non-root owned", owner: 1000, euid: 0},
		{name: "non-root service root owned", owner: 0, euid: 1000, wantAccept: true},
		{name: "non-root service same uid owned", owner: 1000, euid: 1000, wantAccept: true},
		{name: "non-root service other uid owned", owner: 1001, euid: 1000},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateComponentOwner(test.owner, test.euid)
			if got := err == nil; got != test.wantAccept {
				t.Fatalf("validateComponentOwner(%d, %d) error = %v, accept=%t", test.owner, test.euid, err, got)
			}
		})
	}
}

func TestOpenComponentExecutableAppliesRuntimeModeAndIdentityChecks(t *testing.T) {
	dir := t.TempDir()
	nodeCtl := filepath.Join(dir, "node-ctl")
	valid := filepath.Join(dir, "valid")
	writable := filepath.Join(dir, "writable")
	for path, mode := range map[string]os.FileMode{nodeCtl: 0o500, valid: 0o500, writable: 0o522} {
		writeFileWithMode(t, path, []byte("binary"), mode)
	}
	opened, err := OpenComponentExecutable(valid, nodeCtl)
	if err != nil {
		t.Fatalf("current-user runtime executable: %v", err)
	}
	_ = opened.Close()
	for name, path := range map[string]string{"writable": writable, "same file": nodeCtl} {
		t.Run(name, func(t *testing.T) {
			opened, err := OpenComponentExecutable(path, nodeCtl)
			if opened != nil {
				_ = opened.Close()
			}
			if err == nil {
				t.Fatal("runtime executable validation succeeded")
			}
		})
	}
}

func TestExecutablesUseExactNodeCtlAndAdjacentHelperFallback(t *testing.T) {
	dir := t.TempDir()
	nodeCtl := filepath.Join(dir, "node-ctl")
	executables := ExecutablesForNodeCtl(nodeCtl)
	if got := executables.OrchestratorCtl(); got != nodeCtl {
		t.Fatalf("node-ctl = %q, want exact %q", got, nodeCtl)
	}
	if got := executables.SandboxCtl(); got != "sandbox-ctl" {
		t.Fatalf("missing adjacent helper = %q, want PATH fallback", got)
	}
	adjacent := filepath.Join(dir, "sandbox-ctl")
	writeFileWithMode(t, adjacent, nil, 0o755)
	if got := executables.SandboxCtl(); got != adjacent {
		t.Fatalf("adjacent helper = %q, want %q", got, adjacent)
	}
	absolute := filepath.Join(t.TempDir(), "explicit-helper")
	if got := executables.binary(absolute); got != absolute {
		t.Fatalf("absolute helper = %q, want %q", got, absolute)
	}
}

func writeFileWithMode(t *testing.T, path string, contents []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

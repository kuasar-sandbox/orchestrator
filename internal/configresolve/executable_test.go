package configresolve

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateComponentExecutable(t *testing.T) {
	dir := t.TempDir()
	nodeCtl := filepath.Join(dir, "node-ctl")
	custom := filepath.Join(dir, "xconductor")
	for _, path := range []string{nodeCtl, custom} {
		if err := os.WriteFile(path, []byte("binary"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidateComponentExecutable(custom, nodeCtl); err != nil {
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
			if err := os.WriteFile(path, nil, 0o644); err != nil {
				t.Fatal(err)
			}
			return path
		}, want: "not executable"},
		{name: "writable", prep: func() string {
			path := filepath.Join(dir, "writable")
			if err := os.WriteFile(path, nil, 0o775); err != nil {
				t.Fatal(err)
			}
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
			err := ValidateComponentExecutable(path, nodeCtl)
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
		if err := os.WriteFile(path, []byte(contents), 0o500); err != nil {
			t.Fatal(err)
		}
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

func TestComponentOwnerMustMatchEffectiveUser(t *testing.T) {
	if sameComponentOwner(uint32(os.Geteuid())) != true {
		t.Fatal("effective user did not trust its own component")
	}
	if sameComponentOwner(uint32(os.Geteuid()+1)) != false {
		t.Fatal("component owned by another user was trusted")
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
	if err := os.WriteFile(adjacent, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := executables.SandboxCtl(); got != adjacent {
		t.Fatalf("adjacent helper = %q, want %q", got, adjacent)
	}
	absolute := filepath.Join(t.TempDir(), "explicit-helper")
	if got := executables.binary(absolute); got != absolute {
		t.Fatalf("absolute helper = %q, want %q", got, absolute)
	}
}

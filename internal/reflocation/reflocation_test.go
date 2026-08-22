package reflocation

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

func TestResolve(t *testing.T) {
	name := "tenant-source"
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	wantPath := "/mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + name
	got, err := Resolve("file:///mnt/shared/snapshots", name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Path != wantPath || got.URI != "file://"+wantPath {
		t.Fatalf("Resolve() = %+v, want path=%q uri=%q", got, wantPath, "file://"+wantPath)
	}
}

func TestResolveRejectsInvalidInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		parent string
		loc    string
	}{
		{name: "missing parent", loc: "valid"},
		{name: "relative parent", parent: "file://relative", loc: "valid"},
		{name: "remote parent", parent: "https://example.test/path", loc: "valid"},
		{name: "path separator", parent: "file:///tmp", loc: "a/b"},
		{name: "dot", parent: "file:///tmp", loc: "."},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Resolve(test.parent, test.loc); err == nil {
				t.Fatal("invalid ref location input was accepted")
			}
		})
	}
}

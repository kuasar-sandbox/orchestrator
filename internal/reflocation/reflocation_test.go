package reflocation

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
)

func TestPublicationNameAndResolve(t *testing.T) {
	entity := "0198f7a1-1234-7234-9abc-0123456789ab"
	name := PublicationName(entity)
	if name != entity {
		t.Fatalf("PublicationName() = %q, want %q", name, entity)
	}
	got, err := Resolve("file:///mnt/shared/snapshots", name)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	wantPath := "/mnt/shared/snapshots/" + digest[:2] + "/" + digest[2:4] + "/" + name
	if got.Path != wantPath || got.URI != "file://"+wantPath {
		t.Fatalf("Resolve() = %+v, want path=%q", got, wantPath)
	}
	// A bare entity id must never produce a date-shaped path segment; guard
	// against the dated layout creeping back in.
	for _, seg := range strings.Split(strings.Trim(got.Path, "/"), "/") {
		if len(seg) == 8 && strings.TrimLeft(seg, "0123456789") == "" {
			t.Fatalf("path %q contains a date-shaped segment %q", got.Path, seg)
		}
	}
}

// TestSameEntityConvergesToSameLocation pins the retry contract: the name is a
// pure function of the entity id, so any retry — minutes or days apart, before
// or after any process restart — necessarily derives the identical publication
// name and therefore the identical directory. No persisted state is consulted
// or needed.
func TestSameEntityConvergesToSameLocation(t *testing.T) {
	entity := "0198f7a1-1234-7234-9abc-0123456789ab"
	parent := "file:///mnt/shared/snapshots"

	name1 := PublicationName(entity)
	name2 := PublicationName(entity)
	if name1 != name2 {
		t.Fatalf("retry changed the publication name: %q vs %q", name1, name2)
	}
	loc1, err := Resolve(parent, name1)
	if err != nil {
		t.Fatal(err)
	}
	loc2, err := Resolve(parent, name2)
	if err != nil {
		t.Fatal(err)
	}
	if loc1 != loc2 {
		t.Fatalf("retry resolved different locations: %+v vs %+v", loc1, loc2)
	}
}

func TestResolveRejectsInvalidInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		parent string
		loc    string
	}{
		{name: "missing parent", loc: "0198f7a1-1234-7234-9abc-0123456789ab"},
		{name: "relative parent", parent: "file://relative", loc: "0198f7a1-1234-7234-9abc-0123456789ab"},
		{name: "remote parent", parent: "https://example.test/path", loc: "0198f7a1-1234-7234-9abc-0123456789ab"},
		{name: "empty name", parent: "file:///tmp", loc: ""},
		{name: "path separator", parent: "file:///tmp", loc: "a/b"},
		{name: "dot", parent: "file:///tmp", loc: "."},
		{name: "dot-prefixed", parent: "file:///tmp", loc: ".entity"},
		{name: "leading hyphen", parent: "file:///tmp", loc: "-entity"},
		{name: "space", parent: "file:///tmp", loc: "enti ty"},
		{name: "non-ascii", parent: "file:///tmp", loc: "实体"},
		{name: "date-shaped name is a valid entity id", parent: "file:///tmp", loc: "12345678"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Resolve(test.parent, test.loc)
			if test.name == "date-shaped name is a valid entity id" {
				// A bare 8-digit id is a legitimate opaque entity id under the
				// undated layout; only the dated layout could reject it.
				if err != nil {
					t.Fatalf("Resolve() rejected a valid bare id: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid ref location input was accepted")
			}
		})
	}
}

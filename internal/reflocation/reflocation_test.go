package reflocation

import (
	"crypto/sha256"
	"fmt"
	"testing"
	"time"
)

func TestPublicationNameAndResolve(t *testing.T) {
	entity := "0198f7a1-1234-7234-9abc-0123456789ab"
	published := time.Date(2026, 8, 24, 23, 59, 0, 0, time.UTC)
	name := PublicationName(entity, published)
	if name != entity+"-20260824" {
		t.Fatalf("PublicationName() = %q, want %q", name, entity+"-20260824")
	}
	got, err := Resolve("file:///mnt/shared/snapshots", name)
	if err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(name)))
	wantPath := "/mnt/shared/snapshots/20260824/" + digest[:2] + "/" + digest[2:4] + "/" + name
	if got.Path != wantPath || got.URI != "file://"+wantPath {
		t.Fatalf("Resolve() = %+v, want path=%q", got, wantPath)
	}
}

func TestResolveUsesPublicationDateNotEntityDate(t *testing.T) {
	// An entity with a 2025-era v7 id exported today must land in today's
	// bucket: a GC deleting old date buckets can never remove it.
	entity := "0194a1b2-c3d4-7234-9abc-0123456789ab" // 2025-era timestamp
	name := PublicationName(entity, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	got, err := Resolve("file:///mnt/shared/snapshots", name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Path != "/mnt/shared/snapshots/20260824/"+got.Path[len("/mnt/shared/snapshots/20260824/"):] {
		t.Fatal("unreachable")
	}
	if got.Path[:len("/mnt/shared/snapshots/20260824/")] != "/mnt/shared/snapshots/20260824/" {
		t.Fatalf("path %q is not bucketed by publication date", got.Path)
	}
}

func TestResolveBucketLabelsSortChronologically(t *testing.T) {
	entity := "0198f7a1-1234-7234-9abc-0123456789ab"
	older := PublicationName(entity, time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC))
	newer := PublicationName(entity, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if older[len(older)-8:] >= newer[len(newer)-8:] {
		t.Fatalf("date suffixes must sort chronologically: %q vs %q", older, newer)
	}
}

func TestResolveAcceptsLeapDay(t *testing.T) {
	// Feb 29 is a real calendar date on leap years — and only then.
	for _, test := range []struct {
		name string
		loc  string
		ok   bool
	}{
		{loc: "0198f7a1-1234-7234-9abc-0123456789ab-20240229", ok: true},
		{loc: "0198f7a1-1234-7234-9abc-0123456789ab-20260228", ok: true},
		{loc: "0198f7a1-1234-7234-9abc-0123456789ab-20250229", ok: false}, // 2025 is not a leap year
	} {
		t.Run(test.loc[len(test.loc)-8:], func(t *testing.T) {
			_, err := Resolve("file:///mnt/shared/snapshots", test.loc)
			if test.ok && err != nil {
				t.Fatalf("Resolve() rejected valid date: %v", err)
			}
			if !test.ok && err == nil {
				t.Fatal("Resolve() accepted a non-existent calendar date")
			}
		})
	}
}

func TestResolveRejectsInvalidInputs(t *testing.T) {
	for _, test := range []struct {
		name   string
		parent string
		loc    string
	}{
		{name: "missing parent", loc: "0198f7a1-1234-7234-9abc-0123456789ab-20260824"},
		{name: "relative parent", parent: "file://relative", loc: "0198f7a1-1234-7234-9abc-0123456789ab-20260824"},
		{name: "remote parent", parent: "https://example.test/path", loc: "0198f7a1-1234-7234-9abc-0123456789ab-20260824"},
		{name: "no date suffix", parent: "file:///tmp", loc: "0198f7a1-1234-7234-9abc-0123456789ab"},
		{name: "wrong separator", parent: "file:///tmp", loc: "0198f7a1-1234-7234-9abc-0123456789ab_20260824"},
		{name: "short date", parent: "file:///tmp", loc: "0198f7a1-1234-7234-9abc-0123456789ab-2026082"},
		{name: "non-numeric date", parent: "file:///tmp", loc: "0198f7a1-1234-7234-9abc-0123456789ab-2026ab24"},
		{name: "impossible day", parent: "file:///tmp", loc: "0198f7a1-1234-7234-9abc-0123456789ab-20260230"},
		{name: "impossible month", parent: "file:///tmp", loc: "0198f7a1-1234-7234-9abc-0123456789ab-20261301"},
		{name: "date only", parent: "file:///tmp", loc: "20260824"},
		{name: "path separator", parent: "file:///tmp", loc: "a/b-20260824"},
		{name: "dot", parent: "file:///tmp", loc: ".-20260824"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Resolve(test.parent, test.loc); err == nil {
				t.Fatal("invalid ref location input was accepted")
			}
		})
	}
}

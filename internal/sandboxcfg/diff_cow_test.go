package sandboxcfg

import (
	"fmt"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestDiffCOWRequestPolicy(t *testing.T) {
	for _, raw := range []string{`{"diff_cow":null}`, `{"diff_cow":{"cache_size":""}}`, `{"diff_cow":{"cache_size":"1KiB"}}`, `{"diff_cow":{"cache_size":null}}`, `{"diff_cow":{"unknown":"4KiB"}}`, `{"diff_cow":{"cache_size":"8MiB","max_dirty_size":"16MiB"}}`} {
		if _, err := ParseResourcePatch(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	base, err := ParseResourcePatch(`{"diff_cow":{"cache_size":"64MiB","max_dirty_size":"32MiB"}}`)
	if err != nil {
		t.Fatal(err)
	}
	over, _ := ParseResourcePatch(`{"diff_cow":{"max_dirty_size":"8MiB"}}`)
	merged, err := MergeResourcePatch(base, over)
	if err != nil {
		t.Fatal(err)
	}
	if *merged.DiffCOW.CacheSize != "64MiB" || *merged.DiffCOW.MaxDirtySize != "8MiB" {
		t.Fatal(merged)
	}
	*merged.DiffCOW.CacheSize = "128MiB"
	if *base.DiffCOW.CacheSize != "64MiB" {
		t.Fatal("aliased patch")
	}
	policy := NodeResourcePolicy{DiffCOW: rtconfig.DiffCOWConfig{CacheSize: "64MiB", MaxDirtySize: "32MiB"}}
	resolved, err := ResolveResources(ResourceResolveInput{Node: policy, Patch: over})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.DiffCOW.CacheSize != "64MiB" || resolved.DiffCOW.MaxDirtySize != "8MiB" {
		t.Fatal(resolved.DiffCOW)
	}
}

func TestDiffCOWRestoreAndDefaults(t *testing.T) {
	capacity := rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"}
	for _, restore := range []bool{false, true} {
		p, err := ParseResourcePatch(`{"diff_cow":{"max_dirty_size":"8MiB"}}`)
		if err != nil {
			t.Fatal(err)
		}
		r, err := ResolveResources(ResourceResolveInput{Patch: p, Restore: restore, ArtifactCapacity: &capacity})
		if err != nil {
			t.Fatal(err)
		}
		cache, dirty, err := r.DiffCOW.Bytes()
		if err != nil || cache != 32<<20 || dirty != 8<<20 {
			t.Fatalf("restore=%v: %v %v %v", restore, cache, dirty, err)
		}
	}
	p, _ := ParseResourcePatch(`{"diff_cow":{"max_dirty_size":"128MiB"}}`)
	if _, err := ResolveResources(ResourceResolveInput{Patch: p}); err == nil {
		t.Fatal("accepted dirty above effective cache")
	}
}

func TestDiffCOWErrorIdentifiesLeafAndValue(t *testing.T) {
	for _, field := range []string{"cache_size", "max_dirty_size"} {
		for _, value := range []string{"", "1KiB", "-4096", "18446744073709551616"} {
			_, err := ParseResourcePatch(fmt.Sprintf(`{"diff_cow":{%q:%q}}`, field, value))
			if err == nil || !strings.Contains(err.Error(), "diff_cow."+field) || !strings.Contains(err.Error(), fmt.Sprintf("got %q", value)) {
				t.Fatalf("%s=%q: %v", field, value, err)
			}
		}
	}
}

package sandboxcfg

import (
	"testing"
	"time"
)

func TestCacheIdleAfterResourcePatchResolution(t *testing.T) {
	node := NodeResourcePolicy{DiffCOW: &DiffCOWPatch{
		CacheSize: stringPtr("64MiB"), CacheIdleAfter: stringPtr("30s"),
	}}
	patch, err := ParseResourcePatch(`{"diff_cow":{"max_dirty_size":"16MiB","cache_idle_after":"250ms"}}`)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := ResolveResources(ResourceResolveInput{Node: node, Patch: patch})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.DiffCOW.CacheIdleAfter != "250ms" {
		t.Fatalf("override ignored: %+v", resolved.DiffCOW)
	}
	d, err := resolved.DiffCOW.CacheIdleDuration()
	if err != nil || d != 250*time.Millisecond {
		t.Fatalf("duration %v %v", d, err)
	}
	canon, err := MarshalResourcePatch(patch)
	if err != nil || canon != `{"diff_cow":{"max_dirty_size":"16MiB","cache_idle_after":"250ms"}}` {
		t.Fatalf("patch changed: %s %v", canon, err)
	}
	for _, invalid := range []string{
		`{"diff_cow":{"cache_idle_after":"0s"}}`,
		`{"diff_cow":{"cache_idle_after":null}}`,
		`{"diff_cow":{"cache_idle_after":"wrong"}}`,
	} {
		p, err := ParseResourcePatch(invalid)
		if err == nil {
			_, err = ResolveResources(ResourceResolveInput{Node: node, Patch: p})
		}
		if err == nil {
			t.Errorf("accepted invalid %s", invalid)
		}
	}
}

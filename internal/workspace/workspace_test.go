package workspace

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestExtractValidateAndProject(t *testing.T) {
	input := map[string]string{Namespace: `[{"name":"prj","mounts":[{"path":"/data","subpath":"./src"},{"path":"/out","subpath":"output"}],"disk":{"migrationToken":"secret"}},{"name":"logs","mounts":[{"path":"/logs"}],"slot":{"size":"8GiB"}}]`}
	pub, entries, err := Extract(input)
	if err != nil || len(entries) != 2 || strings.Contains(pub[Namespace], "secret") || strings.Contains(pub[Namespace], "slot") || !strings.Contains(input[Namespace], "secret") {
		t.Fatalf("bad extraction: %s %v", pub[Namespace], err)
	}
	plan := []Prepared{{Name: "prj", Mounts: entries[0].Mounts, Disk: &rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///seed"}}, {Name: "logs", Mounts: entries[1].Mounts, Slot: entries[1].Slot}}
	c := rtconfig.SandboxConfig{}
	if err := ApplyImage(&c, plan); err != nil {
		t.Fatal(err)
	}
	if c.Boot.Disks[0].Disk == nil || c.Boot.Disks[0].Slot != nil || c.Boot.Disks[1].Slot == nil || c.Mounts[0].Subpath != "src" || len(c.Mounts) != 3 || c.Metadata[Namespace] != pub[Namespace] {
		t.Fatalf("wrong image config: %+v", c)
	}
	portable := &rtconfig.PortableSandboxConfig{Version: 2, Metadata: c.Metadata, Mounts: c.Mounts}
	portable.Boot.Disks = []rtconfig.PortableDiskConfig{{Name: "prj", PureBlock: true}, {Name: "logs", Slot: c.Boot.Disks[1].Slot}}
	captured, err := Captured(portable)
	if err != nil || TopologyJSON(captured) != pub[Namespace] {
		t.Fatalf("captured: %v %v", captured, err)
	}
	portable.Boot.Disks[0].PureBlock = false
	if _, err := Captured(portable); err == nil {
		t.Fatal("accepted an ordinary legacy disk based solely on metadata")
	}
	portable.Boot.Disks[0].PureBlock = true
	portable.Mounts[0].Subpath = "other"
	if _, err := Captured(portable); err == nil {
		t.Fatal("accepted mismatched source directories")
	}
}

func TestParseRejectsAmbiguousWorkspaceDeclarations(t *testing.T) {
	for _, raw := range []string{
		`null`, `{}`, `false`, `[{"name":"x","mounts":[]}]`, `[{"name":"x","slot":null}]`, `[{"name":"x","disk":{},"slot":{}}]`, `[{"name":"x","disk":{"diskRef":"file:///a"}}]`, `[{"name":"x","disk":{"migrationToken":"x","version":1}}]`, `[{"name":"x","slot":{"size":"3"}}]`, `[{"name":"x","name":"y"}]`, `[{"name":"x","disk":{"kind":"disk","version":1}}]`, `[{"name":"x","mounts":[{"path":"/a","subpath":"../b"}]}]`, `[{"name":"x","mounts":[{"path":"/a","subpath":"b"},{"path":"/c","subpath":"b/e"}]}]`, `[{"name":"x","mounts":[{"path":"/a"}]},{"name":"y","mounts":[{"path":"/a/b"}]}]`,
	} {
		if _, err := Parse(raw); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	plan := []Prepared{{Name: "p", Mounts: []Mount{{Path: "/a", Subpath: "b"}, {Path: "/aa", Subpath: "bb"}}}}
	config := rtconfig.SandboxConfig{Mounts: []rtconfig.MountConfig{{Target: "/a/c", Type: "tmpfs"}}}
	if err := ApplyImage(&config, plan); err == nil {
		t.Fatal("workspace shadowed an ordinary mount")
	}
	nine := make([]Entry, 9)
	for i := range nine {
		nine[i].Name = string(rune('a' + i))
	}
	raw, _ := json.Marshal(nine)
	if _, err := Parse(string(raw)); err == nil {
		t.Fatal("accepted more than eight disks")
	}
	locations := map[string]string{"a": "file:///shared"}
	if err := MergeLocations(locations, []Prepared{{RefLocations: rtconfig.RefLocations{"a": "/shared", "b": "/node B"}}}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(locations, map[string]string{"a": "file:///shared", "b": "file:///node%20B"}) {
		t.Fatalf("bad locations: %v", locations)
	}
	if err := MergeLocations(locations, []Prepared{{RefLocations: rtconfig.RefLocations{"a": "/other"}}}); err == nil {
		t.Fatal("accepted conflicting named location")
	}
}

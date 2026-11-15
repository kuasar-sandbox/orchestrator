package workspace

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestExtractValidateAndProject(t *testing.T) {
	input := map[string]string{Namespace: `[{"name":"prj","mounts":[{"path":"/data","subpath":"src"},{"path":"/out","subpath":"output"}],"disk":{"migrationToken":"secret"}},{"name":"logs","mounts":[{"path":"/logs"}],"slot":{"size":"8GiB"}}]`}
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
	captured := Captured(portable)
	if TopologyJSON(captured) != pub[Namespace] {
		t.Fatalf("captured: %v", captured)
	}
	portable.Boot.Disks[0].PureBlock = false
	if got := Captured(portable); len(got) != 1 || got[0].Name != "logs" {
		t.Fatalf("views after removing native device: %v", got)
	}
	portable.Boot.Disks[0].PureBlock = true
	portable.Mounts[0].Subpath = "other"
	if got := Captured(portable); len(got) != 1 || got[0].Name != "logs" {
		t.Fatalf("views after changing source directory: %v", got)
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

func TestInputPathComponentsAndSavedView(t *testing.T) {
	for _, m := range []Mount{
		{Path: "/a/./b"}, {Path: "/a/../b"}, {Path: "/a", Subpath: "."},
		{Path: "/a", Subpath: "src/./b"}, {Path: "/a", Subpath: "src/../b"},
	} {
		raw := TopologyJSON([]Entry{{Name: "project", Mounts: []Mount{m}}})
		if _, err := Parse(raw); err == nil {
			t.Errorf("new declaration accepted %+v", m)
		}
	}
	const valid = `[{"name":"project","mounts":[{"path":"/.git","subpath":"v..2"}]}]`
	if entries, err := Parse(valid); err != nil || TopologyJSON(entries) != valid {
		t.Fatalf("valid dotted name: %v, %v", entries, err)
	}
	const saved = `[{"name":"project","mounts":[{"path":"/src/./tree","subpath":"."}]}]`
	cfg := &rtconfig.PortableSandboxConfig{Version: 2, Metadata: map[string]string{Namespace: saved},
		Mounts: []rtconfig.MountConfig{{Type: "disk", Target: "/src/tree", Source: "project"}}}
	cfg.Boot.Disks = []rtconfig.PortableDiskConfig{{Name: "project", Slot: &rtconfig.DiskSlotConfig{}}}
	entries := Captured(cfg)
	want := `[{"name":"project","mounts":[{"path":"/src/tree"}]}]`
	if TopologyJSON(entries) != want || PublicMetadata(cfg.Metadata)[Namespace] != want {
		t.Fatalf("saved view no longer readable: %v", entries)
	}
}

func TestSavedViewsDropOnlyStaleOrAmbiguousEntries(t *testing.T) {
	const raw = `[
		{"name":"first","mounts":[{"path":"/first"}],"disk":{"migrationToken":"private"}},
		{"name":"bad","mounts":"wrong"},
		{"name":"duplicate","mounts":[{"path":"/dup"}]},
		{"name":"nested","mounts":[{"path":"/shared/nested"}]},
		{"name":"duplicate","mounts":[]},
		{"name":"parent","mounts":[{"path":"/shared"}]},
		{"name":"last","mounts":[{"path":"/last"}]}]`
	const want = `[{"name":"first","mounts":[{"path":"/first"}]},{"name":"last","mounts":[{"path":"/last"}]}]`
	cfg := &rtconfig.PortableSandboxConfig{Version: 2, Metadata: map[string]string{Namespace: raw, "other": "kept"}}
	for _, m := range []rtconfig.MountConfig{
		{Type: "disk", Source: "first", Target: "/first"},
		{Type: "disk", Source: "duplicate", Target: "/dup"},
		{Type: "disk", Source: "parent", Target: "/shared"},
		{Type: "disk", Source: "nested", Target: "/shared/nested"},
		{Type: "disk", Source: "last", Target: "/last"},
	} {
		cfg.Boot.Disks = append(cfg.Boot.Disks, rtconfig.PortableDiskConfig{Name: m.Source, PureBlock: true})
		cfg.Mounts = append(cfg.Mounts, m)
	}
	if got := TopologyJSON(Captured(cfg)); got != want {
		t.Fatalf("captured views = %s, want %s", got, want)
	}
	if got := PublicMetadata(cfg.Metadata); got[Namespace] != want || got["other"] != "kept" || cfg.Metadata[Namespace] != raw {
		t.Fatalf("public metadata = %+v", got)
	}
	if _, err := Parse(raw); err == nil {
		t.Fatal("new input accepted a partially invalid declaration")
	}
}

func TestWorkspaceProjectionLeavesOrdinaryMountsAlone(t *testing.T) {
	source := &rtconfig.DiskV1{Kind: "disk", Version: 1, DiffTemplate: "file:///seed"}
	plan := []Prepared{
		{Name: "ordinary", Disk: source, Mounts: []Mount{{Path: "/data"}}},
		{Name: "other-slot", Slot: &rtconfig.DiskSlotConfig{}, Mounts: []Mount{{Path: "/work"}}},
	}
	cfg := rtconfig.SandboxConfig{Mounts: []rtconfig.MountConfig{{Type: "tmpfs", Target: "/data/cache"}}}
	if err := ApplyImage(&cfg, plan); err != nil {
		t.Fatalf("unrelated slot changed ordinary mounts: %v", err)
	}
	if cfg.Boot.Disks[0].Disk != source || cfg.Boot.Disks[0].Slot != nil || cfg.Boot.Disks[1].Slot == nil {
		t.Fatalf("disk declarations changed: %+v", cfg.Boot.Disks)
	}
}

func TestAdditionalSBXDevicesOnlyUseNewNamesInDeclarationOrder(t *testing.T) {
	captured := []Entry{{Name: "existing", Mounts: []Mount{{Path: "/existing"}}}}
	choices := []Prepared{{Name: "second", Mounts: []Mount{{Path: "/second"}}}, {Name: "existing"}, {Name: "first", Mounts: []Mount{{Path: "/first"}}}}
	got, err := AdditionalDevices([]string{"ordinary", "existing"}, captured, choices)
	if err != nil || len(got) != 2 || got[0].Name != "second" || got[1].Name != "first" {
		t.Fatalf("added=%+v: %v", got, err)
	}
	for _, p := range []Prepared{{Name: "ordinary", Mounts: []Mount{{Path: "/x"}}}, {Name: "added"}, {Name: "added", Mounts: []Mount{{Path: "/existing/sub"}}}} {
		if _, err := AdditionalDevices([]string{"ordinary", "existing"}, captured, []Prepared{p}); err == nil {
			t.Fatalf("accepted %+v", p)
		}
	}
	if _, err := AdditionalDevices([]string{"a", "b", "c", "d", "e", "f", "g", "h"}, nil, choices[:1]); err == nil {
		t.Fatal("accepted ninth disk")
	}
}

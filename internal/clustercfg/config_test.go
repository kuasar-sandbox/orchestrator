package clustercfg

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRouterNeedsDomain(t *testing.T) {
	c := DefaultRouter()
	if err := c.Validate(); err == nil {
		t.Fatal("expected domain-required error")
	}
	c.Domain = "sandboxes.example.com"
	if err := c.Validate(); err != nil {
		t.Fatalf("default router + domain should validate: %v", err)
	}
}

func TestLoadRegistryPartialAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	os.WriteFile(path, []byte("member:\n  listen: \":8800\"\n"), 0o600)
	c, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Member.Listen != ":8800" || c.NodeListen() != ":8800" {
		t.Fatalf("member/node listen defaults not filled: member=%+v node=%+v", c.Member, c.NodeLink)
	}
	if got := c.SelfNodeAdvertise(); got != defaultRegistryBootstrap {
		t.Fatalf("self node_advertise default=%q, want %q", got, defaultRegistryBootstrap)
	}
	if c.Membership.Active != 1 || len(c.Membership.Versions) != 1 || c.Membership.Owners.RouteLink != 1 || c.Membership.Owners.PlacerLink != 1 {
		t.Fatalf("membership defaults not filled: %+v", c.Membership)
	}
	if c.Membership.ReloadReadyTimeout != "10s" {
		t.Fatalf("membership reload_ready_timeout default=%q, want 10s", c.Membership.ReloadReadyTimeout)
	}
	if c.NodeLink.HeartbeatInterval != "10s" || c.NodeLink.NodeDeadAfter != "30s" {
		t.Fatalf("node_link defaults not filled: %+v", c.NodeLink)
	}
	if c.RouteLink.ParkTimeout != "30s" || c.NodeList.WatchRetention != 10000 ||
		c.PlacerLink.PlaceTimeout != "2s" {
		t.Fatalf("link defaults not filled: route=%+v node_list=%+v scale=%+v", c.RouteLink, c.NodeList, c.PlacerLink)
	}
}

func TestLoadPlacerPartialAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "placer.yaml")
	os.WriteFile(path, []byte("placement:\n  candidates: 3\n"), 0o600)
	c, err := LoadPlacer(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Placement.Candidates != 3 || c.Placement.ZoneAdmitMax != "yellow" ||
		c.Placement.ImportSourceOwnerCount != 3 || c.Placement.ImportSourceLeaseTTL != "15s" || c.Placement.SelectorPatchRefresh != "1m" {
		t.Fatalf("placement merge wrong: %+v", c.Placement)
	}
	if c.Registry.Bootstrap == "" {
		t.Fatal("placer registry.bootstrap default not filled")
	}
}

func TestPlacerImportGroupsValidatesFileSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(t.TempDir(), "placer.yaml")
	raw := []byte("import_groups:\n  - source_id: file-a\n    source_type: file\n    path: " + dir + "\n")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadPlacer(path)
	if err != nil {
		t.Fatalf("load placer file source: %v", err)
	}
	if len(c.ImportGroups) != 1 || c.ImportGroups[0].Path != dir {
		t.Fatalf("import_groups not loaded: %+v", c.ImportGroups)
	}
	c.ImportGroups[0].SourceType = "unknown"
	if err := c.Validate(); err == nil {
		t.Fatal("unknown source type should be rejected")
	}
}

func TestRegistryValidateRejects(t *testing.T) {
	c := DefaultRegistry()
	c.RouteLink.ParkTimeout = "notaduration"
	if err := c.Validate(); err == nil {
		t.Fatal("bad duration should be rejected")
	}
	c = DefaultRegistry()
	c.Membership.ReloadReadyTimeout = "notaduration"
	if err := c.Validate(); err == nil {
		t.Fatal("bad membership reload_ready_timeout should be rejected")
	}
}

func TestRegistryMembershipNextAllowsNextOnlyMember(t *testing.T) {
	c := DefaultRegistry()
	c.Member.ID = "c"
	c.Membership.Active = 1
	c.Membership.Next = 2
	c.Membership.Versions = []MembershipVersion{
		{Version: 1, Members: []MembershipMember{
			{ID: "a", Advertise: "http://a.example.test"},
			{ID: "b", Advertise: "http://b.example.test"},
		}},
		{Version: 2, Members: []MembershipMember{
			{ID: "b", Advertise: "http://b.example.test"},
			{ID: "c", Advertise: "http://c.example.test"},
		}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("next-only member should validate: %v", err)
	}
	c.Member.ID = "d"
	if err := c.Validate(); err == nil {
		t.Fatal("member outside active+next should be rejected")
	}
}

func TestRegistryMembershipOldGraceAllowsGraceOnlyMember(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.yaml")
	raw := `member:
  id: registry-a
  listen: "127.0.0.1:0"
membership:
  active: 2
  old_grace: 1
  versions:
    - version: 1
      members:
        - { id: registry-a, advertise: "http://registry-a.example.test" }
        - { id: registry-b, advertise: "http://registry-b.example.test" }
        - { id: registry-c, advertise: "http://registry-c.example.test" }
    - version: 2
      members:
        - { id: registry-b, advertise: "http://registry-b.example.test" }
        - { id: registry-c, advertise: "http://registry-c.example.test" }
        - { id: registry-d, advertise: "http://registry-d.example.test" }
  owners:
    route_link: 2
    node_link: 2
    placer_link: 2
    node_list: 2
`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadRegistry(path)
	if err != nil {
		t.Fatalf("old_grace-only registry member should validate: %v", err)
	}
	owners := c.Membership.OwnerVersions()
	if len(owners) != 1 || owners[0].Version != 2 {
		t.Fatalf("old_grace version must not be part of owner views: %+v", owners)
	}
	shards := c.Membership.ShardVersions()
	if len(shards) != 2 || shards[0].Version != 2 || shards[1].Version != 1 {
		t.Fatalf("shard versions should include active then old_grace: %+v", shards)
	}
	members := c.Membership.MemberVersions()
	if len(members) != 2 || members[0].Version != 2 || members[1].Version != 1 {
		t.Fatalf("member versions should include active then old_grace: %+v", members)
	}
}

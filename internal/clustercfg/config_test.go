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
	if c.Membership.Active != 1 || len(c.Membership.Versions) != 1 || c.Membership.Owners.RouteLink != 1 {
		t.Fatalf("membership defaults not filled: %+v", c.Membership)
	}
	if c.NodeLink.RevisionRetention != 10000 || c.NodeLink.HeartbeatInterval != "10s" {
		t.Fatalf("node_link defaults not filled: %+v", c.NodeLink)
	}
	if c.RouteLink.ParkTimeout != "30s" || c.NodeList.WatchRetention != 10000 || c.ScaleLink.PlaceTimeout != "2s" {
		t.Fatalf("link defaults not filled: route=%+v node_list=%+v scale=%+v", c.RouteLink, c.NodeList, c.ScaleLink)
	}
}

func TestLoadScalerPartialAppliesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scaler.yaml")
	os.WriteFile(path, []byte("placement:\n  candidates: 3\n"), 0o600)
	c, err := LoadScaler(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.Placement.Candidates != 3 || c.Placement.ZoneAdmitMax != "yellow" || c.Placement.NodeDeadAfter != "30s" {
		t.Fatalf("placement merge wrong: %+v", c.Placement)
	}
	if c.Registry.Bootstrap == "" {
		t.Fatal("scaler registry.bootstrap default not filled")
	}
}

func TestRegistryValidateRejects(t *testing.T) {
	c := DefaultRegistry()
	c.RouteLink.ParkTimeout = "notaduration"
	if err := c.Validate(); err == nil {
		t.Fatal("bad duration should be rejected")
	}
}

func TestRegistryMembershipNextAllowsNextOnlyMember(t *testing.T) {
	c := DefaultRegistry()
	c.Member.ID = "c"
	c.Membership.Active = 1
	c.Membership.Next = 2
	c.Membership.Versions = []MembershipVersion{
		{Version: 1, Members: []MembershipMember{{ID: "a"}, {ID: "b"}}},
		{Version: 2, Members: []MembershipMember{{ID: "b"}, {ID: "c"}}},
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("next-only member should validate: %v", err)
	}
	c.Member.ID = "d"
	if err := c.Validate(); err == nil {
		t.Fatal("member outside active+next should be rejected")
	}
}

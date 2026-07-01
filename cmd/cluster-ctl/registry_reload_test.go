package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

func TestRegistryReloadRejectsDirectActiveSwitchWithoutJointConfig(t *testing.T) {
	old := loadRegistryReloadTestConfig(t, registryReloadTestConfig(1, 0, "registry-b", map[int][]string{
		1: {"registry-a", "registry-b", "registry-c"},
	}))
	path := writeRegistryReloadTestConfig(t, registryReloadTestConfig(2, 0, "registry-b", map[int][]string{
		2: {"registry-b", "registry-c", "registry-d"},
	}))
	cfgState, reg := newRegistryReloadTestRuntime(t, old)

	err := reloadRegistryConfig(context.Background(), path, old, cfgState, reg, nil, nil)
	if err == nil {
		t.Fatal("direct active membership switch without prior joint config should be rejected")
	}
	if !strings.Contains(err.Error(), "membership transition") {
		t.Fatalf("reload rejected with unexpected error: %v", err)
	}
}

func TestRegistryReloadAllowsCutoverFromConfiguredNextMembership(t *testing.T) {
	old := loadRegistryReloadTestConfig(t, registryReloadTestConfig(1, 2, "registry-b", map[int][]string{
		1: {"registry-a", "registry-b", "registry-c"},
		2: {"registry-b", "registry-c", "registry-d"},
	}))
	path := writeRegistryReloadTestConfig(t, registryReloadTestConfig(2, 0, "registry-b", map[int][]string{
		2: {"registry-b", "registry-c", "registry-d"},
	}))
	cfgState, reg := newRegistryReloadTestRuntime(t, old)

	if err := reloadRegistryConfig(context.Background(), path, old, cfgState, reg, nil, nil); err != nil {
		t.Fatalf("cutover from old next membership should reload: %v", err)
	}
	if got := cfgState.get().Membership.Active; got != 2 {
		t.Fatalf("active membership after cutover = %d, want 2", got)
	}
}

func TestBuildRegistryTopologyKeepsOldGracePeersOutOfOwnerViews(t *testing.T) {
	cfg := loadRegistryReloadTestConfig(t, `member:
  id: registry-b
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
    scale_link: 2
    node_list: 2
`)
	views, nodeOwners, err := buildRegistryTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 1 || views[0].Version != 2 {
		t.Fatalf("old_grace version must not enter owner views: %+v", views)
	}
	if !strings.HasPrefix(views[0].Label, "registry.2.") {
		t.Fatalf("owner view label = %q, want computed registry.2 label", views[0].Label)
	}
	if _, ok := nodeOwners["registry-a"]; !ok {
		t.Fatalf("old_grace peer registry-a missing from node owner remotes: %+v", nodeOwners)
	}
}

func TestNewRegistryStoresSetsNodeListHeartbeatRefresh(t *testing.T) {
	cfg := loadRegistryReloadTestConfig(t, registryReloadTestConfig(1, 0, "registry-b", map[int][]string{
		1: {"registry-a", "registry-b", "registry-c"},
	}))
	stores, _, err := newRegistryStores(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := stores.NodeListHeartbeatRefreshSec(); got != 1 {
		t.Fatalf("node_list heartbeat refresh sec = %d, want 1 for node_dead_after=3s", got)
	}
}

func newRegistryReloadTestRuntime(t *testing.T, cfg *clustercfg.RegistryConfig) (*registryRuntimeConfig, *registry.Registry) {
	t.Helper()
	stores, remoteNodeOwners, err := newRegistryStores(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New(stores, nil, cfg.RouteLink.ParkDur(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	reg.SetRemoteNodeOwners(remoteNodeOwners)
	return newRegistryRuntimeConfig(cfg), reg
}

func loadRegistryReloadTestConfig(t *testing.T, raw string) *clustercfg.RegistryConfig {
	t.Helper()
	cfg, err := clustercfg.LoadRegistry(writeRegistryReloadTestConfig(t, raw))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func writeRegistryReloadTestConfig(t *testing.T, raw string) string {
	t.Helper()
	path := t.TempDir() + "/registry.yaml"
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func registryReloadTestConfig(active, next int, self string, versions map[int][]string) string {
	var b strings.Builder
	b.WriteString("member:\n")
	b.WriteString("  id: " + self + "\n")
	b.WriteString("  listen: \"127.0.0.1:0\"\n")
	b.WriteString("membership:\n")
	b.WriteString("  active: ")
	b.WriteString(intString(active))
	b.WriteString("\n")
	if next != 0 {
		b.WriteString("  next: ")
		b.WriteString(intString(next))
		b.WriteString("\n")
	}
	b.WriteString("  versions:\n")
	for version, members := range versions {
		b.WriteString("    - version: ")
		b.WriteString(intString(version))
		b.WriteString("\n")
		b.WriteString("      members:\n")
		for _, member := range members {
			b.WriteString("        - { id: ")
			b.WriteString(member)
			b.WriteString(", advertise: \"http://")
			b.WriteString(member)
			b.WriteString(".example.test\" }\n")
		}
	}
	b.WriteString("  owners:\n")
	for _, ns := range []string{"route_link", "node_link", "scale_link", "node_list"} {
		b.WriteString("    ")
		b.WriteString(ns)
		b.WriteString(": 2\n")
	}
	b.WriteString("node_link:\n")
	b.WriteString("  heartbeat_interval: \"500ms\"\n")
	b.WriteString("  node_dead_after: \"3s\"\n")
	b.WriteString("route_link:\n")
	b.WriteString("  park_timeout: \"5s\"\n")
	return b.String()
}

func intString(v int) string {
	const digits = "0123456789"
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v%10]
		v /= 10
	}
	return string(buf[i:])
}

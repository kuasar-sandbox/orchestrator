package controlplane

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type allowTestDirectoryEntries struct{}

func (allowTestDirectoryEntries) AllowDirectoryEntry(session.DirectoryEntry) bool { return true }

func TestReconnectRouterExcludesUnavailableRendezvousWinner(t *testing.T) {
	registryLayout := reconnectTestRegistryLayout()
	available := map[string]bool{"registry-a": true, "registry-b": true, "registry-c": true}
	router, err := NewRegistryLayoutReconnectRouter("registry-a", registryLayout, func(memberID string) bool {
		return available[memberID]
	})
	if err != nil {
		t.Fatal(err)
	}
	var nodeID, originalWinner string
	for index := 0; index < 1000; index++ {
		candidate := fmt.Sprintf("node-%d", index)
		ranked, rankErr := session.RankReconnectTargets(candidate, []session.Member{
			{MemberID: "registry-a", Weight: 1, Available: true},
			{MemberID: "registry-b", Weight: 1, Available: true},
			{MemberID: "registry-c", Weight: 1, Available: true},
		})
		if rankErr == nil && ranked[0] != "registry-a" {
			nodeID, originalWinner = candidate, ranked[0]
			break
		}
	}
	if nodeID == "" {
		t.Fatal("could not find a non-local rendezvous winner")
	}
	available[originalWinner] = false
	targets, err := router.RedirectTargets(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range targets {
		if target.MemberID == originalWinner {
			t.Fatalf("unavailable winner remained a redirect target: %+v", targets)
		}
	}
}

func TestSessionMeshPeerHealthRejectsOlderProbeResult(t *testing.T) {
	mesh, err := NewSessionMesh("registry-a", session.NewDirectory(allowTestDirectoryEntries{}), []SessionPeer{
		{MemberID: "registry-b", Endpoint: "https://registry-b:7700", Client: &http.Client{}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	mesh.recordPeerHealth("registry-b", 2, false)
	mesh.recordPeerHealth("registry-b", 1, true)
	if mesh.MemberAvailable("registry-b") {
		t.Fatal("older successful probe overrode a newer failure")
	}
	if !mesh.MemberAvailable("registry-a") {
		t.Fatal("local Registry member must remain available to itself")
	}
}

func TestReconnectRouterAcceptsLocallyWhenAllPeersUnavailable(t *testing.T) {
	router, err := NewRegistryLayoutReconnectRouter("registry-a", reconnectTestRegistryLayout(), func(string) bool { return false })
	if err != nil {
		t.Fatal(err)
	}
	targets, err := router.RedirectTargets("node-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 0 {
		t.Fatalf("local member did not accept reconnect: %+v", targets)
	}
}

func reconnectTestRegistryLayout() raftstore.RegistryLayout {
	return raftstore.RegistryLayout{Members: []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a:7700", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b:7700", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c:7700", RaftEndpoint: "registry-c:63001"},
	}}
}

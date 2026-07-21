package controlplane

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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

func TestSessionMeshPullsDirectoryAcrossBoundedSnapshotPages(t *testing.T) {
	sourceDirectory := session.NewDirectory(allowTestDirectoryEntries{})
	for index := 0; index < 600; index++ {
		entry := session.DirectoryEntry{
			NodeID: fmt.Sprintf("node-%04d", index), EnrollmentID: fmt.Sprintf("enrollment-%04d", index),
			Tuple: session.Tuple{NodeEpoch: 1, SessionSeq: 1}, HolderMemberID: "registry-b",
		}
		if !sourceDirectory.Apply(session.DirectoryDelta{Entry: entry, Up: true}) {
			t.Fatalf("source entry %d was not installed", index)
		}
	}
	source, err := NewSessionMesh("registry-b", sourceDirectory, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	source.Mount(mux)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == sessionSnapshotPath {
			requests.Add(1)
		}
		mux.ServeHTTP(w, request)
	}))
	defer server.Close()

	destinationDirectory := session.NewDirectory(allowTestDirectoryEntries{})
	destination, err := NewSessionMesh("registry-a", destinationDirectory, []SessionPeer{{
		MemberID: "registry-b", Endpoint: server.URL, Client: server.Client(),
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	destination.healthSeq.Store(1)
	destination.recordPeerHealth("registry-b", 1, false)
	destination.pullSnapshots(context.Background())
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(destinationDirectory.Snapshot()) == 600 && destination.MemberAvailable("registry-b") {
			if requests.Load() != 3 {
				t.Fatalf("snapshot requests = %d, want 3", requests.Load())
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("destination records = %d, peer available=%v, requests=%d",
		len(destinationDirectory.Snapshot()), destination.MemberAvailable("registry-b"), requests.Load())
}

func TestSessionSnapshotPageIsBoundedByEncodedBytes(t *testing.T) {
	records := make([]session.DirectoryRecord, sessionSnapshotPage)
	for index := range records {
		nodeID := fmt.Sprintf("node-%04d-%s", index, strings.Repeat("n", 8<<10))
		records[index] = session.DirectoryRecord{Entry: session.DirectoryEntry{
			NodeID: nodeID, EnrollmentID: strings.Repeat("e", 8<<10),
			Tuple: session.Tuple{NodeEpoch: 1, SessionSeq: 1}, HolderMemberID: "registry-b",
		}, Available: true}
	}
	response, err := boundedSessionSnapshotResponse(records, "more")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maximumSessionRPC || len(response.Records) == 0 || len(response.Records) >= len(records) {
		t.Fatalf("bounded response bytes=%d records=%d", len(encoded), len(response.Records))
	}
	if !validSessionSnapshotPage(response, "") {
		t.Fatalf("byte-bounded continuation is invalid: %+v", response)
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

package routeclient

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

func TestGenerationAuthorizationRequiresEveryFinalServeGate(t *testing.T) {
	client := testClient(t)
	base := routeapi.PermitResponse{
		ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1", SystemEpoch: 1,
		RegistryLayoutDigest: client.digest, CommitIndex: 7, MaxLifetimeMillis: 5000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}
	identity := serveIdentity(base)
	for _, test := range []struct {
		name   string
		mutate func(*routeapi.PermitResponse)
	}{
		{name: "serve", mutate: func(p *routeapi.PermitResponse) { p.ServeGate = false }},
		{name: "cutover", mutate: func(p *routeapi.PermitResponse) { p.CutoverGate = false }},
		{name: "recovery", mutate: func(p *routeapi.PermitResponse) { p.RecoveryClosed = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			permit := base
			test.mutate(&permit)
			client.permit = &cachedPermit{response: permit, expires: time.Now().Add(time.Minute)}
			if _, err := client.CurrentServeIdentity(false); err == nil || client.CacheAuthorized(identity) {
				t.Fatal("closed final gate authorized Router reads or cache forwarding")
			}
		})
	}
	client.permit = &cachedPermit{response: base, expires: time.Now().Add(time.Minute)}
	if got, err := client.CurrentServeIdentity(true); err != nil || got != identity || !client.CacheAuthorized(identity) {
		t.Fatalf("open serveIdentity = %+v, %v cache=%v", got, err, client.CacheAuthorized(identity))
	}
	base.WriteGate = false
	client.permit = &cachedPermit{response: base, expires: time.Now().Add(time.Minute)}
	if _, err := client.CurrentServeIdentity(true); err == nil {
		t.Fatal("closed write gate authorized a mutation")
	}
	if _, err := client.CurrentServeIdentity(false); err != nil {
		t.Fatalf("write gate incorrectly blocked reads: %v", err)
	}
}

func TestLocalReadsUseReplicaSpreadRendezvousWithoutLeaderBias(t *testing.T) {
	client := testClient(t)
	client.leaders[0] = routeapi.LeaderHint{
		MemberID: "registry-a", Endpoint: client.registryLayout.Members[0].InternalEndpoint, Term: 10,
	}
	primaries := make(map[string]int)
	for index := 0; index < 256; index++ {
		endpoints := client.localReadEndpoints(0, string(rune(index)))
		if len(endpoints) != 3 {
			t.Fatalf("local endpoints=%d, want 3", len(endpoints))
		}
		primaries[endpoints[0].MemberID]++
	}
	if len(primaries) != 3 {
		t.Fatalf("rendezvous primaries were not spread across replicas: %v", primaries)
	}
	writeOrder := client.shardEndpoints(0)
	if len(writeOrder) != 3 || writeOrder[0].MemberID != "registry-a" {
		t.Fatalf("leader hint was not retained for strong/write paths: %+v", writeOrder)
	}
}

func testClient(t *testing.T) *Client {
	t.Helper()
	registryLayout := testRouteRegistryLayout()
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make([]Endpoint, len(registryLayout.Members))
	for index, member := range registryLayout.Members {
		endpoints[index] = Endpoint{MemberID: member.MemberID, BaseURL: member.InternalEndpoint, Client: &http.Client{}}
	}
	client, err := New(registryLayout, digest, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testRouteRegistryLayout() raftstore.RegistryLayout {
	members := []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a.test:9443", RaftEndpoint: "127.0.0.1:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b.test:9443", RaftEndpoint: "127.0.0.1:63002"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c.test:9443", RaftEndpoint: "127.0.0.1:63003"},
	}
	replicas := []raftstore.ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-c", ReplicaID: 3},
	}
	shards := make([]raftstore.ShardPlacement, 16)
	for shardID := range shards {
		shards[shardID] = raftstore.ShardPlacement{
			ShardID: uint32(shardID), Replicas: append([]raftstore.ReplicaPlacement(nil), replicas...),
		}
	}
	bootstrap := sha256.Sum256([]byte("bootstrap"))
	return raftstore.RegistryLayout{
		FormatVersion: raftstore.RegistryLayoutFormatV1, ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1",
		RegistryLayoutVersion: 1, SchemaVersion: 1, ProtocolVersion: 1, HashVersion: "ShardHashV1",
		VirtualShardCount: 16, RouteBucketCount: 16, BuildBucketCount: 16,
		ReplicationFactor: raftstore.DefaultReplication, ServePermitMaxMillis: 5000,
		BootstrapTokenDigest: hex.EncodeToString(bootstrap[:]), Members: members,
		SystemReplicas: append([]raftstore.ReplicaPlacement(nil), replicas...), DataShards: shards,
	}
}

package cluster

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
)

func TestShardRecordKeys(t *testing.T) {
	if got := NodeLinkShard("n1"); got != shardkv.ShardKey("n1") {
		t.Fatalf("NodeLinkShard=%q", got)
	}
	group, routeKey, ok := ParseNodeSandboxRecordKey(NodeSandboxRecordKey("/g", "rk"))
	if !ok || group != "/g" || routeKey != "rk" {
		t.Fatalf("node sandbox parse group=%q route=%q ok=%v", group, routeKey, ok)
	}
	group, buildID, ok := ParseNodeBuildRecordKey(NodeBuildRecordKey("/g", "b1"))
	if !ok || group != "/g" || buildID != "b1" {
		t.Fatalf("node build parse group=%q build=%q ok=%v", group, buildID, ok)
	}
	fp, ok := ParseNodeManifestKeyRecordKey(NodeManifestKeyRecordKey("fp1"))
	if !ok || fp != "fp1" {
		t.Fatalf("manifest key parse fp=%q ok=%v", fp, ok)
	}
	if got := RouteLinkShard("/g"); got != shardkv.ShardKey("/g") {
		t.Fatalf("RouteLinkShard=%q", got)
	}
	if got, ok := ParseRouteSandboxRecordKey(RouteSandboxRecordKey("rk")); !ok || got != "rk" {
		t.Fatalf("route sandbox parse=%q ok=%v", got, ok)
	}
	if got, ok := ParseRouteBuildRecordKey(RouteBuildRecordKey("b1")); !ok || got != "b1" {
		t.Fatalf("route build parse=%q ok=%v", got, ok)
	}
	sourceID, ok := ParsePlacerImportSourceShard(PlacerImportSourceShard("source-a"))
	if !ok || sourceID != "source-a" {
		t.Fatalf("scale import parse source=%q ok=%v", sourceID, ok)
	}
}

func TestShardValueCodec(t *testing.T) {
	raw, err := EncodeShardValue(PlacerImportSourceState{SourceID: "src", OwnerID: "s1", Term: 2})
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeShardValue[PlacerImportSourceState](raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceID != "src" || got.OwnerID != "s1" || got.Term != 2 {
		t.Fatalf("decoded=%+v", got)
	}
}

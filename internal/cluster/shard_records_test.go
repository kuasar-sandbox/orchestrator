package cluster

import (
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
)

func TestShardRecordKeys(t *testing.T) {
	if got := NodeLinkShard("n1"); got != shardkv.ShardKey("n1") {
		t.Fatalf("NodeLinkShard=%q", got)
	}
	sandboxID, ok := ParseNodeSandboxRecordKey(NodeSandboxRecordKey("sb1"))
	if !ok || sandboxID != "sb1" {
		t.Fatalf("node sandbox parse=%q ok=%v", sandboxID, ok)
	}
	buildID, ok := ParseNodeBuildRecordKey(NodeBuildRecordKey("b1"))
	if !ok || buildID != "b1" {
		t.Fatalf("node build parse=%q ok=%v", buildID, ok)
	}
	fp, ok := ParseNodeKeyPairRecordKey(NodeKeyPairRecordKey("fp1"))
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

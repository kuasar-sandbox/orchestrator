package placer

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
)

func nodeVersionForTest(commit uint64, batch uint16) metadata.Version {
	var version metadata.Version
	binary.BigEndian.PutUint64(version[:8], commit)
	binary.BigEndian.PutUint16(version[8:], batch)
	return version
}

func TestDecodeNodeListRequiresCommittedVersion(t *testing.T) {
	for _, revision := range []string{"", `,"version":null`, `,"version":1`, `,"version":"00000000000000000000"`, `,"version":"ffffffffffffffffffff"`} {
		if _, ok := decodeNodeList(json.RawMessage(`{"node_id":"n1"` + revision + `}`)); ok {
			t.Fatalf("accepted uncommitted directory entry: %s", revision)
		}
	}
	version := nodeVersionForTest(17, 256)
	node, ok := decodeNodeList(json.RawMessage(`{"node_id":"n1","version":"` + version.String() + `"}`))
	if !ok || node.Version != version {
		t.Fatalf("lost complete directory record version: %+v, ok=%v", node, ok)
	}
}

func TestNodeViewUsesFullDirectoryRecordVersion(t *testing.T) {
	older := &registry.NodeRecord{NodeID: "n1", Version: nodeVersionForTest(17, 255), APIEndpoint: "old"}
	newer := &registry.NodeRecord{NodeID: "n1", Version: nodeVersionForTest(17, 256), APIEndpoint: "new"}
	if fresherNode(older, newer) || !fresherNode(newer, older) || fresherNode(newer, newer) {
		t.Fatal("node freshness must compare the transaction order in the complete record version")
	}
	view := &nodeView{sources: map[string]*nodeSource{
		"older": {live: map[string]*registry.NodeRecord{"n1": older}, synced: true},
		"newer": {live: map[string]*registry.NodeRecord{"n1": newer}, synced: true},
	}}
	for range 20 {
		got := view.values()
		if len(got) != 1 || got[0].Version != newer.Version || got[0].APIEndpoint != "new" {
			t.Fatalf("merged node view chose an older directory version: %+v", got)
		}
	}
}

package cluster

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	RecordSetRouteSandbox    shardkv.RecordSetName = "sandbox"
	RecordSetRouteBuild      shardkv.RecordSetName = "build"
	RecordSetNodeProfile     shardkv.RecordSetName = "profile"
	RecordSetNodeSandbox     shardkv.RecordSetName = "sandbox"
	RecordSetNodeBuild       shardkv.RecordSetName = "build"
	RecordSetNodeManifestKey shardkv.RecordSetName = "manifest_key"
	RecordSetNodeListNodes   shardkv.RecordSetName = "nodes"
	RecordSetPlacerImport    shardkv.RecordSetName = "import"

	NodeLinkProfileRecord  shardkv.RecordKey = "profile"
	RouteLinkProfileRecord shardkv.RecordKey = "profile"
	PlacerLinkStateRecord  shardkv.RecordKey = "state"
	NodeListShard          shardkv.ShardKey  = "node_list"
)

func NodeLinkShard(nodeID string) shardkv.ShardKey {
	return shardkv.ShardKey(nodeID)
}

func NodeSandboxRecordKey(group, routeKey string) shardkv.RecordKey {
	return shardkv.RecordKey(group + "\x00" + routeKey)
}

func NodeBuildRecordKey(group, buildID string) shardkv.RecordKey {
	return shardkv.RecordKey(group + "\x00" + buildID)
}

func NodeManifestKeyRecordKey(fingerprint string) shardkv.RecordKey {
	return shardkv.RecordKey(fingerprint)
}

func RouteLinkShard(group string) shardkv.ShardKey {
	return shardkv.ShardKey(group)
}

func RouteSandboxRecordKey(routeKey string) shardkv.RecordKey {
	return shardkv.RecordKey(routeKey)
}

func RouteBuildRecordKey(buildID string) shardkv.RecordKey {
	return shardkv.RecordKey(buildID)
}

func NodeListRecordKey(nodeID string) shardkv.RecordKey {
	return shardkv.RecordKey(nodeID)
}

func PlacerImportSourceShard(sourceID string) shardkv.ShardKey {
	return shardkv.ShardKey("import/source/" + sourceID)
}

func ParseNodeSandboxRecordKey(key shardkv.RecordKey) (group, routeKey string, ok bool) {
	return parseTwoPartRecordKey(string(key))
}

func ParseNodeBuildRecordKey(key shardkv.RecordKey) (group, buildID string, ok bool) {
	return parseTwoPartRecordKey(string(key))
}

func ParseNodeManifestKeyRecordKey(key shardkv.RecordKey) (fingerprint string, ok bool) {
	return string(key), key != ""
}

func ParseRouteSandboxRecordKey(key shardkv.RecordKey) (routeKey string, ok bool) {
	return string(key), key != ""
}

func ParseRouteBuildRecordKey(key shardkv.RecordKey) (buildID string, ok bool) {
	return string(key), key != ""
}

func ParsePlacerImportSourceShard(shard shardkv.ShardKey) (sourceID string, ok bool) {
	value, ok := strings.CutPrefix(string(shard), "import/source/")
	return value, ok && value != ""
}

func parseTwoPartRecordKey(key string) (string, string, bool) {
	left, right, ok := strings.Cut(key, "\x00")
	return left, right, ok && left != "" && right != ""
}

// NodeProfileRecord is the node_link profile record. Per-node sandboxes, builds,
// and manifest keys are stored as separate node_link records in the same shard.
type NodeProfileRecord struct {
	NodeID            string                    `json:"node_id"`
	State             NodeState                 `json:"state"`
	Labels            map[string]string         `json:"labels,omitempty"`
	Capacity          int                       `json:"capacity,omitempty"`
	BuildCapacity     *routesync.BuildResources `json:"build_capacity,omitempty"`
	DataEndpoint      string                    `json:"data_endpoint,omitempty"`
	RuntimeDigest     string                    `json:"runtime_digest,omitempty"`
	Zone              string                    `json:"zone,omitempty"`
	Allocated         int64                     `json:"allocated,omitempty"`
	Pool              int64                     `json:"pool,omitempty"`
	BuildAlloc        *routesync.BuildResources `json:"build_alloc,omitempty"`
	Counts            int                       `json:"counts,omitempty"`
	Draining          bool                      `json:"draining,omitempty"`
	LastHeartbeatUnix int64                     `json:"last_heartbeat_unix,omitempty"`
	ResumeToken       string                    `json:"resume_token,omitempty"`
	LinkOwner         string                    `json:"link_owner,omitempty"`
}

type PlacerImportSourceState struct {
	SourceID      string `json:"source_id"`
	OwnerID       string `json:"owner_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	Term          uint64 `json:"term,omitempty"`
	Cursor        string `json:"cursor,omitempty"`
	Round         uint64 `json:"round,omitempty"`
	NextRunUnixMs int64  `json:"next_run_unix_ms,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	ExpiresUnixMs int64  `json:"expires_unix_ms,omitempty"`
}

func EncodeShardValue(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode shard value: %w", err)
	}
	return raw, nil
}

func DecodeShardValue[T any](raw []byte) (T, error) {
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("cluster: decode shard value: %w", err)
	}
	return out, nil
}

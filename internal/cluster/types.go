package cluster

import (
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

const (
	NamespaceRouteLink = "route_link"
	NamespaceNodeLink  = "node_link"
	NamespaceNodeList  = "node_list"
	NamespaceScaleLink = "scale_link"
)

// Ballot is the unique write id used by route_link/node_link writes. Multi-member
// owners compare ballots lexicographically by (round, writer); size-1 uses the
// same shape so the storage contract does not change during clustering.
type Ballot struct {
	Round  uint64 `json:"round"`
	Writer string `json:"writer"`
}

func (b Ballot) Less(o Ballot) bool {
	if b.Round != o.Round {
		return b.Round < o.Round
	}
	return b.Writer < o.Writer
}

func (b Ballot) IsZero() bool { return b.Round == 0 && b.Writer == "" }

type RecordMeta struct {
	Ballot    Ballot    `json:"ballot"`
	Rev       uint64    `json:"rev"`
	UpdatedAt time.Time `json:"updated_at"`
}

const (
	ScaleLinkKindTaskLease  = "task_lease"
	ScaleLinkKindAllocation = "allocation"
)

func ScaleLinkTaskKey(taskID string) string { return "task\x00" + taskID }

func ScaleLinkAllocationKey(group string) string { return ScaleLinkKindAllocation + "\x00" + group }

func ScaleLinkShardKey(recordKey string) string { return NamespaceScaleLink + "\x00" + recordKey }

type ScaleLinkRecord struct {
	Meta            RecordMeta `json:"meta"`
	Key             string     `json:"key"`
	Kind            string     `json:"kind"`
	TaskID          string     `json:"task_id"`
	OwnerID         string     `json:"owner_id,omitempty"`
	RunID           string     `json:"run_id,omitempty"`
	Term            uint64     `json:"term,omitempty"`
	ReadyLabel      string     `json:"ready_label,omitempty"`
	Group           string     `json:"group,omitempty"`
	NodeIDs         []string   `json:"node_ids,omitempty"`
	KeyFingerprint  string     `json:"key_fingerprint,omitempty"`
	ManifestKeyType string     `json:"manifest_key_type,omitempty"`
	ManifestKey     string     `json:"manifest_key,omitempty"`
	ManifestKeyRef  string     `json:"manifest_key_ref,omitempty"`
	ExpiresUnixMs   int64      `json:"expires_unix_ms,omitempty"`
}

type RouteState string

const (
	RouteNone     RouteState = "none"
	RouteReserved RouteState = "reserved"
	RouteReady    RouteState = "ready"
	RoutePaused   RouteState = "paused"
	RouteDead     RouteState = "dead"
)

// RouteRecord is the group-sharded route_link value. AccessToken is the
// scaler-derived data-plane token for the current SandboxID generation; registry
// route owners return it without consulting sandbox-group providers.
type RouteRecord struct {
	Meta           RecordMeta                `json:"meta"`
	Group          string                    `json:"group"`
	RouteKey       string                    `json:"route_key"`
	SandboxID      string                    `json:"sandbox_id,omitempty"`
	State          RouteState                `json:"state"`
	NodeID         string                    `json:"node_id,omitempty"`
	TemplateID     string                    `json:"template_id,omitempty"`
	Config         map[string]string         `json:"config,omitempty"`
	AccessToken    string                    `json:"access_token,omitempty"`
	BuildID        string                    `json:"build_id,omitempty"`
	BuildState     string                    `json:"build_state,omitempty"`
	BuildResources *routesync.BuildResources `json:"build_resources,omitempty"`
	BuildReason    string                    `json:"build_reason,omitempty"`
	CreatedUnix    int64                     `json:"created_unix,omitempty"`
}

func RouteKey(group, routeKey string) string { return group + "\x00" + routeKey }

type NodeState string

const (
	NodeUnknown NodeState = "unknown"
	NodeLive    NodeState = "live"
	NodeDrained NodeState = "drained"
	NodeDead    NodeState = "dead"
)

type NodeRecord struct {
	Meta              RecordMeta                `json:"meta"`
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
	ManifestKeys      []NodeManifestKey         `json:"manifest_keys,omitempty"`
	Sandboxes         []NodeSandboxRef          `json:"sandboxes,omitempty"`
	Builds            []NodeBuildRef            `json:"builds,omitempty"`
}

type NodeManifestKey struct {
	Fingerprint     string `json:"fingerprint"`
	Type            string `json:"type,omitempty"`
	Value           string `json:"value,omitempty"`
	Ref             string `json:"ref,omitempty"`
	ExpiresUnix     int64  `json:"expires_unix,omitempty"`
	SentExpiresUnix int64  `json:"sent_expires_unix,omitempty"`
}

type NodeSandboxRef struct {
	Group     string `json:"group"`
	RouteKey  string `json:"route_key"`
	SandboxID string `json:"sandbox_id,omitempty"`
}

type NodeBuildRef struct {
	Group   string `json:"group"`
	BuildID string `json:"build_id"`
}

// NodeListEntry is the low-frequency WATCH_LIST projection consumed by scaler.
// High-frequency load stays in node_link and is fetched at placement time.
type NodeListEntry struct {
	Meta              RecordMeta                `json:"meta"`
	SourceMeta        RecordMeta                `json:"source_meta,omitempty"`
	NodeID            string                    `json:"node_id"`
	Labels            map[string]string         `json:"labels,omitempty"`
	Capacity          int                       `json:"capacity,omitempty"`
	BuildCapacity     *routesync.BuildResources `json:"build_capacity,omitempty"`
	DataEndpoint      string                    `json:"data_endpoint,omitempty"`
	RuntimeDigest     string                    `json:"runtime_digest,omitempty"`
	Draining          bool                      `json:"draining,omitempty"`
	LastHeartbeatUnix int64                     `json:"last_heartbeat_unix,omitempty"`
	Deleted           bool                      `json:"deleted,omitempty"`
}

func ProjectNodeList(n NodeRecord) NodeListEntry {
	return NodeListEntry{
		SourceMeta: n.Meta, NodeID: n.NodeID, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
		BuildCapacity: cloneBuildResources(n.BuildCapacity), DataEndpoint: n.DataEndpoint,
		RuntimeDigest: n.RuntimeDigest, Draining: n.Draining, LastHeartbeatUnix: n.LastHeartbeatUnix,
	}
}

func cloneBuildResources(in *routesync.BuildResources) *routesync.BuildResources {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

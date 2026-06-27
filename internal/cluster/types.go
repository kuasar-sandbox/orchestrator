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

type RouteState string

const (
	RouteNone     RouteState = "none"
	RouteReserved RouteState = "reserved"
	RouteReady    RouteState = "ready"
	RoutePaused   RouteState = "paused"
	RouteDead     RouteState = "dead"
)

// RouteRecord is the group-sharded route_link value. It does not store access
// tokens; routers derive those from auth_key and sandbox_id.
type RouteRecord struct {
	Meta       RecordMeta        `json:"meta"`
	Group      string            `json:"group"`
	RouteKey   string            `json:"route_key"`
	SandboxID  string            `json:"sandbox_id,omitempty"`
	State      RouteState        `json:"state"`
	NodeID     string            `json:"node_id,omitempty"`
	TemplateID string            `json:"template_id,omitempty"`
	Config     map[string]string `json:"config,omitempty"`
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
}

// NodeListEntry is the low-frequency WATCH_LIST projection consumed by scaler.
// High-frequency load stays in node_link and is fetched at placement time.
type NodeListEntry struct {
	Meta              RecordMeta                `json:"meta"`
	NodeID            string                    `json:"node_id"`
	Labels            map[string]string         `json:"labels,omitempty"`
	Capacity          int                       `json:"capacity,omitempty"`
	BuildCapacity     *routesync.BuildResources `json:"build_capacity,omitempty"`
	DataEndpoint      string                    `json:"data_endpoint,omitempty"`
	RuntimeDigest     string                    `json:"runtime_digest,omitempty"`
	Draining          bool                      `json:"draining,omitempty"`
	LastHeartbeatUnix int64                     `json:"last_heartbeat_unix,omitempty"`
}

func ProjectNodeList(n NodeRecord) NodeListEntry {
	return NodeListEntry{
		Meta: n.Meta, NodeID: n.NodeID, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
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

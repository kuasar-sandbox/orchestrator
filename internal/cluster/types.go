package cluster

import (
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const (
	NamespaceRouteLink  = "route_link"
	NamespaceNodeLink   = "node_link"
	NamespaceNodeList   = "node_list"
	NamespacePlacerLink = "placer_link"
)

// Ballot is the unique write id used by registry replicated namespaces. Multi-member
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

type NodeState string

const (
	NodeUnknown NodeState = "unknown"
	NodeLive    NodeState = "live"
	NodeDrained NodeState = "drained"
	NodeDead    NodeState = "dead"
)

type NodeRecord struct {
	Meta                      RecordMeta                     `json:"meta"`
	NodeID                    string                         `json:"node_id"`
	State                     NodeState                      `json:"state"`
	Labels                    map[string]string              `json:"labels,omitempty"`
	Capacity                  int                            `json:"capacity,omitempty"`
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit `json:"build_registration_capacity,omitempty"`
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit `json:"build_execution_capacity,omitempty"`
	APIEndpoint               string                         `json:"api_endpoint,omitempty"`
	DataEndpoint              string                         `json:"data_endpoint,omitempty"`
	RuntimeDigest             string                         `json:"runtime_digest,omitempty"`
	Zone                      string                         `json:"zone,omitempty"`
	Allocated                 int64                          `json:"allocated,omitempty"`
	Pool                      int64                          `json:"pool,omitempty"`
	BuildRegistrationUsage    *routesync.BuildAdmissionUsage `json:"build_registration_usage,omitempty"`
	BuildExecutionUsage       *routesync.BuildAdmissionUsage `json:"build_execution_usage,omitempty"`
	Counts                    int                            `json:"counts,omitempty"`
	Draining                  bool                           `json:"draining,omitempty"`
	LastHeartbeatUnix         int64                          `json:"last_heartbeat_unix,omitempty"`
	ResumeToken               string                         `json:"resume_token,omitempty"`
	LinkOwner                 string                         `json:"link_owner,omitempty"`
	KeyPairs                  []NodeKeyPair                  `json:"key_pairs,omitempty"`
	Sandboxes                 []NodeSandboxRef               `json:"sandboxes,omitempty"`
	Builds                    []NodeBuildRef                 `json:"builds,omitempty"`
}

// NodeKeyPair is the node_link desired lease for one tenant credential pair.
// APISecretFingerprint is the record identity and lifecycle-command lookup key.
// APISecret and ManifestKey are installed and acknowledged atomically.
type NodeKeyPair struct {
	APISecretFingerprint   string `json:"api_secret_fingerprint"`
	APISecretType          string `json:"api_secret_type,omitempty"`
	APISecret              string `json:"api_secret,omitempty"`
	APISecretRef           string `json:"api_secret_ref,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	ManifestKeyType        string `json:"manifest_key_type,omitempty"`
	ManifestKey            string `json:"manifest_key,omitempty"`
	ManifestKeyRef         string `json:"manifest_key_ref,omitempty"`
	ExpiresUnix            int64  `json:"expires_unix,omitempty"`
	AckedExpiresUnix       int64  `json:"acked_expires_unix,omitempty"`
}

type NodeSandboxRef struct {
	Group                string `json:"group"`
	RouteKey             string `json:"route_key"`
	SandboxID            string `json:"sandbox_id"`
	SandboxGeneration    uint64 `json:"sandbox_generation"`
	NodeSandboxID        string `json:"node_sandbox_id"`
	Profile              string `json:"profile"`
	APISecretFingerprint string `json:"api_secret_fingerprint"`
}

type NodeBuildRef struct {
	Group   string `json:"group"`
	BuildID string `json:"build_id"`
}

// NodeListEntry is the low-frequency WATCH_LIST catalog consumed by placer.
// Liveness and high-frequency load remain authoritative at the node owner.
type NodeListEntry struct {
	Meta                      RecordMeta                     `json:"meta"`
	SourceMeta                RecordMeta                     `json:"source_meta,omitempty"`
	NodeID                    string                         `json:"node_id"`
	Labels                    map[string]string              `json:"labels,omitempty"`
	Capacity                  int                            `json:"capacity,omitempty"`
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit `json:"build_registration_capacity,omitempty"`
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit `json:"build_execution_capacity,omitempty"`
	APIEndpoint               string                         `json:"api_endpoint,omitempty"`
	DataEndpoint              string                         `json:"data_endpoint,omitempty"`
	RuntimeDigest             string                         `json:"runtime_digest,omitempty"`
	Draining                  bool                           `json:"draining,omitempty"`
	Deleted                   bool                           `json:"deleted,omitempty"`
}

func ProjectNodeList(n NodeRecord) NodeListEntry {
	return NodeListEntry{
		SourceMeta: n.Meta, NodeID: n.NodeID, Labels: cloneStringMap(n.Labels), Capacity: n.Capacity,
		BuildRegistrationCapacity: cloneBuildAdmissionLimit(n.BuildRegistrationCapacity),
		BuildExecutionCapacity:    cloneBuildAdmissionLimit(n.BuildExecutionCapacity),
		APIEndpoint:               n.APIEndpoint, DataEndpoint: n.DataEndpoint,
		RuntimeDigest: n.RuntimeDigest, Draining: n.Draining,
	}
}

func cloneBuildResources(in *routesync.BuildResources) *routesync.BuildResources {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneBuildAdmissionLimit(in *routesync.BuildAdmissionLimit) *routesync.BuildAdmissionLimit {
	if in == nil {
		return nil
	}
	out := *in
	out.Resources = cloneBuildResources(in.Resources)
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

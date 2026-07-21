package routesync

import (
	"encoding/hex"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/placementproto"
)

// Cluster node-link message types (node.md §10 / cluster.md). They extend the
// Msg union for the node <-> registry channel: the node DIALS the registry and is
// the execution-state authority (its sandbox routes flow as ID-only
// Upsert/Delete + Bookmark), while the registry resolves cluster identity from
// its per-node ownership table and sends Commands the other way. The frame codec and
// the ServeAuthority loop are the same routesync engine the proxy plane uses;
// only the handshake (NodeRegister vs Hello) and the uplink (Command vs Wake)
// differ. Build events arrive with Phase 5.
const (
	TypeNodeRegister   = "node_register"   // node -> registry (node identity; first up-frame)
	TypeHeartbeat      = "heartbeat"       // legacy node -> registry water level
	TypePlacementLoad  = "placement_load"  // node -> Holder request-time placement snapshot
	TypeCommand        = "command"         // registry -> node (lifecycle / key primitive)
	TypeCmdAck         = "cmd_ack"         // node -> registry (command accepted / rejected)
	TypeExecutionEvent = "execution_event" // node -> registry durable Sandbox/Build fact
	TypeEventAck       = "event_ack"       // registry -> node (committed event watermark)
)

type PlacementLoadSnapshot = placementproto.PlacementLoadSnapshot

// NodeLinkPath is the HTTP path node-ctl conductor serve dials to open its node_link
// channel to the registry.
const NodeLinkPath = "/node-link/session"

type SessionTuple struct {
	NodeEpoch  uint64
	SessionSeq uint64
}

// PlaceReq is a registry placement request to the placer.
// TargetRuntimeDigest lets the placer require a matching guest runtime; empty
// means no runtime constraint.
type PlaceReq struct {
	ReqID               string            `json:"req_id"`
	Group               string            `json:"group"`
	RouteKey            string            `json:"route_key"`
	SandboxID           string            `json:"sandbox_id,omitempty"`
	Config              map[string]string `json:"config,omitempty"`
	Build               bool              `json:"build,omitempty"` // a build placement (resource-aware, §4.5)
	TargetRuntimeDigest string            `json:"target_runtime,omitempty"`
	ExcludeNodeIDs      []string          `json:"exclude_node_ids,omitempty"`
}

// PlaceResult is the placer's answer (NodeID set, or NoNode when nothing eligible).
type PlaceResult struct {
	ReqID          string            `json:"req_id"`
	NodeID         string            `json:"node_id,omitempty"`
	NoNode         bool              `json:"no_node,omitempty"`
	Error          string            `json:"error,omitempty"`
	TemplateRef    string            `json:"template_ref,omitempty"`
	TargetPort     int               `json:"target_port,omitempty"`
	Config         map[string]string `json:"config,omitempty"`
	KeyFingerprint string            `json:"key_fp,omitempty"`
	AccessToken    string            `json:"access_token,omitempty"`
	ImageRepo      string            `json:"image_repo,omitempty"`
	RegistryAuth   string            `json:"registry_auth,omitempty"`
}

// SelectorPatch is the placer's placement projection for a group. NodeIDs is the
// explicit node set that should hold the group's manifest key; Selectors carries
// the shuffle-effective selector projection.
type SelectorPatch struct {
	Group           string              `json:"group"`
	Selectors       []map[string]string `json:"selectors"`
	NodeIDs         []string            `json:"node_ids,omitempty"`
	KeyFingerprint  string              `json:"key_fp,omitempty"`
	ManifestKeyType string              `json:"manifest_key_type,omitempty"`
	ManifestKey     string              `json:"manifest_key,omitempty"`
	ManifestKeyRef  string              `json:"manifest_key_ref,omitempty"`
	ImportSourceID  string              `json:"import_source_id,omitempty"`
	ImportOwnerID   string              `json:"import_owner_id,omitempty"`
	ImportRunID     string              `json:"import_run_id,omitempty"`
	ImportTerm      uint64              `json:"import_term,omitempty"`
}

// Command kinds (Command.Kind) — the lifecycle + key primitives the registry
// drives the node with. The node executes via its existing e2b lifecycle (the
// command just carries the intent) and reports the terminal state on the route
// stream. Command callers wait for ack acceptance; Reserve completion waits on
// the route event.
const (
	CmdCreate        = "create"         // boot a sandbox from a template
	CmdConnect       = "connect"        // resume a node-local PAUSED sandbox
	CmdDelete        = "delete"         // destroy a sandbox
	CmdKeyPut        = "key_put"        // durably install / renew a complete group key lease
	CmdKeyDrop       = "key_drop"       // drop one exact group key lease; TTL remains authoritative
	CmdBuildRegister = "build_register" // pre-provision a build on the node (registry-assigned ids, §7.5)

	CmdSandboxAdmitDispatch = "sandbox_admit_dispatch"
	CmdBuildAdmitDispatch   = "build_admit_dispatch"
	CmdSandboxResume        = "sandbox_resume"
	CmdSandboxDelete        = "sandbox_delete"
	CmdRebindExecution      = "rebind_execution"
	CmdFinalizeWorkflow     = "finalize_workflow"
)

// TypeBuildEvent: node -> registry, a build's state transition (cluster.md);
// the registry converges the BuildStore (§6.1) + releases the build's reserved
// resources on a terminal state.
const TypeBuildEvent = "build_event"

// BuildEvent reports a build's state up the node-link (§5.1). The node-link
// owner resolves cluster identity from its per-node build table.
type BuildEvent struct {
	BuildID            string `json:"build_id"`
	NodeEpoch          uint64 `json:"node_epoch,omitempty"`
	EventSeq           uint64 `json:"event_seq,omitempty"`
	RegistryGeneration string `json:"registry_generation,omitempty"`
	BindingDigest      string `json:"binding_digest,omitempty"`
	State              string `json:"state"`
	TemplateID         string `json:"template_id,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

type EventAck struct {
	ObjectKind         string `json:"object_kind"` // sandbox | build
	ObjectID           string `json:"object_id"`
	RegistryGeneration string `json:"registry_generation"`
	BindingDigest      string `json:"binding_digest"`
	EventSeq           uint64 `json:"event_seq"`
}

func (a EventAck) Validate() error {
	if (a.ObjectKind != "sandbox" && a.ObjectKind != "build") || a.ObjectID == "" ||
		a.RegistryGeneration == "" || a.EventSeq == 0 {
		return errors.New("routesync: incomplete execution event ACK")
	}
	digest, err := hex.DecodeString(a.BindingDigest)
	if err != nil || len(digest) != 32 {
		return errors.New("routesync: execution event ACK Binding digest must be SHA-256 hex")
	}
	return nil
}

// EventCursor is process-local replay pagination, not an execution identity or
// authority token. An empty cursor starts at the beginning of the durable set.
type EventCursor struct {
	ObjectKind string
	ObjectID   string
}

// ExecutionEvent is the bounded latest-event wire projection backed by the
// node-local durable outbox. Fields irrelevant to the object kind/state remain
// empty.
type ExecutionEvent struct {
	ObjectKind         string `json:"object_kind"`
	ObjectID           string `json:"object_id"`
	NodeID             string `json:"node_id"`
	NodeEpoch          uint64 `json:"node_epoch"`
	RegistryGeneration string `json:"registry_generation"`
	BindingDigest      string `json:"binding_digest"`
	EventSeq           uint64 `json:"event_seq"`
	State              string `json:"state"`

	DataEndpoint       string `json:"data_endpoint,omitempty"`
	AccessToken        string `json:"access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	TemplateRef        string `json:"template_ref,omitempty"`
	SnapshotLocation   string `json:"snapshot_location,omitempty"`
	ArtifactRef        string `json:"artifact_ref,omitempty"`
	Reason             string `json:"reason,omitempty"`
}

const MaxExecutionEventBytes = 256 << 10

func (e ExecutionEvent) Validate() error {
	if e.ObjectID == "" || e.NodeID == "" || e.NodeEpoch == 0 || e.RegistryGeneration == "" ||
		e.EventSeq == 0 || e.State == "" {
		return errors.New("routesync: incomplete execution event")
	}
	if e.ObjectKind != "sandbox" && e.ObjectKind != "build" {
		return errors.New("routesync: invalid execution event kind")
	}
	digest, err := hex.DecodeString(e.BindingDigest)
	if err != nil || len(digest) != 32 {
		return errors.New("routesync: execution event Binding digest must be SHA-256 hex")
	}
	return nil
}

// CmdAck statuses.
const (
	AckAccepted = "accepted"
	AckRejected = "rejected"

	DispatchAcceptedAdmitted = "ACCEPTED_ADMITTED"
	DispatchAcceptedQueued   = "ACCEPTED_QUEUED"
	DispatchDefinitiveReject = "DEFINITIVE_REJECT"
	DispatchSessionMoved     = "SESSION_MOVED"
	DispatchConflict         = "CONFLICT"
	DispatchWrongBinding     = "WRONG_BINDING"
	DispatchUnknown          = "UNKNOWN"
)

// NodeRegister is the node's first up-frame on node-link: its identity + capacity,
// so the registry can place sandboxes (and later builds) on it and forward the
// data plane to it (cluster.md / §6.1).
type NodeRegister struct {
	Version          int               `json:"version"`
	NodeID           string            `json:"node_id"`
	EnrollmentID     string            `json:"enrollment_id,omitempty"`
	NodeEpoch        uint64            `json:"node_epoch,omitempty"`
	SessionSeq       uint64            `json:"session_seq,omitempty"`
	LoadModelVersion uint16            `json:"load_model_version,omitempty"`
	Labels           map[string]string `json:"labels,omitempty"`          // zone / pool / slot / node (nodeSelectors)
	Capacity         int               `json:"capacity,omitempty"`        // max sandboxes (headroom signal)
	BuildCapacity    *BuildResources   `json:"build_capacity,omitempty"`  // CPU/mem/storage build pool (§7.5)
	DataEndpoint     string            `json:"data_endpoint,omitempty"`   // host:port the router forwards data-plane to
	RuntimeDigest    string            `json:"runtime_digest,omitempty"`  // guest runtime identity
	AcceptRedirect   bool              `json:"accept_redirect,omitempty"` // node can reconnect to owner endpoints from Hello.Redirect
}

type NodeLinkRedirect struct {
	Targets []NodeLinkTarget `json:"targets"`
}

type NodeLinkTarget struct {
	MemberID string `json:"member_id,omitempty"`
	Endpoint string `json:"endpoint"`
}

// BuildResources is a node's build resource pool (or a build's request), kept
// independent of sandbox memory because builds run in their own slice (§7.5).
type BuildResources struct {
	CPU     int   `json:"cpu,omitempty"`     // milli-cores
	Mem     int64 `json:"mem,omitempty"`     // bytes
	Storage int64 `json:"storage,omitempty"` // bytes
}

// Heartbeat is the node's periodic water-level report (cluster.md). Draining
// is set by node-side drain (node-resource.md §2.5) so placement excludes the node.
type Heartbeat struct {
	Zone       string          `json:"zone,omitempty"`
	Allocated  int64           `json:"allocated,omitempty"`   // memory allocated (bytes)
	Pool       int64           `json:"pool,omitempty"`        // allocatable pool (bytes)
	BuildAlloc *BuildResources `json:"build_alloc,omitempty"` // in-flight + reserved build usage
	Counts     int             `json:"counts,omitempty"`      // live sandbox count (headroom signal)
	Draining   bool            `json:"draining,omitempty"`
}

// Command is a lifecycle / key primitive the registry sends the node (cluster.md
// §5.1). The node replies with a CmdAck(cmd_id) immediately (accepted/rejected)
// and reports the terminal sandbox state via the route stream; commands are
// idempotent by SID. Fields are populated per Kind.
type Command struct {
	CmdID                  string `json:"cmd_id"`
	Kind                   string `json:"kind"` // CmdCreate | CmdConnect | CmdDelete | CmdKey* | CmdBuildRegister
	SID                    string `json:"sid,omitempty"`
	NodeEpoch              uint64 `json:"node_epoch,omitempty"`
	SessionSeq             uint64 `json:"session_seq,omitempty"`
	RegistryGeneration     string `json:"registry_generation,omitempty"`
	Binding                string `json:"binding,omitempty"`
	BindingDigest          string `json:"binding_digest,omitempty"`
	OldBindingDigest       string `json:"old_binding_digest,omitempty"`
	DemandDigest           string `json:"demand_digest,omitempty"`
	DispatchSpecDigest     string `json:"dispatch_spec_digest,omitempty"`
	AuthKeyFingerprint     string `json:"auth_key_fingerprint,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint,omitempty"`
	Group                  string `json:"group,omitempty"`
	RouteKey               string `json:"route_key,omitempty"`
	NormalizedDemand       []byte `json:"normalized_demand,omitempty"`
	DispatchSpec           []byte `json:"dispatch_spec,omitempty"`
	ProviderPolicy         string `json:"provider_policy_version,omitempty"`
	// create
	TemplateRef    string            `json:"template_ref,omitempty"` // snapshot template ref (cold start = fast restore)
	KeyFingerprint string            `json:"key_fp,omitempty"`       // manifest-key fingerprint the node must already hold
	Config         map[string]string `json:"config,omitempty"`       // merged sandbox config (node default ⊕ group ⊕ create)
	AccessToken    string            `json:"access_token,omitempty"` // MAC(auth_key,sid), supplied by registry
	// key_put / key_drop
	KeyLease    *NodeKeyLeaseV1    `json:"key_lease,omitempty"`
	KeyLeaseRef *NodeKeyLeaseRefV1 `json:"key_lease_ref,omitempty"`
	// The old manifest-only fields remain consumed by the pre-cutover runtime.
	// The atomic final cutover removes that path; new dispatch uses KeyLease.
	ManifestKeyType string `json:"manifest_key_type,omitempty"` // inline | ref
	ManifestKey     string `json:"manifest_key,omitempty"`      // hex; only on inline key_put
	ManifestKeyRef  string `json:"manifest_key_ref,omitempty"`  // provider ref; resolved out-of-band by node owner
	ExpiresUnix     int64  `json:"expires_unix,omitempty"`      // lease expiry (key_put)
	// build_register (§7.5): pre-provision a build with registry-assigned ids +
	// reserved resources. ImageRepo/RegistryAuth are the group's image-pull creds,
	// delivered WITH the build task and used transiently (never persisted on the node).
	BuildID        string          `json:"build_id,omitempty"`
	Profile        string          `json:"profile,omitempty"`
	BuildResources *BuildResources `json:"build_resources,omitempty"`
	ImageRepo      string          `json:"image_repo,omitempty"`
	RegistryAuth   string          `json:"registry_auth,omitempty"` // docker config.json; transient
}

// CmdAck acknowledges a Command's receipt; the terminal outcome arrives via the
// route stream, not here.
type CmdAck struct {
	CmdID       string             `json:"cmd_id"`
	Status      string             `json:"status"` // AckAccepted | AckRejected
	Outcome     string             `json:"outcome,omitempty"`
	Reason      string             `json:"reason,omitempty"`
	KeyLeaseRef *NodeKeyLeaseRefV1 `json:"key_lease_ref,omitempty"`
}

package routesync

// Cluster node-link message types (node.md §10 / cluster.md §5). They extend the
// Msg union for the node <-> registry channel: the node DIALS the registry and is
// the route authority (its sandbox routes flow as Upsert/Delete + Bookmark, with
// Group/RouteKey/MigrationToken set on each RouteEntry), while the registry
// subscribes (KindRegistry) and sends Commands the other way. The frame codec and
// the ServeAuthority loop are the same routesync engine the proxy plane uses;
// only the handshake (NodeRegister vs Hello) and the uplink (Command vs Wake)
// differ. Build events arrive with Phase 5.
const (
	TypeNodeRegister = "node_register" // node -> registry (node identity; first up-frame)
	TypeHeartbeat    = "heartbeat"     // node -> registry (water level)
	TypeCommand      = "command"       // registry -> node (lifecycle / key primitive)
	TypeCmdAck       = "cmd_ack"       // node -> registry (command accepted / rejected)
	// Scaler-link (cluster.md §5.2): the scaler DIALS the registry (no scaler
	// listen); the registry reverse-requests placement down, the scaler answers up.
	TypePlaceReq      = "place_req"      // registry -> scaler (place this sandbox)
	TypePlaceResult   = "place_result"   // scaler -> registry (suggested node, or no_node)
	TypeSelectorPatch = "selector_patch" // scaler -> registry (shuffle-effective nodeSelectors overlay, §4.4/§7.6)
)

// NodeLinkPath / ScalerLinkPath are the HTTP paths node-ctl serve / cluster-ctl
// scaler dial to open their full-duplex channels to the registry (h2c/h2).
const (
	NodeLinkPath   = "/internal/node-link"
	ScalerLinkPath = "/internal/scaler-link"
)

// PlaceReq is a registry placement request to the scaler (cluster.md §5.2/§7.5).
// TargetRuntimeDigest (when known, e.g. a migration's snapshot runtime) lets the
// scaler prefer runtime-compatible nodes (§4.2); empty = no runtime constraint.
type PlaceReq struct {
	ReqID               string `json:"req_id"`
	Group               string `json:"group"`
	RouteKey            string `json:"route_key"`
	Build               bool   `json:"build,omitempty"`         // a build placement (resource-aware, §4.5)
	TargetRuntimeDigest string `json:"target_runtime,omitempty"`
}

// PlaceResult is the scaler's answer (NodeID set, or NoNode when nothing eligible).
type PlaceResult struct {
	ReqID  string `json:"req_id"`
	NodeID string `json:"node_id,omitempty"`
	NoNode bool   `json:"no_node,omitempty"`
}

// SelectorPatch is the scaler's shuffle-effective nodeSelectors for a group: the
// static selectors narrowed to the group's pinned shuffle slots (§4.4). The
// registry uses it as the key-distribution allocation set (§7.6).
type SelectorPatch struct {
	Group     string              `json:"group"`
	Selectors []map[string]string `json:"selectors"`
}

// Command kinds (Command.Kind) — the lifecycle + key primitives the registry
// drives the node with. The node executes via its existing e2b lifecycle (the
// command just carries the intent) and reports the terminal state on the route
// stream; a Reserve waits on that route event, not the ack.
const (
	CmdCreate  = "create"   // boot a sandbox (cold template restore, or migration import+restore)
	CmdConnect = "connect"  // resume a node-local PAUSED sandbox
	CmdDelete  = "delete"   // destroy a sandbox (kill, or the SAVED two-phase reclaim step)
	CmdKeyPut  = "key_put"  // install / renew a manifest-key lease (reconcile re-sends; cluster.md §7.6)
	CmdKeyDrop = "key_drop" // drop a key lease
)

// CmdAck statuses.
const (
	AckAccepted = "accepted"
	AckRejected = "rejected"
)

// NodeRegister is the node's first up-frame on node-link: its identity + capacity,
// so the registry can place sandboxes (and later builds) on it and forward the
// data plane to it (cluster.md §5.1 / §6.1).
type NodeRegister struct {
	NodeID        string            `json:"node_id"`
	Labels        map[string]string `json:"labels,omitempty"`         // zone / pool / slot / node (nodeSelectors)
	Capacity      int               `json:"capacity,omitempty"`       // max sandboxes (headroom fallback signal)
	BuildCapacity *BuildResources   `json:"build_capacity,omitempty"` // CPU/mem/storage build pool (§7.5)
	DataEndpoint  string            `json:"data_endpoint,omitempty"`  // host:port the router forwards data-plane to
	RuntimeDigest string            `json:"runtime_digest,omitempty"` // guest runtime identity (placement compat)
}

// BuildResources is a node's build resource pool (or a build's request), kept
// independent of sandbox memory because builds run in their own slice (§7.5).
type BuildResources struct {
	CPU     int   `json:"cpu,omitempty"`     // milli-cores
	Mem     int64 `json:"mem,omitempty"`     // bytes
	Storage int64 `json:"storage,omitempty"` // bytes
}

// Heartbeat is the node's periodic water-level report (cluster.md §5.1). Draining
// is set by node-side drain (node-resource.md §2.5) so placement excludes the node.
type Heartbeat struct {
	Zone       string          `json:"zone,omitempty"`
	Allocated  int64           `json:"allocated,omitempty"`  // memory allocated (bytes)
	Pool       int64           `json:"pool,omitempty"`       // allocatable pool (bytes)
	BuildAlloc *BuildResources `json:"build_alloc,omitempty"` // in-flight + reserved build usage
	Counts     int             `json:"counts,omitempty"`     // live sandbox count (headroom fallback)
	Draining   bool            `json:"draining,omitempty"`
}

// Command is a lifecycle / key primitive the registry sends the node (cluster.md
// §5.1). The node replies with a CmdAck(cmd_id) immediately (accepted/rejected)
// and reports the terminal sandbox state via the route stream; commands are
// idempotent by SID. Fields are populated per Kind.
type Command struct {
	CmdID    string `json:"cmd_id"`
	Kind     string `json:"kind"` // CmdCreate | CmdConnect | CmdDelete | CmdKey*
	SID      string `json:"sid,omitempty"`
	Group    string `json:"group,omitempty"`
	RouteKey string `json:"route_key,omitempty"`
	// create
	TemplateRef    string            `json:"template_ref,omitempty"`    // snapshot template ref (cold start = fast restore)
	KeyFingerprint string            `json:"key_fp,omitempty"`          // manifest-key fingerprint the node must already hold
	Config         map[string]string `json:"config,omitempty"`          // merged sandbox config (node default ⊕ group ⊕ create)
	MigrationToken string            `json:"migration_token,omitempty"` // SAVED one-step import + restore
	// key_put / key_drop
	ManifestKey string `json:"manifest_key,omitempty"` // hex; only on key_put
	ExpiresUnix int64  `json:"expires_unix,omitempty"` // lease expiry (key_put)
}

// CmdAck acknowledges a Command's receipt; the terminal outcome arrives via the
// route stream, not here.
type CmdAck struct {
	CmdID  string `json:"cmd_id"`
	Status string `json:"status"` // AckAccepted | AckRejected
	Reason string `json:"reason,omitempty"`
}

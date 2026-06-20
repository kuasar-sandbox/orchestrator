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
)

// Command kinds (Command.Kind) — the lifecycle + key primitives the registry
// drives the node with. The node executes via its existing e2b lifecycle (the
// command just carries the intent) and reports the terminal state on the route
// stream; a Reserve waits on that route event, not the ack.
const (
	CmdCreate   = "create"    // boot a sandbox (cold template restore, or migration import+restore)
	CmdConnect  = "connect"   // resume a node-local PAUSED sandbox
	CmdDelete   = "delete"    // destroy a sandbox (kill, or the SAVED two-phase reclaim step)
	CmdKeyPut   = "key_put"   // install a manifest-key lease (cluster.md §7.6)
	CmdKeyRenew = "key_renew" // renew a key lease
	CmdKeyDrop  = "key_drop"  // drop a key lease
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
	// key_put / key_renew / key_drop
	ManifestKey string `json:"manifest_key,omitempty"` // hex; only on key_put
	ExpiresUnix int64  `json:"expires_unix,omitempty"` // lease expiry (key_put / key_renew)
}

// CmdAck acknowledges a Command's receipt; the terminal outcome arrives via the
// route stream, not here.
type CmdAck struct {
	CmdID  string `json:"cmd_id"`
	Status string `json:"status"` // AckAccepted | AckRejected
	Reason string `json:"reason,omitempty"`
}

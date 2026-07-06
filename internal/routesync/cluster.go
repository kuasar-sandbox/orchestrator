package routesync

// Cluster node-link message types (node.md §10 / cluster.md). They extend the
// Msg union for the node <-> registry channel: the node DIALS the registry and is
// the route authority (its sandbox routes flow as Upsert/Delete + Bookmark, with
// Group/RouteKey set on each RouteEntry), while the registry
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

// NodeLinkPath is the HTTP path node-ctl conductor serve dials to open its node_link
// channel to the registry.
const NodeLinkPath = "/node-link/session"

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
	CmdKeyPut        = "key_put"        // install / renew a manifest-key lease (heartbeat refresh; cluster.md)
	CmdKeyDrop       = "key_drop"       // drop a key lease
	CmdBuildRegister = "build_register" // pre-provision a build on the node (registry-assigned ids, §7.5)
)

// TypeBuildEvent: node -> registry, a build's state transition (cluster.md);
// the registry converges the BuildStore (§6.1) + releases the build's reserved
// resources on a terminal state.
const TypeBuildEvent = "build_event"

// BuildEvent reports a build's state up the node-link (§5.1). State is one of
// registered/building/ready/error; TemplateID carries the persist id on ready.
type BuildEvent struct {
	BuildID    string `json:"build_id"`
	Group      string `json:"group"`
	State      string `json:"state"`
	TemplateID string `json:"template_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// CmdAck statuses.
const (
	AckAccepted = "accepted"
	AckRejected = "rejected"
)

// NodeRegister is the node's first up-frame on node-link: its identity + capacity,
// so the registry can place sandboxes (and later builds) on it and forward the
// data plane to it (cluster.md / §6.1).
type NodeRegister struct {
	NodeID         string            `json:"node_id"`
	Labels         map[string]string `json:"labels,omitempty"`          // zone / pool / slot / node (nodeSelectors)
	Capacity       int               `json:"capacity,omitempty"`        // max sandboxes (headroom signal)
	BuildCapacity  *BuildResources   `json:"build_capacity,omitempty"`  // CPU/mem/storage build pool (§7.5)
	DataEndpoint   string            `json:"data_endpoint,omitempty"`   // host:port the router forwards data-plane to
	RuntimeDigest  string            `json:"runtime_digest,omitempty"`  // guest runtime identity
	AcceptRedirect bool              `json:"accept_redirect,omitempty"` // node can reconnect to owner endpoints from Hello.Redirect
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
	CmdID    string `json:"cmd_id"`
	Kind     string `json:"kind"` // CmdCreate | CmdConnect | CmdDelete | CmdKey*
	SID      string `json:"sid,omitempty"`
	Group    string `json:"group,omitempty"`
	RouteKey string `json:"route_key,omitempty"`
	// create
	TemplateRef    string            `json:"template_ref,omitempty"` // snapshot template ref (cold start = fast restore)
	KeyFingerprint string            `json:"key_fp,omitempty"`       // manifest-key fingerprint the node must already hold
	Config         map[string]string `json:"config,omitempty"`       // merged sandbox config (node default ⊕ group ⊕ create)
	AccessToken    string            `json:"access_token,omitempty"` // MAC(auth_key,sid), supplied by registry
	// key_put / key_drop
	ManifestKeyType string `json:"manifest_key_type,omitempty"` // inline | ref
	ManifestKey     string `json:"manifest_key,omitempty"`      // hex; only on inline key_put
	ManifestKeyRef  string `json:"manifest_key_ref,omitempty"`  // provider ref; resolved out-of-band by node owner
	ExpiresUnix     int64  `json:"expires_unix,omitempty"`      // lease expiry (key_put)
	// build_register (§7.5): pre-provision a build with registry-assigned ids +
	// reserved resources. ImageRepo/RegistryAuth are the group's image-pull creds,
	// delivered WITH the build task and used transiently (never persisted on the node).
	BuildID        string          `json:"build_id,omitempty"`
	BuildResources *BuildResources `json:"build_resources,omitempty"`
	ImageRepo      string          `json:"image_repo,omitempty"`
	RegistryAuth   string          `json:"registry_auth,omitempty"` // docker config.json; transient
}

// CmdAck acknowledges a Command's receipt; the terminal outcome arrives via the
// route stream, not here.
type CmdAck struct {
	CmdID  string `json:"cmd_id"`
	Status string `json:"status"` // AckAccepted | AckRejected
	Reason string `json:"reason,omitempty"`
}

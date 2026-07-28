package routesync

// Cluster node-link message types (node.md §10 / cluster.md). They extend the
// Msg union for the node <-> registry channel: the node DIALS the registry and is
// the execution-state authority (its sandbox routes flow as ID-only
// Upsert/Delete + Bookmark), while the registry resolves cluster identity from
// its per-node ownership table and sends Commands the other way. The frame codec and
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
	ExcludeNodeIDs      []string          `json:"exclude_node_ids,omitempty"`
}

// PlaceResult is the placer's answer (NodeID set, or NoNode when nothing eligible).
type PlaceResult struct {
	ReqID                string            `json:"req_id"`
	NodeID               string            `json:"node_id,omitempty"`
	NoNode               bool              `json:"no_node,omitempty"`
	Error                string            `json:"error,omitempty"`
	TemplateRef          string            `json:"template_ref,omitempty"`
	TargetPort           int               `json:"target_port,omitempty"`
	Config               map[string]string `json:"config,omitempty"`
	APISecretFingerprint string            `json:"api_secret_fingerprint,omitempty"`
	ImageRepo            string            `json:"image_repo,omitempty"`
	RegistryAuth         string            `json:"registry_auth,omitempty"`
}

// SelectorPatch is the placer's placement projection for a group. NodeIDs is the
// explicit node set that should hold the group's API/manifest credential pair;
// Selectors carries the shuffle-effective selector projection. The pair is one
// atomic unit: both fingerprints and both typed secret carriers are present, or
// all credential fields are absent.
type SelectorPatch struct {
	Group                  string              `json:"group"`
	Selectors              []map[string]string `json:"selectors"`
	NodeIDs                []string            `json:"node_ids,omitempty"`
	APISecretFingerprint   string              `json:"api_secret_fingerprint,omitempty"`
	APISecretType          string              `json:"api_secret_type,omitempty"`
	APISecret              string              `json:"api_secret,omitempty"`
	APISecretRef           string              `json:"api_secret_ref,omitempty"`
	ManifestKeyFingerprint string              `json:"manifest_key_fingerprint,omitempty"`
	ManifestKeyType        string              `json:"manifest_key_type,omitempty"`
	ManifestKey            string              `json:"manifest_key,omitempty"`
	ManifestKeyRef         string              `json:"manifest_key_ref,omitempty"`
	ImportSourceID         string              `json:"import_source_id,omitempty"`
	ImportOwnerID          string              `json:"import_owner_id,omitempty"`
	ImportRunID            string              `json:"import_run_id,omitempty"`
	ImportTerm             uint64              `json:"import_term,omitempty"`
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
	CmdKeyPut        = "key_put"        // install / renew an API/manifest key-pair lease
	CmdKeyDrop       = "key_drop"       // drop a key lease
	CmdBuildRegister = "build_register" // pre-provision a build on the node (registry-assigned ids, §7.5)
)

// TypeBuildEvent: node -> registry, a build's state transition (cluster.md);
// the registry converges the BuildStore (§6.1) + releases the build's reserved
// resources on a terminal state.
const TypeBuildEvent = "build_event"

// BuildEvent reports a build's state up the node-link (§5.1). The node-link
// owner resolves cluster identity from its per-node build table.
type BuildEvent struct {
	BuildID    string `json:"build_id"`
	State      string `json:"state"`
	TemplateID string `json:"template_id,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// ClusterSandboxContext carries Registry-owned sandbox identity to a node. The
// node persists Group and RouteKey separately from user metadata, and stores the
// optional AuthSandboxID as the stable credential subject. It never derives any
// of these values from Command.SID.
type ClusterSandboxContext struct {
	Group         string `json:"group"`
	RouteKey      string `json:"route_key"`
	AuthSandboxID string `json:"auth_sandbox_id,omitempty"`
}

// CmdAck statuses.
const (
	AckAccepted = "accepted"
	AckRejected = "rejected"

	// MaxConnectTimeoutSeconds is the largest whole-second timeout that can be
	// converted to time.Duration without overflow.
	MaxConnectTimeoutSeconds int64 = (1<<63 - 1) / 1_000_000_000
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
	CmdID string `json:"cmd_id"`
	Kind  string `json:"kind"` // CmdCreate | CmdConnect | CmdDelete | CmdKey* | CmdBuildRegister
	SID   string `json:"sid,omitempty"`
	// create and lifecycle credential binding
	TemplateRef          string                 `json:"template_ref,omitempty"`           // snapshot template ref (cold start = fast restore)
	Profile              string                 `json:"profile,omitempty"`                // sandbox profile; also used by build_register
	APISecretFingerprint string                 `json:"api_secret_fingerprint,omitempty"` // create selects an installed pair; later commands match the existing row
	Config               map[string]string      `json:"config,omitempty"`                 // merged sandbox config (node default ⊕ group ⊕ create)
	Cluster              *ClusterSandboxContext `json:"cluster,omitempty"`                // Registry-owned group/route/auth identity
	MigrationToken       string                 `json:"migration_token,omitempty"`        // connect import when the exact target is absent
	TimeoutSeconds       int                    `json:"timeout_seconds,omitempty"`        // connect: positive requested lifetime applied before acknowledgement
	// key_put / key_drop
	APISecretType          string `json:"api_secret_type,omitempty"`          // inline | ref
	APISecret              string `json:"api_secret,omitempty"`               // hex; only on inline key_put
	APISecretRef           string `json:"api_secret_ref,omitempty"`           // provider ref
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint,omitempty"` // full fingerprint of manifest content key
	ManifestKeyType        string `json:"manifest_key_type,omitempty"`        // inline | ref
	ManifestKey            string `json:"manifest_key,omitempty"`             // hex; only on inline key_put
	ManifestKeyRef         string `json:"manifest_key_ref,omitempty"`         // provider ref
	ExpiresUnix            int64  `json:"expires_unix,omitempty"`             // lease expiry (key_put)
	// build_register (§7.5): pre-provision a build with registry-assigned ids +
	// reserved resources. ImageRepo/RegistryAuth are the group's image-pull creds,
	// delivered WITH the build task and used transiently (never persisted on the node).
	BuildID        string          `json:"build_id,omitempty"`
	BuildResources *BuildResources `json:"build_resources,omitempty"`
	ImageRepo      string          `json:"image_repo,omitempty"`
	RegistryAuth   string          `json:"registry_auth,omitempty"` // docker config.json; transient
}

// ConnectResult is the synchronous result of an accepted CmdConnect. It projects
// only the public service credentials stored in the sandbox business row; secret
// roots and their fingerprints never appear in the result.
type ConnectResult struct {
	NodeSandboxID      string `json:"node_sandbox_id"`
	TemplateID         string `json:"template_id"`
	Profile            string `json:"profile"`
	EnvdAccessToken    string `json:"envd_access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken string `json:"forward_access_token"`
}

// CmdAck acknowledges a Command's receipt. CmdConnect additionally returns its
// synchronously prepared result; asynchronous resume completion still arrives
// through the route stream.
type CmdAck struct {
	CmdID      string         `json:"cmd_id"`
	Status     string         `json:"status"` // AckAccepted | AckRejected
	Reason     string         `json:"reason,omitempty"`
	HTTPStatus int            `json:"http_status,omitempty"`
	Connect    *ConnectResult `json:"connect,omitempty"`
}

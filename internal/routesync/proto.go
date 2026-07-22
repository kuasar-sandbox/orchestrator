// Package routesync is the orchestrator's route-distribution protocol used in
// proxy_mode=external (and by any other subscriber, e.g. a platform agent). A
// subscriber registers on the orchestrator's config-socket plugin plane —
// PUT /internal/plugin/{id}/register — and that single long-lived, bidirectional
// h2c request carries the stream both ways:
//
//	subscriber -> orchestrator :  Register(caps) -> Wake(exact Binding) (route_wake only: data-plane
//	                                                                     traffic for a paused sandbox)
//	orchestrator -> subscriber :  Hello(policy)  -> Upsert* -> Bookmark -> Upsert/Delete (live deltas)
//
// The orchestrator is the route authority and the connection responder: it no longer
// dials anyone. The subscriber (the proxy master, or an observer) is the dialer +
// lease holder — the connection IS the registration. Closing it
// deregisters; a second registration with the same id evicts (and closes) the first.
//
// The initial route set is streamed one Upsert per sandbox, then a Bookmark marks
// "initial sync complete" — no materialized all-routes frame (bounded send-side
// memory at high sandbox density). The subscriber applies the stream against a sync
// epoch and, on the Bookmark, drops entries it did not see this stream (which
// recovers deletions that happened while it was disconnected.
//
// On a Wake the orchestrator resumes the sandbox (single-flight) and the resulting
// Upsert flows back down, unparking the proxy's held request. The wire is
// length-prefixed JSON frames (no gRPC/protobuf) — the same framing style as the rest
// of internal/configsock, extended to a continuous stream. Transport is h2c so the
// single request carries both directions full-duplex (golang.org/x/net/http2).
package routesync

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// Version is the protocol version exchanged in Hello/Register.
const Version = 4

// PluginRegisterPattern is the config-socket route pattern (Go 1.22 method+wildcard)
// a subscriber registers + opens its route stream on. PluginRegisterPath builds the
// concrete path the subscriber dials.
const PluginRegisterPattern = "PUT /internal/plugin/{id}/register"

const pluginPathPrefix = "/internal/plugin/"

// PluginRegisterPath is the registration path for a given plugin id.
func PluginRegisterPath(id string) string { return pluginPathPrefix + id + "/register" }

// Subscribe kinds (Register.Subscribe.Kind).
const (
	KindRoute     = "route"      // route stream only (observer)
	KindRouteWake = "route_wake" // route stream + this subscriber issues Wakes (a proxy)
)

// RouteEntry.State values (mirror internal/types.State string values).
const (
	StateRunning = "running"
	StatePaused  = "paused"
	StateDead    = "dead"
)

// Message types.
const (
	TypeRegister = "register" // subscriber -> orchestrator (caps; first up-frame)
	TypeHello    = "hello"    // orchestrator -> subscriber (carries Policy)
	TypeUpsert   = "upsert"   // orchestrator -> subscriber (one route added/changed)
	TypeDelete   = "delete"   // orchestrator -> subscriber (one route removed)
	TypeBookmark = "bookmark" // orchestrator -> subscriber (initial route stream complete; synced)
	TypeWake     = "wake"     // subscriber -> orchestrator (resume one exact execution)
)

// RouteEntry is the per-sandbox routing + auth state the orchestrator distributes
// so a proxy can serve the data plane on its own (no per-request callback). State
// "paused"/missing makes the proxy send a Wake; "running" lets it forward.
type RouteEntry struct {
	SandboxID          string `json:"sid"`
	NodeID             string `json:"node_id,omitempty"`
	NodeEpoch          uint64 `json:"node_epoch,omitempty"`
	RegistryGeneration string `json:"registry_generation,omitempty"`
	BindingDigest      string `json:"binding_digest,omitempty"`
	EventSeq           uint64 `json:"event_seq,omitempty"`
	Profile            string `json:"profile"`                // "e2b" | "bare"
	TemplateID         string `json:"template_id,omitempty"`  // for MMDS envID (proxy-served metadata)
	State              string `json:"state"`                  // "running" | "paused" | "dead"
	EnvdUDS            string `json:"envd_uds,omitempty"`     // e2b control port 49983
	CiUDS              string `json:"ci_uds,omitempty"`       // e2b code-interpreter port 49999
	FloatingIP         string `json:"floatingip,omitempty"`   // host-reachable addr for user ports
	AccessToken        string `json:"access_token,omitempty"` // envdAccessToken; X-Access-Token must match
	// TrafficAccessToken is the SDK compatibility token returned by create. It is
	// reported by the node route authority and preserved by route_link, but is not
	// used as the data-plane X-Access-Token.
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	// SnapshotLocation is "" for running/dead, else "local" (node-bound checkpoint
	// bundle — blocks a node drain unless migrated) or "remote" (uploaded, portable).
	// A subscriber (e.g. the platform agent) reads it to decide migration; the actual
	// MIGRATION_TOKEN is minted on demand by export-sandbox, never broadcast here.
	SnapshotLocation string `json:"snap_loc,omitempty"`
	// MmdsSecret is the per-sandbox MMDS signing key (hex), derived deterministically
	// from the manifest key + id (keys.MmdsSecret) so every proxy worker reads the
	// same key from the shared route view.
	MmdsSecret string `json:"mmds_secret,omitempty"`
}

func (r RouteEntry) HasExecutionFence() bool {
	return r.NodeID != "" || r.NodeEpoch != 0 || r.RegistryGeneration != "" || r.BindingDigest != "" || r.EventSeq != 0
}

func (r RouteEntry) ValidateExecutionFence() error {
	if r.SandboxID == "" {
		return errors.New("routesync: route entry requires a sandbox ID")
	}
	if r.HasExecutionFence() && (r.NodeID == "" || r.NodeEpoch == 0 || r.RegistryGeneration == "" ||
		r.BindingDigest == "" || r.EventSeq == 0) {
		return errors.New("routesync: managed route entry requires a complete execution fence")
	}
	return nil
}

// RouteDelete removes only the execution named by its fence. Standalone routes
// have no execution fence and use SandboxID alone; cluster-managed routes must
// carry the complete identity and event watermark.
type RouteDelete struct {
	SandboxID          string `json:"sid"`
	NodeID             string `json:"node_id,omitempty"`
	NodeEpoch          uint64 `json:"node_epoch,omitempty"`
	RegistryGeneration string `json:"registry_generation,omitempty"`
	BindingDigest      string `json:"binding_digest,omitempty"`
	EventSeq           uint64 `json:"event_seq,omitempty"`
}

func (d RouteDelete) HasExecutionFence() bool {
	return d.NodeID != "" || d.NodeEpoch != 0 || d.RegistryGeneration != "" || d.BindingDigest != "" || d.EventSeq != 0
}

func (d RouteDelete) Validate() error {
	if d.SandboxID == "" {
		return errors.New("routesync: route delete requires a sandbox ID")
	}
	if d.HasExecutionFence() && (d.NodeID == "" || d.NodeEpoch == 0 || d.RegistryGeneration == "" ||
		d.BindingDigest == "" || d.EventSeq == 0) {
		return errors.New("routesync: managed route delete requires a complete execution fence")
	}
	return nil
}

// RouteWake identifies the exact paused execution a proxy observed. A wake is
// only a hint to resume that execution; it must never authorize a resume after
// the node epoch, Registry History Generation, or system-owned Binding has changed.
type RouteWake struct {
	SandboxID          string `json:"sid"`
	NodeID             string `json:"node_id,omitempty"`
	NodeEpoch          uint64 `json:"node_epoch,omitempty"`
	RegistryGeneration string `json:"registry_generation,omitempty"`
	BindingDigest      string `json:"binding_digest,omitempty"`
}

// Policy is the operational policy the orchestrator pushes to a proxy at handshake
// (central control: the proxy need not be told these locally).
type Policy struct {
	Domain            string `json:"domain,omitempty"`
	AuthMode          string `json:"auth_mode,omitempty"`           // off | log | enforce
	ParkTimeoutMS     int    `json:"park_timeout_ms,omitempty"`     // hold a request awaiting route/resume
	RequireRouterMTLS bool   `json:"require_router_mtls,omitempty"` // external TCP ingress accepts only Router peers
}

// Msg is one wire message — a tagged union; exactly one payload field is set for a
// given Type.
type Msg struct {
	Type     string       `json:"type"`
	Hello    *Hello       `json:"hello,omitempty"`    // hello (orchestrator -> subscriber)
	Register *Register    `json:"register,omitempty"` // register (subscriber -> orchestrator, first up-frame)
	Route    *RouteEntry  `json:"route,omitempty"`    // upsert
	Delete   *RouteDelete `json:"delete,omitempty"`   // delete
	Wake     *RouteWake   `json:"wake,omitempty"`     // wake
	SID      string       `json:"sid,omitempty"`      // command target
	// Final cluster node-link variants. Node registration, load and durable facts
	// flow to the Holder; fenced commands and acknowledgements flow both ways.
	NodeReg        *NodeRegister          `json:"node_register,omitempty"`
	Load           *PlacementLoadSnapshot `json:"placement_load,omitempty"`
	Cmd            *Command               `json:"command,omitempty"`
	Ack            *CmdAck                `json:"cmd_ack,omitempty"`
	RevToken       string                 `json:"rev_token,omitempty"`
	FullSync       bool                   `json:"full_sync,omitempty"`       // bookmark follows a full snapshot, not an incremental replay
	ExecutionEvent *ExecutionEvent        `json:"execution_event,omitempty"` // durable Sandbox event
	EventAck       *EventAck              `json:"event_ack,omitempty"`       // registry -> node durable event acknowledgement
}

// Hello is the orchestrator's first down-frame; it carries the operational Policy.
type Hello struct {
	Version    int               `json:"version"`
	Policy     Policy            `json:"policy,omitempty"`
	ResumeFrom string            `json:"resume_from,omitempty"` // node-link subscriber request; empty means full sync
	Redirect   *NodeLinkRedirect `json:"redirect,omitempty"`
}

// Register is the subscriber's first up-frame: the capabilities it wants wired. The
// capabilities are independent — the orchestrator wires each on its own and does not
// enforce combinations (a proxy without subscribe, a subscribe without proxy, etc.
// are all the subscriber's own call).
type Register struct {
	Version   int        `json:"version"`
	Subscribe *Subscribe `json:"subscribe,omitempty"` // route stream; nil = lease only (no routes)
	Proxy     *Proxy     `json:"proxy,omitempty"`     // accepts proxyForwarder data-plane requests
	Mmds      bool       `json:"mmds,omitempty"`      // serves MMDS (the per-sandbox secret ships on every entry)
	// ResumeFrom (opt-in) asks the authority to replay the route changelog strictly
	// after this token instead of a full re-sync. The token is intentionally a
	// string so a node owner can embed a source fingerprint and reject incremental
	// resume across node restart / instance boundaries.
	ResumeFrom string `json:"resume_from,omitempty"`
}

// Subscribe selects the route-stream flavor.
type Subscribe struct {
	Kind string `json:"kind"` // KindRoute | KindRouteWake
}

// Proxy declares the UDS the orchestrator's proxyForwarder forwards
// data-plane requests to (this subscriber serves them from its synced table).
type Proxy struct {
	Socket Socket `json:"socket"`
}

type Socket struct {
	Path string `json:"path"`
}

// subscribes reports whether the orchestrator should stream routes to this plugin.
func (r Register) subscribes() bool { return r.Subscribe != nil }

// handlesWake reports whether the subscriber issues Wakes (route_wake), so the
// orchestrator acts on inbound Wake frames.
func (r Register) handlesWake() bool { return r.Subscribe != nil && r.Subscribe.Kind == KindRouteWake }

// SubscribeKind is the declared subscribe kind, or "" if not subscribing (for logs).
func (r Register) SubscribeKind() string {
	if r.Subscribe == nil {
		return ""
	}
	return r.Subscribe.Kind
}

// Event is a route change the orchestrator publishes to the route-sync client,
// which fans it out to every connected proxy as an Upsert/Delete.
type Event struct {
	Kind   string      // TypeUpsert | TypeDelete
	Route  RouteEntry  // Upsert
	Delete RouteDelete // Delete
}

const maxFrame = 1 << 20 // 1 MiB — generous bound for a single route/wake frame (no all-routes frame)

// WriteMsg writes a length-prefixed JSON frame ([4B LE len][json]).
func WriteMsg(w io.Writer, m *Msg) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(b) > maxFrame {
		return errors.New("routesync: message too large")
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// ReadMsg reads one length-prefixed JSON frame.
func ReadMsg(r io.Reader) (*Msg, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > maxFrame {
		return nil, errors.New("routesync: bad frame length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var m Msg
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

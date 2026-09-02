// Package routesync is the orchestrator's route-distribution protocol used by
// the independent proxy (and by any other subscriber, e.g. a platform agent). A
// subscriber registers on the orchestrator's config-socket plugin plane —
// PUT /internal/plugin/{id}/register — and that single long-lived, bidirectional
// h2c request carries the stream both ways:
//
//	subscriber -> orchestrator :  Register(caps) -> Wake/RouteBarrierAck
//	orchestrator -> subscriber :  Hello(policy) -> Upsert* -> Bookmark -> Upsert/Delete/RouteBarrier
//
// The orchestrator is the route authority and the connection responder: it no longer
// dials anyone. The subscriber (the proxy master, or an observer) is the dialer +
// lease holder — the connection IS the registration. Closing it
// deregisters; a second registration with the same id evicts (and closes) the first.
//
// The initial route set is streamed one Upsert per sandbox, then a Bookmark marks
// "initial sync complete" — no materialized all-routes frame (bounded send-side
// memory at high sandbox density). The subscriber applies the stream against a sync
// generation and, on the Bookmark, drops entries it did not see this stream (which
// recovers deletions that happened while it was disconnected.
//
// After admission, a Wake for a known paused route makes the orchestrator resume
// the sandbox (single-flight) and the resulting Upsert flows back down, unparking
// the proxy's held request. Missing routes only wait for passive propagation. The
// wire is length-prefixed JSON frames (no gRPC/protobuf) — the same framing style
// as the rest of internal/configsock, extended to a continuous stream. Transport
// is h2c so the single request carries both directions full-duplex
// (golang.org/x/net/http2).
package routesync

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
)

// Version is the protocol version announced by the authority in Hello.
// Version 3 was the hard cut to stable_id. Version 4 replaced the Snapshot-
// specific route location field with kind-orthogonal artifact_location. Version
// 5 splits node API and data endpoints and removes the proxy forwarding socket;
// mixed peers must fail closed.
const Version = 5

// PluginRegisterPattern is the config-socket route pattern (Go 1.22 method+wildcard)
// a subscriber registers + opens its route stream on. PluginRegisterPath builds the
// concrete path the subscriber dials.
const PluginRegisterPattern = "PUT /internal/plugin/{id}/register"

const pluginPathPrefix = "/internal/plugin/"

// PluginRegisterPath is the registration path for a given plugin id.
func PluginRegisterPath(id string) string { return pluginPathPrefix + id + "/register" }

// ProxyPluginID is the one trusted independent Proxy registration identity.
const ProxyPluginID = "proxy"

// Subscribe kinds (Register.Subscribe.Kind).
const (
	KindRoute     = "route"      // route stream only (observer)
	KindRouteWake = "route_wake" // route stream + this subscriber issues Wakes (a proxy)
	KindRegistry  = "registry"   // cluster registry subscribing to a node's routes (node.md §10)
)

// RouteEntry.State values (mirror internal/types.State string values).
const (
	StateStarting = "starting"
	StateRunning  = "running"
	StatePaused   = "paused"
	StateDead     = "dead"
)

// Message types.
const (
	TypeRegister = "register" // subscriber -> orchestrator (caps; first up-frame)
	TypeHello    = "hello"    // orchestrator -> subscriber (carries Policy)
	TypeUpsert   = "upsert"   // orchestrator -> subscriber (one route added/changed)
	TypeDelete   = "delete"   // orchestrator -> subscriber (one route removed)
	TypeBookmark = "bookmark" // orchestrator -> subscriber (initial route stream complete; synced)
	TypeWake     = "wake"     // subscriber -> orchestrator (resume this sandbox)
	// RouteBarrier follows one or more route mutations on the same ordered down
	// stream. The subscriber ACKs it only after every preceding Upsert has been
	// applied successfully to its serving view.
	TypeRouteBarrier    = "route_barrier"     // orchestrator -> subscriber
	TypeRouteBarrierAck = "route_barrier_ack" // subscriber -> orchestrator
)

// RouteEntry is the per-sandbox routing + auth state the orchestrator distributes
// so a proxy can serve the data plane on its own (no per-request callback). State
// A credential-authorized "paused" route may make the proxy send a Wake;
// "starting" waits passively and "running" lets it forward.
type RouteEntry struct {
	SandboxID  string `json:"sid"`
	Profile    string `json:"profile"`               // "e2b" | "bare"
	TemplateID string `json:"template_id,omitempty"` // for MMDS envID (proxy-served metadata)
	State      string `json:"state"`                 // "starting" | "running" | "paused" | "dead"
	EnvdUDS    string `json:"envd_uds,omitempty"`    // e2b control port 49983
	CiUDS      string `json:"ci_uds,omitempty"`      // e2b code-interpreter port 49999
	FloatingIP string `json:"floatingip,omitempty"`  // host-reachable addr for user ports

	// Credential material is copied from the sandbox business record. Trusted
	// proxy/registry subscribers use the roots and fingerprints for local request
	// authentication; data-plane forwarding selects EnvdAccessToken for the e2b
	// control ports and ForwardAccessToken for other forwarded ports.
	StableID               string `json:"stable_id,omitempty"`
	APISecret              string `json:"api_secret,omitempty"`
	APISecretFingerprint   string `json:"api_secret_fingerprint,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint,omitempty"`
	ServiceSecret          string `json:"service_secret,omitempty"`
	EnvdAccessToken        string `json:"envd_access_token,omitempty"`
	TrafficAccessToken     string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken     string `json:"forward_access_token,omitempty"`
	// ArtifactLocation is "" when the row owns no ResumeSource, "local" for a
	// node-bound E/S capture, or "remote" for a portable E/S reference. It is
	// independent of lifecycle state because a running row may retain ownership.
	// A subscriber (e.g. the platform agent) reads it to decide migration; the actual
	// MIGRATION_TOKEN is minted on demand by export-sandbox, never broadcast here.
	ArtifactLocation string `json:"artifact_location,omitempty"`
	// MmdsSecret is the per-sandbox MMDS signing key (hex), derived deterministically
	// from the manifest key + id (keys.MmdsSecret) so every proxy worker reads the
	// same key from the shared route view.
	MmdsSecret string `json:"mmds_secret,omitempty"`
	// RunID is the current launch/resume incarnation used by MMDSv2 tokens.
	RunID string `json:"run_id,omitempty"`
	// MMDSRoutes is the stable routes-only declaration. It remains outside the
	// fixed-layout proxy SHM and is projected only to a trusted MMDS proxy.
	MMDSRoutes string `json:"mmds_routes,omitempty"`
	// MMDSRouteSecretValues contains the current opaque secret route values.
	// A pointer to an empty map means the store was read successfully and no
	// values exist; nil means unavailable/not projected. It is never written to SHM.
	MMDSRouteSecretValues *MMDSRouteSecretValues `json:"mmds_route_secret_values,omitempty"`
}

// MMDSRouteSecretValues is pointer-wrapped in RouteEntry so RouteEntry remains
// comparable for the fixed-layout route table while JSON still encodes the
// field directly as an object: nil => omitted, pointer-to-empty-map => {}.
type MMDSRouteSecretValues map[string][]byte

// Policy is the operational policy the orchestrator pushes to a proxy at handshake
// (central control: the proxy need not be told these locally).
type Policy struct {
	Domain        string           `json:"domain,omitempty"`
	AuthMode      string           `json:"auth_mode,omitempty"`       // off | log | enforce
	ParkTimeoutMS int              `json:"park_timeout_ms,omitempty"` // hold a request awaiting route/resume
	MMDS          *MMDSProxyPolicy `json:"mmds,omitempty"`
}

// MMDSProxyPolicy is conductor-owned Proxy configuration delivered in
// Hello. Services maps an operator name to its validated unix:// endpoint.
type MMDSProxyPolicy struct {
	Enabled  bool              `json:"enabled"`
	Listen   string            `json:"listen,omitempty"`
	Services map[string]string `json:"services,omitempty"`
}

// Msg is one wire message — a tagged union; exactly one payload field is set for a
// given Type.
type Msg struct {
	Type      string      `json:"type"`
	Hello     *Hello      `json:"hello,omitempty"`      // hello (orchestrator -> subscriber)
	Register  *Register   `json:"register,omitempty"`   // register (subscriber -> orchestrator, first up-frame)
	Route     *RouteEntry `json:"route,omitempty"`      // upsert
	SID       string      `json:"sid,omitempty"`        // delete | wake | command target
	BarrierID string      `json:"barrier_id,omitempty"` // route_barrier | route_barrier_ack
	// Cluster node-link variants (node.md §10): node_register / heartbeat / cmd_ack
	// flow node -> registry; command flows registry -> node; rev stamps down events.
	NodeReg  *NodeRegister `json:"node_register,omitempty"`
	Beat     *Heartbeat    `json:"heartbeat,omitempty"`
	Cmd      *Command      `json:"command,omitempty"`
	Ack      *CmdAck       `json:"cmd_ack,omitempty"`
	Rev      int64         `json:"rev,omitempty"` // per-shard monotonic revision for resume_from (§5.3)
	RevToken string        `json:"rev_token,omitempty"`
	FullSync bool          `json:"full_sync,omitempty"`   // bookmark follows a full snapshot, not an incremental replay
	Build    *BuildEvent   `json:"build_event,omitempty"` // node -> registry build state (§5.1/§7.5)
}

// Hello is the orchestrator's first down-frame; it carries the operational Policy.
type Hello struct {
	Version    int               `json:"version"`
	Policy     Policy            `json:"policy,omitempty"`
	ResumeFrom string            `json:"resume_from,omitempty"` // node-link subscriber request; empty means full sync
	Redirect   *NodeLinkRedirect `json:"redirect,omitempty"`
}

// ValidateHello enforces the single supported protocol version before a client
// processes route or command frames from the session.
func ValidateHello(m *Msg) error {
	if m == nil || m.Type != TypeHello || m.Hello == nil {
		return errors.New("routesync: expected hello frame")
	}
	if m.Hello.Version != Version {
		return fmt.Errorf("routesync: protocol version mismatch: got %d, want %d", m.Hello.Version, Version)
	}
	return nil
}

// Register is the subscriber's first up-frame: the capabilities it wants wired. The
// capabilities are independent — the orchestrator wires each on its own and does not
// enforce combinations (a proxy without subscribe, a subscribe without proxy, etc.
// are all the subscriber's own call).
type Register struct {
	Subscribe *Subscribe `json:"subscribe,omitempty"` // route stream; nil = lease only (no routes)
	Proxy     *Proxy     `json:"proxy,omitempty"`     // trusted independent proxy registration marker and stats capability
	Mmds      bool       `json:"mmds,omitempty"`      // trusted proxy requests MMDS routes/values + policy projection
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

// Proxy marks the trusted independent proxy registration and optionally exposes
// its master-only traffic stats socket.
type Proxy struct {
	StatsSocket *Socket `json:"stats_socket,omitempty"`
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
	Kind  string     // TypeUpsert | TypeDelete | TypeRouteBarrier
	Route RouteEntry // Upsert
	SID   string     // Delete
	// BarrierID is ephemeral stream coordination. It is never appended to the
	// durable route changelog or projected into a RouteEntry.
	BarrierID string // RouteBarrier
}

const maxFrame = 1 << 20 // 1 MiB — generous bound for a single route/wake frame (no all-routes frame)

// WriteMsg writes a length-prefixed JSON frame ([4B LE len][json]).
func WriteMsg(w io.Writer, m *Msg) error {
	if err := validateMessageLimits(m); err != nil {
		return err
	}
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

// ValidateMessage applies the exact encoded frame limit without writing. It is
// used at admission boundaries to avoid accepting an object whose route upsert
// can never be published.
func ValidateMessage(m *Msg) error {
	if err := validateMessageLimits(m); err != nil {
		return err
	}
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(b) > maxFrame {
		return errors.New("routesync: message too large")
	}
	return nil
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
	if err := validateMessageLimits(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func validateMessageLimits(m *Msg) error {
	if m != nil && m.Cmd != nil && len(m.Cmd.MigrationToken) > migrationtoken.MaxWireSize {
		return migrationtoken.ErrTokenTooLarge
	}
	return nil
}

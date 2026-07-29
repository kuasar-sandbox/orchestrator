// Package routesync is the orchestrator's route-distribution protocol used in
// proxy_mode=external (and by any other subscriber, e.g. a platform agent). A
// subscriber registers on the orchestrator's config-socket plugin plane —
// PUT /internal/plugin/{id}/register — and that single long-lived, bidirectional
// h2c request carries the stream both ways:
//
//	subscriber -> orchestrator :  Register(caps) -> Wake(sid)          (route_wake only: data-plane
//	                                                                    traffic for a missing/paused sandbox)
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
// generation and, on the Bookmark, drops entries it did not see this stream (which
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

	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
)

// Version is the protocol version exchanged in Hello/Register.
const Version = 1

// PluginRegisterPattern is the config-socket route pattern (Go 1.22 method+wildcard)
// a subscriber registers + opens its route stream on. PluginRegisterPath builds the
// concrete path the subscriber dials.
const PluginRegisterPattern = "PUT /internal/plugin/{id}/register"

const pluginPathPrefix = "/internal/plugin/"

// PluginRegisterPath is the registration path for a given plugin id.
func PluginRegisterPath(id string) string { return pluginPathPrefix + id + "/register" }

// ProxyPluginID is the well-known plugin registration id the external-mode
// proxy master (cmd/node-ctl/proxy.go's runProxyMaster) always registers
// under. A shared constant (rather than each side hard-coding its own copy
// of "proxy") so the config-socket's MMDSSecrets-capability authorization
// (internal/configsock's handlePluginRegister) and the proxy's own
// registration can never drift apart -- that check is part of what stands
// between an ordinary route observer and live MMDS secret plaintext, so it
// must name the exact same identity the real proxy uses, not a
// coincidentally-matching literal.
const ProxyPluginID = "proxy"

// Subscribe kinds (Register.Subscribe.Kind).
const (
	KindRoute     = "route"      // route stream only (observer)
	KindRouteWake = "route_wake" // route stream + this subscriber issues Wakes (a proxy)
	KindRegistry  = "registry"   // cluster registry subscribing to a node's routes (node.md §10)
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
	TypeWake     = "wake"     // subscriber -> orchestrator (resume this sandbox)
)

// RouteEntry is the per-sandbox routing + auth state the orchestrator distributes
// so a proxy can serve the data plane on its own (no per-request callback). State
// "paused"/missing makes the proxy send a Wake; "running" lets it forward.
type RouteEntry struct {
	SandboxID  string `json:"sid"`
	Profile    string `json:"profile"`               // "e2b" | "bare"
	TemplateID string `json:"template_id,omitempty"` // for MMDS envID (proxy-served metadata)
	State      string `json:"state"`                 // "running" | "paused" | "dead"
	EnvdUDS    string `json:"envd_uds,omitempty"`    // e2b control port 49983
	CiUDS      string `json:"ci_uds,omitempty"`      // e2b code-interpreter port 49999
	FloatingIP string `json:"floatingip,omitempty"`  // host-reachable addr for user ports

	// Credential material is copied from the sandbox business record. Trusted
	// proxy/registry subscribers use the roots and fingerprints for local request
	// authentication; data-plane forwarding selects EnvdAccessToken for the e2b
	// control ports and ForwardAccessToken for other forwarded ports.
	AuthSandboxID          string `json:"auth_sandbox_id,omitempty"`
	APISecret              string `json:"api_secret,omitempty"`
	APISecretFingerprint   string `json:"api_secret_fingerprint,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint,omitempty"`
	ServiceSecret          string `json:"service_secret,omitempty"`
	EnvdAccessToken        string `json:"envd_access_token,omitempty"`
	TrafficAccessToken     string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken     string `json:"forward_access_token,omitempty"`
	// SnapshotLocation is "" for running/dead, else "local" (node-bound checkpoint
	// bundle — blocks a node drain unless migrated) or "remote" (uploaded, portable).
	// A subscriber (e.g. the platform agent) reads it to decide migration; the actual
	// MIGRATION_TOKEN is minted on demand by export-sandbox, never broadcast here.
	SnapshotLocation string `json:"snap_loc,omitempty"`
	// MmdsSecret is the per-sandbox MMDS signing key (hex), derived deterministically
	// from the manifest key + id (keys.MmdsSecret) so every proxy worker reads the
	// same key from the shared route view.
	MmdsSecret string `json:"mmds_secret,omitempty"`
	// MMDSRoutes is the canonical kuasar-sandbox.mmds specification JSON (see
	// internal/sandboxcfg.ExtractMMDS), empty for the common case of a sandbox
	// with no specified routes. Only static route bodies ride here today, which
	// are not secret data; a secret-route backend will need a separate,
	// access-gated channel rather than riding on this field. A proxy master
	// consuming this holds it in ordinary process memory, NOT the fixed-layout
	// proxyshm mmap table, since this field is bounded but variable-length (see
	// internal/mmdsrpc).
	MMDSRoutes string `json:"mmds_routes,omitempty"`
	// MMDSSecrets is sid's decrypted MMDS secret-value blob (internal/store's
	// GetMMDSSecretBlob JSON), empty for the common case of no configured
	// secret values. Unlike MMDSRoutes (static bodies, not secret), this field
	// carries real secret plaintext, so — unlike every other field on this
	// struct — it is NOT synced unconditionally: writeEvent/Range clear it
	// per-subscriber unless the subscriber registered with MMDSSecrets: true
	// (see Register.MMDSSecrets), so only a subscriber prepared to serve MMDS
	// secrets ever receives it over the wire.
	MMDSSecrets string `json:"mmds_secrets,omitempty"`
	// RunID is the sandbox's current run incarnation (the systemd runner
	// instance id assigned at launch/resume). Not secret, but not synced
	// unconditionally to every subscriber's own external interpretation
	// either -- it exists on the wire so a proxy can bind a minted MMDS
	// token to "this specific incarnation" and reject a token replayed
	// after pause/resume (which assigns a fresh RunID), matching the
	// internal-mode orchestrator's own live view of the same field.
	RunID string `json:"run_id,omitempty"`
}

// Policy is the operational policy the orchestrator pushes to a proxy at handshake
// (central control: the proxy need not be told these locally).
type Policy struct {
	Domain        string `json:"domain,omitempty"`
	AuthMode      string `json:"auth_mode,omitempty"`       // off | log | enforce
	ParkTimeoutMS int    `json:"park_timeout_ms,omitempty"` // hold a request awaiting route/resume
	// MMDSParkTimeoutMS bounds how long a worker's MMDSRoute call parks on a
	// specified-but-never-configured secret (revision==0) before returning
	// absent -- the external-mode mirror of mmds.routes.secret.park_timeout,
	// pushed as node policy so internal/external mode guest-observable wait
	// behavior stays consistent without needing the value in the tenant's
	// MMDS specification.
	MMDSParkTimeoutMS int `json:"mmds_park_timeout_ms,omitempty"`
}

// Msg is one wire message — a tagged union; exactly one payload field is set for a
// given Type.
type Msg struct {
	Type     string      `json:"type"`
	Hello    *Hello      `json:"hello,omitempty"`    // hello (orchestrator -> subscriber)
	Register *Register   `json:"register,omitempty"` // register (subscriber -> orchestrator, first up-frame)
	Route    *RouteEntry `json:"route,omitempty"`    // upsert
	SID      string      `json:"sid,omitempty"`      // delete | wake | command target
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

// Register is the subscriber's first up-frame: the capabilities it wants wired. The
// capabilities are independent — the orchestrator wires each on its own and does not
// enforce combinations (a proxy without subscribe, a subscribe without proxy, etc.
// are all the subscriber's own call).
type Register struct {
	Subscribe *Subscribe `json:"subscribe,omitempty"` // route stream; nil = lease only (no routes)
	Proxy     *Proxy     `json:"proxy,omitempty"`     // accepts proxyForwarder data-plane requests
	Mmds      bool       `json:"mmds,omitempty"`      // serves MMDS (the per-sandbox secret ships on every entry)
	// MMDSSecrets opts this subscriber into receiving RouteEntry.MMDSSecrets
	// (real secret plaintext) on its route stream -- unlike every other
	// Register capability, gated per-subscriber at write time (writeEvent /
	// Range) rather than always carried. A subscriber that isn't a real proxy
	// prepared to serve MMDS secrets should leave this false; it simply never
	// sees the field, not merely asked politely to ignore it.
	MMDSSecrets bool `json:"mmds_secrets,omitempty"`
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
	Kind  string     // TypeUpsert | TypeDelete
	Route RouteEntry // Upsert
	SID   string     // Delete
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

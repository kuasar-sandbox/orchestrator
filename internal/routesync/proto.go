// Package routesync is the orchestrator<->proxy route-distribution protocol used
// in proxy_mode=external. The orchestrator (the route authority) dials each proxy
// worker's UDS and runs a single long-lived, bidirectional stream over h2c:
//
//	orchestrator -> proxy :  Hello(policy) -> Snapshot(all routes) -> Upsert/Delete (deltas)
//	proxy -> orchestrator :  HelloAck       -> Wake(sid)            (data-plane traffic for a
//	                                                                 missing/paused sandbox)
//
// On a Wake the orchestrator resumes the sandbox (single-flight) and the resulting
// Upsert flows back down, unparking the proxy's held request. The wire is
// length-prefixed JSON frames (no gRPC/protobuf) — the same framing style as
// internal/configsock, extended to a continuous stream. Transport is h2c so a
// single request carries both directions full-duplex (golang.org/x/net/http2,
// already a dependency).
package routesync

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// Version is the protocol version exchanged in Hello/HelloAck.
const Version = 1

// SyncHeader marks the bidi route-sync request on the proxy's UDS so it is routed
// to the stream handler rather than the data-forward (fallback) data-plane handler.
const SyncHeader = "X-Orch-Routesync"

// SyncPath is the request path the orchestrator dials for the route-sync stream.
const SyncPath = "/routesync"

// RouteEntry.State values (mirror internal/types.State string values).
const (
	StateRunning = "running"
	StatePaused  = "paused"
	StateDead    = "dead"
)

// Message types.
const (
	TypeHello    = "hello"     // orchestrator -> proxy (carries Policy)
	TypeHelloAck = "hello_ack" // proxy -> orchestrator
	TypeSnapshot = "snapshot"  // orchestrator -> proxy (full route set; replace-all)
	TypeUpsert   = "upsert"    // orchestrator -> proxy (one route added/changed)
	TypeDelete   = "delete"    // orchestrator -> proxy (one route removed)
	TypeWake     = "wake"      // proxy -> orchestrator (resume this sandbox)
)

// RouteEntry is the per-sandbox routing + auth state the orchestrator distributes
// so a proxy can serve the data plane on its own (no per-request callback). State
// "paused"/missing makes the proxy send a Wake; "running" lets it forward.
type RouteEntry struct {
	SandboxID   string `json:"sid"`
	Profile     string `json:"profile"`                // "e2b" | "bare"
	State       string `json:"state"`                  // "running" | "paused" | "dead"
	EnvdUDS     string `json:"envd_uds,omitempty"`     // e2b control port 49983
	CiUDS       string `json:"ci_uds,omitempty"`       // e2b code-interpreter port 49999
	FloatingIP  string `json:"floatingip,omitempty"`   // host-reachable addr for user ports
	AccessToken string `json:"access_token,omitempty"` // envdAccessToken; X-Access-Token must match
}

// Policy is the operational policy the orchestrator pushes to a proxy at handshake
// (central control: the proxy need not be told these locally).
type Policy struct {
	Domain        string `json:"domain,omitempty"`
	AuthMode      string `json:"auth_mode,omitempty"`       // off | log | enforce
	ParkTimeoutMS int    `json:"park_timeout_ms,omitempty"` // hold a request awaiting route/resume
}

// Msg is one wire message — a tagged union; exactly one payload field is set for a
// given Type.
type Msg struct {
	Type   string       `json:"type"`
	Hello  *Hello       `json:"hello,omitempty"`
	Routes []RouteEntry `json:"routes,omitempty"` // snapshot
	Route  *RouteEntry  `json:"route,omitempty"`  // upsert
	SID    string       `json:"sid,omitempty"`    // delete | wake
}

// Hello is the first frame each side sends. The orchestrator's carries the Policy.
type Hello struct {
	Version int    `json:"version"`
	Role    string `json:"role"` // "orchestrator" | "proxy"
	Policy  Policy `json:"policy,omitempty"`
}

// Event is a route change the orchestrator publishes to the route-sync client,
// which fans it out to every connected proxy as an Upsert/Delete.
type Event struct {
	Kind  string     // TypeUpsert | TypeDelete
	Route RouteEntry // Upsert
	SID   string     // Delete
}

const maxFrame = 16 << 20 // 16 MiB — a full snapshot of all sandboxes fits in one frame

// writeMsg writes a length-prefixed JSON frame ([4B LE len][json]).
func writeMsg(w io.Writer, m *Msg) error {
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

// readMsg reads one length-prefixed JSON frame.
func readMsg(r io.Reader) (*Msg, error) {
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

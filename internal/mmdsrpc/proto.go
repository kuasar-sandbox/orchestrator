// Package mmdsrpc is the external-mode worker↔master RPC channel for MMDS
// endpoints: each proxy worker holds one inherited socketpair connection to
// the proxy master and multiplexes
// concurrent guest requests over it as EndpointRequest/EndpointResponse
// frames, with EndpointCancel for cancellation. There is no prior generic
// multiplexed-RPC substrate in this repo (the existing worker↔master pipes
// — wake/notify/metrics — are narrow, single-purpose, one-directional
// signals; see cmd/node-ctl/proxy.go), so this package builds one from
// scratch, reusing internal/routesync's [4B LE len][JSON] frame idiom
// (that package's own WriteMsg/ReadMsg are typed to routesync.Msg and
// cannot be reused directly for a different message shape, so the ~15-line
// codec below duplicates the same wire format rather than the code).
package mmdsrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// defaultMaxFrame bounds one frame when the caller passes maxFrame<=0 — the
// same absolute ceiling as internal/routesync.maxFrame (1 MiB), comfortably
// above the ~512KiB default max_worker_rpc_frame_bytes node policy. Actual
// Client/Server instances take max_worker_rpc_frame_bytes as their own
// maxFrame (clamped to this ceiling), so the node-policy value is honored
// rather than silently overridden by a fixed constant.
const defaultMaxFrame = 1 << 20

const (
	typeRequest  = "request"
	typeCancel   = "cancel"
	typeResponse = "response"
)

// wireMsg is the one frame shape both directions use — a tagged union
// exactly like routesync.Msg.
type wireMsg struct {
	Type      string            `json:"type"`
	RequestID uint64            `json:"request_id"`
	Request   *EndpointRequest  `json:"request,omitempty"`
	Response  *EndpointResponse `json:"response,omitempty"`
}

// EndpointRequest asks the master to resolve and serve a guest path for a
// sandbox. The worker never sends a backend name or URL — only the exact
// (sandbox_id,path) the master itself resolves against its own endpoint
// table; the master never accepts a backend name or URL from the worker.
type EndpointRequest struct {
	SandboxID      string `json:"sandbox_id"`
	Path           string `json:"path"`
	DeadlineUnixMS int64  `json:"deadline_unix_ms,omitempty"`
}

// EndpointResponse is the master's answer: a combined resolve+serve result
// (not a bare lookup) — see Client's doc comment for why the two are
// collapsed into one round trip here even though
// internal/mmds.EndpointAuthority exposes them as separate Lookup/
// ServeStore/ServeRelay calls.
type EndpointResponse struct {
	Found       bool   `json:"found"`
	Name        string `json:"name,omitempty"`
	BackendType string `json:"backend_type,omitempty"` // "store" | "relay"
	Present     bool   `json:"present,omitempty"`      // store: value present; relay: upstream classification obtained
	Status      int    `json:"status,omitempty"`       // relay only: the classified HTTP status
	ContentType string `json:"content_type,omitempty"`
	Body        []byte `json:"body,omitempty"`
	Revision    int64  `json:"revision,omitempty"` // store only
	// ErrorCode, when non-empty, means the master itself failed to resolve
	// or serve the request (as opposed to Found/Present=false, a legitimate
	// negative result) — the worker must map this to 503/504, never treat
	// it as "not found". One of the ErrCode* constants below.
	ErrorCode string `json:"error_code,omitempty"`
}

// ErrorCode values EndpointResponse.ErrorCode carries — a bounded enum, not
// free text, so the worker can classify without string-matching arbitrary
// error messages.
const (
	ErrCodeWorkerInflightLimit = "worker_inflight_limit" // per-connection inflight cap exceeded
	ErrCodeUnavailable         = "unavailable"           // master's endpoint table itself is unavailable
)

// clampMaxFrame normalizes a configured max_worker_rpc_frame_bytes value:
// <=0 falls back to defaultMaxFrame; anything above defaultMaxFrame is
// clamped down to it (the absolute ceiling internal/routesync's own framing
// uses, kept consistent across both packages' wire formats).
func clampMaxFrame(n int) int {
	if n <= 0 || n > defaultMaxFrame {
		return defaultMaxFrame
	}
	return n
}

func writeFrame(w io.Writer, m *wireMsg, maxFrame int) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(b) > maxFrame {
		return errors.New("mmdsrpc: message too large")
	}
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func readFrame(r io.Reader, maxFrame int) (*wireMsg, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > uint32(maxFrame) {
		return nil, errors.New("mmdsrpc: bad frame length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var m wireMsg
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

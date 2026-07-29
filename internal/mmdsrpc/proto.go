// Package mmdsrpc is the proxy master <-> proxy worker framed RPC used in
// proxy_mode=external to resolve a tenant-specified kuasar-sandbox.mmds route
// for a guest request. Workers run as separate processes (in their
// own network namespace) and cannot share the master's in-heap
// proxyshm.MMDSRoutes store directly, unlike the fixed-layout proxyshm mmap
// table other route data rides on -- a per-sandbox MMDS specification is bounded
// but variable-length (up to mmds.routes.max_namespace_bytes), so it is kept
// out of that mmap table entirely (see proxyshm.MMDSRoutes's doc comment) and
// fetched over this RPC instead, one inherited AF_UNIX SOCK_STREAM socketpair
// per worker, alongside the existing wake/notify/metrics pipes
// (cmd/node-ctl/proxy.go).
//
// The wire is the same length-prefixed JSON framing idiom as
// internal/routesync, but hand-rolled here rather than shared: routesync's
// WriteMsg/ReadMsg are typed to routesync.Msg.
package mmdsrpc

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// maxFrame bounds a single request/response frame. A response body is bounded
// well below this by the caller (a static route's data is capped by node
// policy, default 16KiB); this is a generous transport-level ceiling, not the
// content-size policy.
const maxFrame = 1 << 20 // 1 MiB

// EndpointRequest asks the master to resolve one guest-visible MMDS path for a
// specific sandbox. RequestID scopes the response on a connection carrying
// multiple concurrent in-flight requests; DeadlineUnixMS is optional.
type EndpointRequest struct {
	RequestID      uint64 `json:"id"`
	SandboxID      string `json:"sid"`
	Path           string `json:"path"`
	DeadlineUnixMS int64  `json:"deadline_ms,omitempty"`
}

// EndpointResponse is the master's answer. Found=false means sid or path is
// unspecified -- the worker's mmds.Source falls through exactly as the internal
// implementation does. Found=true with an empty Type never happens; Type is
// "static" (ContentType/Body populated), "secret", or "service" (their
// backends land in a later phase; the worker maps those to 503, matching
// internal/mmds's getMeta dispatch).
type EndpointResponse struct {
	RequestID   uint64 `json:"id"`
	Found       bool   `json:"found"`
	Type        string `json:"type,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Body        string `json:"body,omitempty"`
}

func writeFrame(w io.Writer, v any) error {
	b, err := json.Marshal(v)
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

func readFrame(r io.Reader, v any) error {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > maxFrame {
		return errors.New("mmdsrpc: bad frame length")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	return json.Unmarshal(buf, v)
}

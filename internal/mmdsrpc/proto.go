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
// The namespace limit must leave headroom below maxFrame because a frame also
// carries the JSON envelope, sandbox ID, token, and other protocol metadata.
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
// "static" (ContentType/Body populated), "secret" (Present/Retryable/
// ContentType/Body meaningful), or "service" (Target/ServiceName meaningful,
// see below).
//
// For Type=="secret": Present distinguishes "configured" (ContentType/Body
// hold the real value) from "specified but absent". Retryable is meaningful
// only when !Present -- true means the secret has never been configured
// (revision==0, worth polling, matching the internal-mode bounded wait);
// false means it was configured and later revoked (a deliberate revoke gets
// no grace period). The master owns this decision (it has the synced
// revision state); the worker's retry loop just obeys Retryable, never
// reasoning about revisions itself. Body, unlike a static route's Data, may
// be arbitrary non-UTF-8 bytes, so it travels base64-encoded on this wire
// (Type=="static" bodies are always valid UTF-8 and are NOT base64'd).
//
// For Type=="service": the master only resolves the specification -- it never
// dials the registered local service itself. Target is the operator-
// registered service name (an mmds.services key) the sandbox's own specified
// ServiceName resolves to; the worker looks Target up in its own local
// registry (each worker independently loads the same proxy.yaml the master
// does) and dials it directly, setting the X-Kuasar-MMDS-Service header to
// ServiceName -- never Target, since the guest's own alias is what a
// registered service should see, not the operator-side name. No response
// body/status crosses this RPC for "service": the worker calls the local
// service itself, after this RPC has already returned.
//
// Unavailable is meaningful for Type=="secret" and Type=="service"; false
// (its zero value) for every other type.
//
// For Type=="secret": true means the master's secret view is not currently
// synced (proxyshm.MMDSSecrets.Synced()==false -- mid-resync after a
// disconnect, or never yet synced), so Present/Retryable/ContentType/Body are
// meaningless and must not be trusted even if they carry zero values that
// would otherwise look like "never configured". The worker maps this to a
// hard error (503 to the guest), never to the 404 a genuinely absent/revoked
// secret would get -- serving stale plaintext or a false "not configured"
// during a resync is exactly what this guards against.
//
// For Type=="service": true means the master's route declaration view is not
// currently synced (proxyshm.MMDSRoutes.Synced()==false), so Target/
// ServiceName are meaningless. Unlike secret, MMDSRoutes holds no plaintext to
// protect, but a service route still drives a real dial to a local trusted
// service under the sandbox's identity, so acting on a possibly-stale
// declaration is unsafe regardless. The worker maps this to a 503
// StatusCode -- unlike secret, never a hard error -- matching every other
// service-route failure's guest-visible shape (see mmdssvc.Call's Result).
type EndpointResponse struct {
	RequestID   uint64 `json:"id"`
	Found       bool   `json:"found"`
	Type        string `json:"type,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	Body        string `json:"body,omitempty"`
	Present     bool   `json:"present,omitempty"`
	Retryable   bool   `json:"retryable,omitempty"`
	Target      string `json:"target,omitempty"`
	ServiceName string `json:"service_name,omitempty"`
	Unavailable bool   `json:"unavailable,omitempty"`
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

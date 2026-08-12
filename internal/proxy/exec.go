package proxy

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	sandboxctl "github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// serveExecConnect authorizes an exec capability before allowing any lifecycle
// side effect. The first lookup is read-only; only a valid KAT may enter
// ActivateExec, which can resume the sandbox and returns a freshly read identity.
func (p *Proxy) serveExecConnect(w http.ResponseWriter, r *http.Request, sid string) {
	execRouter, ok := p.router.(ExecRouter)
	if !ok || p.execRunRoot == "" {
		p.mx.Inc(`data_requests_total{result="denied"}`)
		writeProxyError(w, http.StatusNotImplemented, "data plane not available on this sandbox", ProxyErrorDenied)
		return
	}

	identity, found, err := execRouter.LookupExec(r.Context(), sid)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusServiceUnavailable, "routing error", ProxyErrorRouteError)
		return
	}
	if !found {
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		// The KAT was admitted and parking has started, so this is not a
		// pre-admission stale response that a cluster router may safely retry.
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorRouteError)
		return
	}

	// Exec authentication is always enforced. It deliberately bypasses the
	// ordinary off/log/enforce data-plane policy and accepts only the original
	// X-Access-Token value.
	token := r.Header.Get(HeaderAccessToken)
	if err := keys.VerifyExecAccessToken(token, identity.ServiceSecret, identity.AuthSandboxID, time.Now()); err != nil {
		p.mx.Inc(`data_requests_total{result="unauthorized"}`)
		writeProxyError(w, http.StatusUnauthorized, "invalid access token", ProxyErrorUnauthorized)
		return
	}
	flow := p.traffic.BeginParking(identity.NodeSandboxID, ConnectServiceExec)
	if flow == nil {
		flow = noopTrafficFlow{}
	}
	defer flow.Close()

	ready, found, err := execRouter.ActivateExec(r.Context(), sid, identity)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusServiceUnavailable, "sandbox activation failed", ProxyErrorRouteError)
		return
	}
	if !found {
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorNotFound)
		return
	}
	// ActivateExec must preserve the exact node-local and credential identity
	// authorized above; a changed lineage cannot inherit this request.
	if ready != identity {
		p.mx.Inc(`data_requests_total{result="unauthorized"}`)
		writeProxyError(w, http.StatusUnauthorized, "invalid access token", ProxyErrorRouteError)
		return
	}

	ctlRoute := Route{
		Kind: KindUDS,
		UDS:  filepath.Join(p.execRunRoot, ready.NodeSandboxID, "ctl.sock"),
	}
	ctlConn, err := p.dial(r.Context(), ctlRoute)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="upstream_error"}`)
		writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
		return
	}
	ctlConn = flow.AttachBackend(ctlConn)

	if r.ProtoMajor == 2 {
		p.serveExecH2(w, r, ctlConn)
		return
	}
	p.serveExecH1(w, r, ctlConn)
}

func (p *Proxy) serveExecH1(w http.ResponseWriter, r *http.Request, ctlConn io.ReadWriteCloser) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		_ = ctlConn.Close()
		writeProxyError(w, http.StatusInternalServerError, "connect unsupported", ProxyErrorUpstreamError)
		return
	}
	client, rw, err := hijacker.Hijack()
	if err != nil {
		_ = ctlConn.Close()
		return
	}
	downstream := &h1ConnectStream{Conn: client, reader: rw.Reader}
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = downstream.Close()
		_ = ctlConn.Close()
		return
	}
	if err := rw.Flush(); err != nil {
		_ = downstream.Close()
		_ = ctlConn.Close()
		return
	}
	p.mx.Inc(`data_requests_total{result="ok"}`)
	// net/http cancels an HTTP/1 request context when it observes a TCP read EOF,
	// but after Hijack that EOF is a valid client write-side half-close. Preserve
	// request values and any explicit deadline while letting connection I/O carry
	// disconnects, so ctl output can still drain after stdin EOF.
	tunnelCtx := context.WithoutCancel(r.Context())
	cancel := func() {}
	if deadline, ok := r.Context().Deadline(); ok {
		tunnelCtx, cancel = context.WithDeadline(tunnelCtx, deadline)
	}
	defer cancel()
	if err := sandboxctl.ProxyExec(tunnelCtx, downstream, ctlConn); err != nil {
		p.logExecRelayError(tunnelCtx, err)
	}
}

func (p *Proxy) serveExecH2(w http.ResponseWriter, r *http.Request, ctlConn io.ReadWriteCloser) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		_ = ctlConn.Close()
		writeProxyError(w, http.StatusInternalServerError, "connect unsupported", ProxyErrorUpstreamError)
		return
	}
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	downstream := &h2ConnectStream{reader: body, writer: w, flusher: flusher}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	p.mx.Inc(`data_requests_total{result="ok"}`)
	if err := sandboxctl.ProxyExec(r.Context(), downstream, ctlConn); err != nil {
		p.logExecRelayError(r.Context(), err)
	}
}

func (p *Proxy) logExecRelayError(ctx context.Context, err error) {
	if p.log == nil || err == nil {
		return
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		p.log.Debug("exec tunnel closed", "err", ctxErr)
		return
	}
	p.log.Debug("exec tunnel relay ended", "err", err)
}

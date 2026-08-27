package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/execadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/execadmission/limits"
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
		// LookupExec is side-effect-free and no credential has been admitted, so a
		// chained cluster router may safely treat this as a stale node-local route.
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorNotFound)
		return
	}

	// Exec authentication is always enforced. It deliberately bypasses the
	// ordinary off/log/enforce data-plane policy and accepts only the original
	// X-Access-Token value.
	token := r.Header.Get(HeaderAccessToken)
	claims, err := keys.ParseAndVerifyExecAccessToken(
		token, identity.ServiceSecret, identity.AuthSandboxID, time.Now(),
	)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="unauthorized"}`)
		writeProxyError(w, http.StatusUnauthorized, "invalid access token", ProxyErrorUnauthorized)
		return
	}
	compiler, err := execadmission.Default()
	if err != nil {
		p.mx.Inc(`data_requests_total{result="denied"}`)
		writeProxyError(w, http.StatusNotImplemented, "exec admission unavailable", ProxyErrorDenied)
		return
	}
	programs, err := compiler.Compile(claims.Conditions)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, "invalid exec conditions", ProxyErrorBadRequest)
		return
	}

	tunnelCtx := r.Context()
	cancelTunnel := func() {}
	if r.ProtoMajor != 2 {
		// net/http cancels an HTTP/1 request context when it observes a TCP read
		// EOF. After Hijack that EOF is a valid client write-side half-close, so
		// preserve values and any explicit deadline while connection I/O carries
		// disconnects.
		tunnelCtx = context.WithoutCancel(r.Context())
		if deadline, ok := r.Context().Deadline(); ok {
			tunnelCtx, cancelTunnel = context.WithDeadline(tunnelCtx, deadline)
		}
	}
	defer cancelTunnel()

	var flow TrafficFlow
	defer func() {
		if flow != nil {
			flow.Close()
		}
	}()
	err = sandboxctl.ServeExecTunnel(tunnelCtx, sandboxctl.ExecTunnelOptions{
		Authorize: func(context.Context) error { return nil },
		AcceptDownstream: func(context.Context) (io.ReadWriteCloser, error) {
			if r.ProtoMajor == 2 {
				return p.acceptExecH2(w, r)
			}
			return p.acceptExecH1(w)
		},
		AuthorizeRequest: func(ctx context.Context, frame *sandboxctl.ExecRequestFrame) error {
			if claims.ExpiresUnix != nil && time.Now().Unix() >= *claims.ExpiresUnix {
				return errors.New("exec capability expired")
			}
			return programs.Evaluate(ctx, frame.Request.Exec)
		},
		DialBackend: func(ctx context.Context, _ *sandboxctl.ExecRequestFrame) (io.ReadWriteCloser, error) {
			flow = p.traffic.BeginParking(identity.NodeSandboxID, ConnectServiceExec)
			if flow == nil {
				flow = noopTrafficFlow{}
			}
			ready, found, err := execRouter.ActivateExec(ctx, sid, identity)
			if err != nil {
				p.mx.Inc(`data_requests_total{result="route_error"}`)
				return nil, errors.New("exec backend unavailable")
			}
			if !found {
				p.mx.Inc(`data_requests_total{result="notfound"}`)
				return nil, errors.New("exec backend unavailable")
			}
			// Activation must preserve the exact identity authorized before 200.
			if ready != identity {
				p.mx.Inc(`data_requests_total{result="unauthorized"}`)
				return nil, errors.New("exec backend unavailable")
			}
			ctlRoute := Route{
				Kind: KindUDS,
				UDS:  filepath.Join(p.execRunRoot, ready.NodeSandboxID, "ctl.sock"),
			}
			ctlConn, err := p.dial(ctx, ctlRoute)
			if err != nil {
				p.mx.Inc(`data_requests_total{result="upstream_error"}`)
				return nil, errors.New("exec backend unavailable")
			}
			return flow.AttachBackend(ctlConn), nil
		},
		FirstRequestTimeout: limits.FirstRequestTimeout,
	})
	if err != nil {
		p.logExecRelayError(tunnelCtx, err)
	}
}

func (p *Proxy) acceptExecH1(w http.ResponseWriter) (io.ReadWriteCloser, error) {
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		writeProxyError(w, http.StatusInternalServerError, "connect unsupported", ProxyErrorUpstreamError)
		return nil, errors.New("exec CONNECT hijack unsupported")
	}
	client, rw, err := hijacker.Hijack()
	if err != nil {
		return nil, errors.New("exec CONNECT hijack failed")
	}
	downstream := &h1ConnectStream{Conn: client, reader: rw.Reader}
	if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		_ = downstream.Close()
		return nil, errors.New("exec CONNECT response failed")
	}
	if err := rw.Flush(); err != nil {
		_ = downstream.Close()
		return nil, errors.New("exec CONNECT response failed")
	}
	p.mx.Inc(`data_requests_total{result="ok"}`)
	return downstream, nil
}

func (p *Proxy) acceptExecH2(w http.ResponseWriter, r *http.Request) (io.ReadWriteCloser, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeProxyError(w, http.StatusInternalServerError, "connect unsupported", ProxyErrorUpstreamError)
		return nil, errors.New("exec CONNECT flush unsupported")
	}
	body := r.Body
	if body == nil {
		body = http.NoBody
	}
	downstream := &h2ConnectStream{reader: body, writer: w, flusher: flusher}
	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	p.mx.Inc(`data_requests_total{result="ok"}`)
	return downstream, nil
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

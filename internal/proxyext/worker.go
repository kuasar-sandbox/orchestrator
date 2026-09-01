package proxyext

import (
	"net/http"

	proxyextension "github.com/kuasar-sandbox/orchestrator/app/proxy/extension"
	internalproxy "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
)

// WorkerHost is the process-local implementation of the public worker Host.
// It exposes only independent route projections and the high-level core
// forwarding pipeline, never raw SHM records or mutable router state.
type WorkerHost struct {
	process proxyextension.Process
	table   *proxyshm.Table
	proxy   *internalproxy.Proxy
}

func NewWorkerHost(process proxyextension.Process, table *proxyshm.Table, core *internalproxy.Proxy) *WorkerHost {
	return &WorkerHost{process: process, table: table, proxy: core}
}

func (h *WorkerHost) Process() proxyextension.Process { return h.process }

func (h *WorkerHost) GetRoute(sandboxID string) (proxyextension.RouteView, bool) {
	if h == nil || h.table == nil || sandboxID == "" {
		return proxyextension.RouteView{}, false
	}
	route, found, revision := h.table.LookupRevision(sandboxID)
	if !found {
		return proxyextension.RouteView{}, false
	}
	return projectRoute(route, revision), true
}

func (h *WorkerHost) ForwardAuthorized(w http.ResponseWriter, r *http.Request, request proxyextension.ForwardRequest) {
	if h == nil || h.proxy == nil {
		http.Error(w, "proxy worker unavailable", http.StatusServiceUnavailable)
		return
	}
	target, ok := internalConnectTarget(request.Target)
	if !ok {
		w.Header().Set(internalproxy.HeaderProxyError, internalproxy.ProxyErrorBadRequest)
		http.Error(w, "invalid private forward request", http.StatusBadRequest)
		return
	}
	h.proxy.ForwardAuthorized(w, r, internalproxy.AuthorizedForwardRequest{
		SandboxID:  request.SandboxID,
		Target:     target,
		Revalidate: request.Revalidate,
		Rewrite:    request.Rewrite,
	})
}

func internalConnectTarget(target proxyextension.ConnectTarget) (internalproxy.ConnectTarget, bool) {
	if target.Port < 0 || target.Port > 65535 {
		return internalproxy.ConnectTarget{}, false
	}
	var service internalproxy.ConnectService
	switch target.Service {
	case proxyextension.ConnectServiceLegacy:
		if target.Port == 0 {
			return internalproxy.ConnectTarget{}, false
		}
		service = internalproxy.ConnectServiceLegacy
	case proxyextension.ConnectServiceForward:
		if target.Port == 0 {
			return internalproxy.ConnectTarget{}, false
		}
		service = internalproxy.ConnectServiceForward
	case proxyextension.ConnectServiceE2BEnvd:
		service = internalproxy.ConnectServiceE2BEnvd
	case proxyextension.ConnectServiceE2BInterpreter:
		service = internalproxy.ConnectServiceE2BInterpreter
	case proxyextension.ConnectServiceExec:
		service = internalproxy.ConnectServiceExec
	default:
		return internalproxy.ConnectTarget{}, false
	}
	return internalproxy.ConnectTarget{Service: service, Port: target.Port}, true
}

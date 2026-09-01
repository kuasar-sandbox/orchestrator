package conductorapp

import (
	"bufio"
	"context"
	"hash/fnv"
	"log/slog"
	"net"
	"net/http"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/appnet"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/netns"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ExternalTrafficProvider reads traffic snapshots from the currently
// registered external proxy master.
type ExternalTrafficProvider struct {
	Plugins *configsock.Registry
}

func (p *ExternalTrafficProvider) SandboxTrafficStats(ctx context.Context, sandboxID, runID string, profile types.Profile, state types.State) (*api.TrafficStats, error) {
	if p == nil || p.Plugins == nil {
		return nil, api.ErrStatsUnavailable
	}
	socket, found := p.Plugins.ProxyStatsTarget()
	if !found {
		return nil, api.ErrStatsUnavailable
	}
	results, err := proxystats.QuerySocket(ctx, socket, []proxystats.TrafficQuery{{
		SandboxID: sandboxID, RunID: runID, Profile: profile, State: state,
	}})
	if err != nil {
		return nil, err
	}
	if len(results) != 1 || results[0].SandboxID != sandboxID || results[0].Stats == nil {
		return nil, api.ErrStatsUnavailable
	}
	return results[0].Stats, nil
}

// ProxyForwarder is the conductor's external-mode fallback. Each request is
// sent unchanged over a fresh connection to one registered proxy UDS; the
// worker's raw ingress wrapper and built-in handler are the final parsers.
type ProxyForwarder struct {
	registry *configsock.Registry
	metrics  *metrics.M
	logger   *slog.Logger
}

func NewProxyForwarder(registry *configsock.Registry, mx *metrics.M, logger *slog.Logger) *ProxyForwarder {
	return &ProxyForwarder{registry: registry, metrics: mx, logger: logger}
}

func (p *ProxyForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	targets := p.registry.ProxyTargets()
	if len(targets) == 0 {
		p.metrics.Inc(`proxy_forwarder_total{result="no_proxy"}`)
		http.Error(w, "no proxy registered", http.StatusBadGateway)
		return
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(proxyAffinityKey(r)))
	p.forward(w, r, targets[hash.Sum32()%uint32(len(targets))])
}

func proxyAffinityKey(r *http.Request) string {
	if r.Method == http.MethodConnect {
		if sandboxID, _, ok := proxy.ParseConnect(r); ok {
			return "sandbox\x00" + sandboxID
		}
	} else if sandboxID, _, ok := proxy.ParseSandbox(r); ok {
		return "sandbox\x00" + sandboxID
	}
	path := ""
	if r.URL != nil {
		path = r.URL.EscapedPath()
		if path == "" {
			path = r.URL.Path
		}
	}
	return "raw\x00" + r.Host + "\x00" + r.Method + "\x00" + path
}

func (p *ProxyForwarder) forward(w http.ResponseWriter, r *http.Request, socket string) {
	backend, err := (&net.Dialer{}).DialContext(r.Context(), "unix", socket)
	if err != nil {
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	outbound := r.Clone(r.Context())
	outbound.RequestURI = ""
	if r.Method == http.MethodConnect {
		// CONNECT payload starts only after the worker accepts the tunnel. In
		// particular, an HTTP/2 request Body is the downstream duplex stream and
		// must not be consumed while writing the HTTP/1.1 handshake to the UDS.
		outbound.Body = http.NoBody
		outbound.ContentLength = 0
		outbound.TransferEncoding = nil
	}
	if err := outbound.Write(backend); err != nil {
		backend.Close()
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	buffered := bufio.NewReader(backend)
	response, err := http.ReadResponse(buffered, outbound)
	if err != nil {
		backend.Close()
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	if r.Method == http.MethodConnect && response.StatusCode == http.StatusOK {
		p.metrics.Inc(`proxy_forwarder_total{result="ok"}`)
		proxy.TunnelBuffered(w, r, backend, buffered)
		return
	}
	defer backend.Close()
	defer response.Body.Close()
	if (response.StatusCode != http.StatusOK && r.Method == http.MethodConnect) ||
		response.Header.Get(proxy.HeaderProxyError) != "" {
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
	} else {
		p.metrics.Inc(`proxy_forwarder_total{result="ok"}`)
	}
	proxy.WriteHTTPResponse(w, response)
}

func buildDataPlane(cfg *publicconfig.Conductor, core *orch.Orchestrator, plugins *configsock.Registry, mx *metrics.M, workerStats *proxystats.WorkerStats, logger *slog.Logger, proxyNS *netns.NetNS) http.Handler {
	switch cfg.Proxy.Mode {
	case publicconfig.ProxyOff:
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			mx.Inc(`data_requests_total{result="off"}`)
			http.Error(w, "data plane disabled (proxy_mode=off)", http.StatusNotImplemented)
		})
	case publicconfig.ProxyExternal:
		logger.Info("external proxy mode: proxy master registers on the config socket")
		return NewProxyForwarder(plugins, mx, logger)
	default:
		counter := proxy.Counter(mx)
		if workerStats != nil {
			counter = workerStats
		}
		return proxy.NewWithDialer(core, func() string { return cfg.Proxy.Auth }, logger, counter, appnet.RouteDialerInNetNS(proxyNS), cfg.Paths.RunRoot).
			WithTrafficTracker(workerStats)
	}
}

package conductorapp

import (
	"context"
	"hash/fnv"
	"log/slog"
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

// ProxyForwarder is the conductor's external-mode fallback. Each request uses
// a fresh chained CONNECT to a registered proxy UDS.
type ProxyForwarder struct {
	registry *configsock.Registry
	metrics  *metrics.M
	logger   *slog.Logger
}

func NewProxyForwarder(registry *configsock.Registry, mx *metrics.M, logger *slog.Logger) *ProxyForwarder {
	return &ProxyForwarder{registry: registry, metrics: mx, logger: logger}
}

func (p *ProxyForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		sandboxID string
		target    proxy.ConnectTarget
		ok        bool
	)
	if r.Method == http.MethodConnect {
		sandboxID, target, ok = proxy.ParseConnect(r)
	} else {
		var port int
		sandboxID, port, ok = proxy.ParseSandbox(r)
		target = proxy.LegacyTarget(port)
	}
	if !ok {
		http.Error(w, "bad sandbox host", http.StatusBadRequest)
		return
	}
	targets := p.registry.ProxyTargets()
	if len(targets) == 0 {
		p.metrics.Inc(`proxy_forwarder_total{result="no_proxy"}`)
		http.Error(w, "no proxy registered", http.StatusBadGateway)
		return
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(sandboxID))
	p.forward(w, r, targets[hash.Sum32()%uint32(len(targets))], sandboxID, target)
}

func (p *ProxyForwarder) forward(w http.ResponseWriter, r *http.Request, socket, sandboxID string, target proxy.ConnectTarget) {
	backend, buffered, response, err := proxy.DialSandboxConnect(r.Context(), "unix", socket, sandboxID, target, r.Header.Get(proxy.HeaderAccessToken))
	if err != nil {
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK {
		backend.Close()
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "connect refused by worker", response.StatusCode)
		return
	}
	if r.Method == http.MethodConnect {
		p.metrics.Inc(`proxy_forwarder_total{result="ok"}`)
		proxy.TunnelBuffered(w, r, backend, buffered)
		return
	}
	defer backend.Close()
	innerResponse, err := proxy.ForwardHTTPOnce(r, backend, buffered, nil)
	if err != nil {
		p.metrics.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	defer innerResponse.Body.Close()
	p.metrics.Inc(`proxy_forwarder_total{result="ok"}`)
	proxy.WriteHTTPResponse(w, innerResponse)
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

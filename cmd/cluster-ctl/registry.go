package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/membertransport"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// runRegistry starts the registry role: the state authority + node_link /
// route_link / scale_link hub (cluster.md §4.1). node_link serves nodes; route_link
// serves routers/admin tools; scale_link serves scalers.
func runRegistry(args []string, log *slog.Logger) error {
	if len(args) > 0 {
		switch args[0] {
		case "export", "import":
			return registryAdminCmd(args)
		}
	}
	fs := flag.NewFlagSet("registry", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/registry.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadRegistry(*cfgPath)
	if err != nil {
		return err
	}
	kv := clusterstore.OpenMemory(cfg.NodeLink.RevisionRetention)
	defer kv.Close()

	stores, remoteNodeOwners, err := newRegistryStores(kv, cfg)
	if err != nil {
		return err
	}
	stores.SetNodeListHeartbeatRefresh(nodeListHeartbeatRefresh(cfg.NodeLink.NodeDeadDur()))

	reg := registry.New(stores, nil, cfg.RouteLink.ParkDur(), log)
	reg.SetRemoteNodeOwners(remoteNodeOwners)
	if active, ok := cfg.Membership.ActiveVersion(); ok {
		reg.SetScaleReadyLabel(active.Label)
	}
	reg.SetPlacer(registry.NewHTTPScalePlacer(reg, cfg.ScaleLink.ScalerReplicaCount, cfg.ScaleLink.PlaceDur()))
	cfgState := newRegistryRuntimeConfig(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	memberMux, err := newMemberTransportMux(cfg, cfg.Member.TLS)
	if err != nil {
		return err
	}
	go runRegistryReload(ctx, *cfgPath, cfgState, reg, memberMux, log)

	// Dead-node sweep (cluster.md §11): reset the sandboxes of nodes whose
	// node-link dropped and whose last heartbeat predates node_dead_after.
	go reg.RunReaper(ctx, cfg.NodeLink.NodeDeadDur())
	// Key predistribution + lease renewal to each group's allocation set (§7.6).
	go reg.RunKeyDistributor(ctx, time.Hour)

	controlMux := http.NewServeMux()
	controlMux.Handle(membertransport.PathPrefix+"/", memberMux)
	mountRegistryControl(controlMux, reg, cfgState, *cfgPath, memberMux)
	if !cfg.NodeLinkSplit() {
		controlMux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
	} else {
		nodeMux := http.NewServeMux()
		nodeMux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)
		go func() {
			if e := serveClusterHTTP(ctx, "node_link", cfg.NodeListen(), cfg.NodeLink.TLS, nodeMux, log); e != nil {
				log.Error("registry node_link", "err", e)
			}
		}()
	}

	log.Info("cluster-ctl registry",
		"member", cfg.Member.ID,
		"listen", cfg.ControlListen(),
		"tls", cfg.Member.TLS.Enabled(),
		"node_link", cfg.NodeListen(),
		"node_link_split", cfg.NodeLinkSplit(),
		"membership_active", cfg.Membership.Active)
	return serveClusterHTTP(ctx, "member", cfg.ControlListen(), cfg.Member.TLS, controlMux, log)
}

func newMemberTransportMux(cfg *clustercfg.RegistryConfig, tlsMaterial clustercfg.TLS) (*membertransport.Mux, error) {
	mux := membertransport.NewMux()
	if err := syncMemberTransportMux(mux, cfg, tlsMaterial); err != nil {
		return nil, err
	}
	return mux, nil
}

func syncMemberTransportMux(mux *membertransport.Mux, cfg *clustercfg.RegistryConfig, tlsMaterial clustercfg.TLS) error {
	resolver := memberResolver(cfg)
	_, client, err := registryMemberClient(cfg.ControlAdvertise(), tlsMaterial)
	if err != nil {
		return err
	}
	for _, version := range cfg.Membership.OwnerVersions() {
		if err := mux.Register(membertransport.New(version.Label, cfg.Member.ID, resolver, client)); err != nil {
			return err
		}
	}
	return nil
}

func memberResolver(cfg *clustercfg.RegistryConfig) membertransport.StaticResolver {
	resolver := membertransport.StaticResolver{}
	for _, member := range unionMembershipMembers(cfg.Membership.OwnerVersions()) {
		if member.ID == "" || member.Advertise == "" {
			continue
		}
		resolver[member.ID] = member.Advertise
	}
	return resolver
}

type registryRuntimeConfig struct {
	mu  sync.RWMutex
	cfg *clustercfg.RegistryConfig
}

func newRegistryRuntimeConfig(cfg *clustercfg.RegistryConfig) *registryRuntimeConfig {
	return &registryRuntimeConfig{cfg: cfg}
}

func (s *registryRuntimeConfig) get() *clustercfg.RegistryConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := *s.cfg
	return &cp
}

func (s *registryRuntimeConfig) set(cfg *clustercfg.RegistryConfig) {
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
}

func mountRegistryControl(mux *http.ServeMux, reg *registry.Registry, cfgState *registryRuntimeConfig, cfgPath string, memberMux *membertransport.Mux) {
	reg.ServeRouteLink(mux)
	reg.ServeScaleLink(mux)
	mux.HandleFunc(clusterstate.RouteReplicaRPCPath, func(w http.ResponseWriter, req *http.Request) {
		clusterstate.ServeRouteReplica(w, req, reg.Stores().LocalRouteReplica())
	})
	mux.HandleFunc(clusterstate.NodeReplicaRPCPath, func(w http.ResponseWriter, req *http.Request) {
		clusterstate.ServeNodeReplica(w, req, reg.Stores().LocalNodeReplica())
	})
	mux.HandleFunc(registry.NodeOwnerRPCPath, func(w http.ResponseWriter, req *http.Request) {
		registry.ServeNodeOwner(w, req, reg.LocalNodeOwner())
	})
	mux.HandleFunc("/cluster/membership", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(cfgState.get().Membership.WithComputedLabels())
	})
	mux.HandleFunc("/cluster/reload", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		cfg := cfgState.get()
		if err := reloadRegistryConfig(req.Context(), cfgPath, cfg, cfgState, reg, memberMux); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func runRegistryReload(ctx context.Context, cfgPath string, cfgState *registryRuntimeConfig, reg *registry.Registry, memberMux *membertransport.Mux, log *slog.Logger) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			old := cfgState.get()
			if err := reloadRegistryConfig(ctx, cfgPath, old, cfgState, reg, memberMux); err != nil {
				log.Error("registry reload failed", "err", err)
			} else {
				next := cfgState.get()
				log.Info("registry reloaded", "membership_active", next.Membership.Active, "membership_next", next.Membership.Next)
			}
		}
	}
}

func reloadRegistryConfig(ctx context.Context, cfgPath string, old *clustercfg.RegistryConfig, cfgState *registryRuntimeConfig, reg *registry.Registry, memberMux *membertransport.Mux) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cfgPath == "" {
		return fmt.Errorf("registry reload: config path is empty")
	}
	next, err := clustercfg.LoadRegistry(cfgPath)
	if err != nil {
		return err
	}
	if old.Member.ID != next.Member.ID {
		return fmt.Errorf("registry reload: member.id change requires restart")
	}
	if old.ControlListen() != next.ControlListen() || old.NodeListen() != next.NodeListen() || old.NodeLinkSplit() != next.NodeLinkSplit() {
		return fmt.Errorf("registry reload: listener changes require restart")
	}
	if old.RouteLink.ParkTimeout != next.RouteLink.ParkTimeout ||
		old.NodeLink.NodeDeadAfter != next.NodeLink.NodeDeadAfter ||
		old.NodeLink.HeartbeatInterval != next.NodeLink.HeartbeatInterval ||
		old.NodeLink.RevisionRetention != next.NodeLink.RevisionRetention {
		return fmt.Errorf("registry reload: non-membership runtime changes require restart")
	}
	active, ok := next.Membership.ActiveVersion()
	if !ok {
		return fmt.Errorf("registry reload: active membership %d not found", next.Membership.Active)
	}
	views, routeReplicas, nodeReplicas, nodeOwners, err := buildRegistryTopology(next)
	if err != nil {
		return err
	}
	if memberMux != nil {
		if err := syncMemberTransportMux(memberMux, next, next.Member.TLS); err != nil {
			return err
		}
	}
	reg.Stores().SetClusterTopology(views,
		next.Membership.Owners.RouteLink, next.Membership.Owners.NodeLink,
		routeReplicas, nodeReplicas)
	reg.Stores().SetMembershipVersion(active.Version)
	reg.Stores().SetNodeListHeartbeatRefresh(nodeListHeartbeatRefresh(next.NodeLink.NodeDeadDur()))
	reg.SetRemoteNodeOwners(nodeOwners)
	reg.SetScaleReadyLabel(active.Label)
	reg.SetPlacer(registry.NewHTTPScalePlacer(reg, next.ScaleLink.ScalerReplicaCount, next.ScaleLink.PlaceDur()))
	cfgState.set(next)
	return nil
}

func nodeListHeartbeatRefresh(deadAfter time.Duration) time.Duration {
	if deadAfter <= 0 {
		return time.Minute
	}
	d := deadAfter / 3
	if d < time.Second {
		d = time.Second
	}
	if d > time.Minute {
		d = time.Minute
	}
	return d
}

func newRegistryStores(kv clusterstore.Store, cfg *clustercfg.RegistryConfig) (*registry.Stores, map[string]registry.NodeOwner, error) {
	active, ok := cfg.Membership.ActiveVersion()
	if !ok {
		return nil, nil, fmt.Errorf("registry: active membership %d not found", cfg.Membership.Active)
	}
	views, routeReplicas, nodeReplicas, nodeOwners, err := buildRegistryTopology(cfg)
	if err != nil {
		return nil, nil, err
	}
	stores := registry.NewClusterStoresWithViews(kv, cfg.Member.ID, views,
		cfg.Membership.Owners.RouteLink, cfg.Membership.Owners.NodeLink,
		routeReplicas, nodeReplicas)
	stores.SetMembershipVersion(active.Version)
	return stores, nodeOwners, nil
}

func buildRegistryTopology(cfg *clustercfg.RegistryConfig) ([]clusterstate.MemberView, map[string]clusterstate.RouteReplica, map[string]clusterstate.NodeReplica, map[string]registry.NodeOwner, error) {
	versions := cfg.Membership.OwnerVersions()
	views := make([]clusterstate.MemberView, 0, len(versions))
	for _, version := range versions {
		views = append(views, clusterstate.MemberView{Version: version.Version, Members: version.MemberIDs()})
	}
	routeReplicas := map[string]clusterstate.RouteReplica{}
	nodeReplicas := map[string]clusterstate.NodeReplica{}
	nodeOwners := map[string]registry.NodeOwner{}
	for _, member := range unionMembershipMembers(versions) {
		if member.ID == "" || member.ID == cfg.Member.ID {
			continue
		}
		base, client, err := registryMemberClient(member.Advertise, cfg.Member.TLS)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("registry: member %q: %w", member.ID, err)
		}
		routeReplicas[member.ID] = clusterstate.NewHTTPRouteReplica(base, client)
		nodeReplicas[member.ID] = clusterstate.NewHTTPNodeReplica(base, client)
		nodeOwners[member.ID] = registry.NewHTTPNodeOwner(base, client)
	}
	return views, routeReplicas, nodeReplicas, nodeOwners, nil
}

func unionMembershipMembers(versions []clustercfg.MembershipVersion) []clustercfg.MembershipMember {
	seen := map[string]bool{}
	var out []clustercfg.MembershipMember
	for _, version := range versions {
		for _, member := range version.Members {
			if member.ID == "" || seen[member.ID] {
				continue
			}
			seen[member.ID] = true
			out = append(out, member)
		}
	}
	return out
}

func registryMemberClient(advertise string, tlsMaterial clustercfg.TLS) (string, *http.Client, error) {
	if advertise == "" {
		return "", nil, fmt.Errorf("advertise is required")
	}
	var tlsCfg *tls.Config
	if endpointServerName(advertise) != "" && tlsMaterial.Enabled() {
		cfg, err := tlsMaterial.ClientConfig(endpointServerName(advertise))
		if err != nil {
			return "", nil, err
		}
		tlsCfg = cfg
	}
	base, client := controlHTTPClient(advertise, tlsCfg)
	client.Timeout = 2 * time.Second
	return base, client, nil
}

func serveClusterHTTP(ctx context.Context, name, addr string, tlsCfg clustercfg.TLS, handler http.Handler, log *slog.Logger) error {
	ln, err := listenLink(addr)
	if err != nil {
		return fmt.Errorf("cluster: %s listen %s: %w", name, addr, err)
	}
	srv := &http.Server{Handler: h2c.NewHandler(handler, &http2.Server{})}
	useTLS := tlsCfg.Enabled() && !strings.HasPrefix(addr, "/")
	if useTLS {
		stls, terr := tlsCfg.ServerConfig()
		if terr != nil {
			return fmt.Errorf("cluster: %s tls: %w", name, terr)
		}
		srv = &http.Server{Handler: handler, TLSConfig: stls}
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Info("cluster listener", "name", name, "listen", addr, "tls", useTLS)
	var serveErr error
	if useTLS {
		serveErr = srv.ServeTLS(ln, "", "")
	} else {
		serveErr = srv.Serve(ln)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}
	return nil
}

// listenLink binds a registry link: a unix socket (path starts with "/") at 0600,
// or a TCP address.
func listenLink(addr string) (net.Listener, error) {
	if strings.HasPrefix(addr, "/") {
		_ = os.Remove(addr)
		if dir := addr[:strings.LastIndexByte(addr, '/')]; dir != "" {
			_ = os.MkdirAll(dir, 0o755)
		}
		ln, err := net.Listen("unix", addr)
		if err != nil {
			return nil, err
		}
		_ = os.Chmod(addr, 0o600)
		return ln, nil
	}
	return net.Listen("tcp", addr)
}

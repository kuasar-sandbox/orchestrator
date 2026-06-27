package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterstore"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/secretbox"
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

	var box *secretbox.Box
	if cfg.SandboxGroup.EncryptionKey != "" {
		if box, err = secretbox.NewFromColonHex(cfg.SandboxGroup.EncryptionKey); err != nil {
			return err
		}
	}
	stores := registry.NewStores(kv, box)

	// Group-config providers: store by default, external:<addr> per interface (§6.2).
	var extTLS *tls.Config
	if cfg.SandboxGroup.TLS.Enabled() {
		if extTLS, err = cfg.SandboxGroup.TLS.ClientConfig(""); err != nil {
			return fmt.Errorf("registry: sandbox_group tls: %w", err)
		}
	}
	resolver := registry.NewGroupResolver(cfg.SandboxGroup, stores, time.Minute, extTLS)

	// Placement is the standalone scaler (cluster.md §4.1/§5.2 — no in-process
	// mode): the registry reverse-requests Place over scale_link. With no
	// scaler attached, cold placement stalls (§11); the data plane is unaffected.
	reg := registry.New(stores, nil, cfg.Reserve.ParkDur(), log)
	reg.SetPlacer(registry.NewChannelPlacer(reg, cfg.Reserve.ParkDur()))
	reg.SetGroupResolver(resolver)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Dead-node sweep (cluster.md §11): reset the sandboxes of nodes whose
	// node-link dropped and whose last heartbeat predates node_dead_after.
	go reg.RunReaper(ctx, cfg.NodeLink.NodeDeadDur())
	// Key predistribution + lease renewal to each group's allocation set (§7.6).
	go reg.RunKeyDistributor(ctx, time.Hour)

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)

	// node-link over node_link.tls mTLS when configured (§5.4), else plain h2c.
	ln, err := net.Listen("tcp", cfg.NodeLink.Listen)
	if err != nil {
		return fmt.Errorf("registry: node_link listen %s: %w", cfg.NodeLink.Listen, err)
	}
	var srv *http.Server
	if cfg.NodeLink.TLS.Enabled() {
		stls, terr := cfg.NodeLink.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("registry: node_link tls: %w", terr)
		}
		srv = &http.Server{Handler: mux, TLSConfig: stls} // h2 over (m)TLS (§5.4)
	} else {
		srv = &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	// route_link is the router/admin-facing link: reserve, resolve, list, verify,
	// and operator import/export.
	routeMux := http.NewServeMux()
	reg.ServeRouteLink(routeMux)
	routeLn, err := listenLink(cfg.RouteLink.Listen)
	if err != nil {
		return fmt.Errorf("registry: route_link listen %s: %w", cfg.RouteLink.Listen, err)
	}
	routeSrv := &http.Server{Handler: routeMux}
	routeTLS := cfg.RouteLink.TLS.Enabled() && !strings.HasPrefix(cfg.RouteLink.Listen, "/")
	if routeTLS {
		stls, terr := cfg.RouteLink.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("registry: route_link tls: %w", terr)
		}
		routeSrv.TLSConfig = stls
	} else {
		routeSrv.Handler = h2c.NewHandler(routeMux, &http2.Server{})
	}
	go func() {
		<-ctx.Done()
		routeSrv.Close()
	}()
	go func() {
		var e error
		if routeTLS {
			e = routeSrv.ServeTLS(routeLn, "", "")
		} else {
			e = routeSrv.Serve(routeLn)
		}
		if e != nil && e != http.ErrServerClosed {
			log.Error("registry route_link", "err", e)
		}
	}()

	// scale_link is the scaler-facing link: node_list/group views and reverse
	// placement over the scale_link session.
	scaleMux := http.NewServeMux()
	reg.ServeScaleLink(scaleMux)
	scaleMux.HandleFunc(routesync.ScaleLinkPath, reg.ServeScalerLink)
	scaleLn, err := listenLink(cfg.ScaleLink.Listen)
	if err != nil {
		return fmt.Errorf("registry: scale_link listen %s: %w", cfg.ScaleLink.Listen, err)
	}
	scaleSrv := &http.Server{Handler: scaleMux}
	scaleTLS := cfg.ScaleLink.TLS.Enabled() && !strings.HasPrefix(cfg.ScaleLink.Listen, "/")
	if scaleTLS {
		stls, terr := cfg.ScaleLink.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("registry: scale_link tls: %w", terr)
		}
		scaleSrv.TLSConfig = stls
	} else {
		scaleSrv.Handler = h2c.NewHandler(scaleMux, &http2.Server{})
	}
	go func() {
		<-ctx.Done()
		scaleSrv.Close()
	}()
	go func() {
		var e error
		if scaleTLS {
			e = scaleSrv.ServeTLS(scaleLn, "", "")
		} else {
			e = scaleSrv.Serve(scaleLn)
		}
		if e != nil && e != http.ErrServerClosed {
			log.Error("registry scale_link", "err", e)
		}
	}()

	log.Info("cluster-ctl registry", "node_link", cfg.NodeLink.Listen, "node_link_tls", cfg.NodeLink.TLS.Enabled(), "route_link", cfg.RouteLink.Listen, "route_link_tls", routeTLS, "scale_link", cfg.ScaleLink.Listen, "scale_link_tls", scaleTLS)
	var serveErr error
	if cfg.NodeLink.TLS.Enabled() {
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

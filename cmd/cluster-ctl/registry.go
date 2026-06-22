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

// runRegistry starts the registry role: the durable state authority + node-link
// hub (cluster.md §4.1). Phase 2 serves the node-link channel (nodes dial it to
// register, stream routes, receive commands); the router/scaler control API
// lands with those roles.
func runRegistry(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("registry", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/registry.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadRegistry(*cfgPath)
	if err != nil {
		return err
	}
	if cfg.State.Backend != clustercfg.BackendSQLite {
		return fmt.Errorf("registry: state.backend %q not implemented yet (Phase 7); use sqlite", cfg.State.Backend)
	}

	kv, err := clusterstore.Open(cfg.State.DSN, cfg.NodeLink.RevisionRetention)
	if err != nil {
		return err
	}
	defer kv.Close()
	_ = os.Chmod(cfg.State.DSN, 0o600)

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
	// mode): the registry reverse-requests Place over the scaler-link. With no
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
	// Idle SAVED record GC (route-key cardinality cap, §10/§12); off when unset.
	go reg.RunRecordGC(ctx, cfg.Reserve.RecordTTLDur())

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

	// Control API (router / scaler dial, cluster.md §5.2; the cluster e2e drives
	// reserve through it).
	controlMux := http.NewServeMux()
	reg.ServeControl(controlMux)
	controlMux.HandleFunc(routesync.ScalerLinkPath, reg.ServeScalerLink) // scaler reverse-call (§5.2)
	controlLn, err := listenControl(cfg.ControlAPI.Listen)
	if err != nil {
		return fmt.Errorf("registry: control_api listen %s: %w", cfg.ControlAPI.Listen, err)
	}
	controlSrv := &http.Server{Handler: controlMux}
	// control mTLS for the cross-host split (cluster.md §5.4); a UDS (local) stays plain.
	controlTLS := cfg.ControlAPI.TLS.Enabled() && !strings.HasPrefix(cfg.ControlAPI.Listen, "/")
	if controlTLS {
		stls, terr := cfg.ControlAPI.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("registry: control_api tls: %w", terr)
		}
		controlSrv.TLSConfig = stls
	} else {
		// Plain (UDS/TCP): serve h2c so the scaler-link's full-duplex h2 stream works
		// (h2c.NewHandler still falls through to HTTP/1.1 for the watch GETs).
		controlSrv.Handler = h2c.NewHandler(controlMux, &http2.Server{})
	}
	go func() {
		<-ctx.Done()
		controlSrv.Close()
	}()
	go func() {
		var e error
		if controlTLS {
			e = controlSrv.ServeTLS(controlLn, "", "")
		} else {
			e = controlSrv.Serve(controlLn)
		}
		if e != nil && e != http.ErrServerClosed {
			log.Error("registry control", "err", e)
		}
	}()

	log.Info("cluster-ctl registry", "node_link", cfg.NodeLink.Listen, "node_link_tls", cfg.NodeLink.TLS.Enabled(), "control_api", cfg.ControlAPI.Listen, "control_api_tls", controlTLS, "state", cfg.State.DSN)
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

// listenControl binds the control API: a unix socket (path starts with "/") at 0600,
// or a TCP address.
func listenControl(addr string) (net.Listener, error) {
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

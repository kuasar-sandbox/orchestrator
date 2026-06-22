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
// register, stream routes, receive commands); the router/scaler op interface
// lands with those roles.
func runRegistry(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("registry", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/config.yaml", "config file")
	storeDSN := fs.String("store", "", "override store.dsn")
	channelListen := fs.String("channel-listen", "", "override channel.listen")
	_ = fs.Parse(args)

	cfg, err := clustercfg.Load(*cfgPath)
	if err != nil {
		return err
	}
	if *storeDSN != "" {
		cfg.Store.DSN = *storeDSN
	}
	if *channelListen != "" {
		cfg.Channel.Listen = *channelListen
	}
	if cfg.Store.Kind != clustercfg.StoreSQLite {
		return fmt.Errorf("registry: store.kind %q not implemented yet (Phase 7); use sqlite", cfg.Store.Kind)
	}

	kv, err := clusterstore.Open(cfg.Store.DSN, cfg.Channel.RevisionRetention)
	if err != nil {
		return err
	}
	defer kv.Close()
	_ = os.Chmod(cfg.Store.DSN, 0o600)

	var box *secretbox.Box
	if cfg.GroupConfig.EncryptionKey != "" {
		if box, err = secretbox.NewFromColonHex(cfg.GroupConfig.EncryptionKey); err != nil {
			return err
		}
	}
	stores := registry.NewStores(kv, box)

	// Group-config providers: store by default, external:<addr> per interface (§6.2).
	var extTLS *tls.Config
	if cfg.GroupConfig.TLS.Enabled() {
		if extTLS, err = cfg.GroupConfig.TLS.ClientConfig(""); err != nil {
			return fmt.Errorf("registry: group_config tls: %w", err)
		}
	}
	resolver := registry.NewGroupResolver(cfg.GroupConfig, stores, time.Minute, extTLS)

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
	go reg.RunReaper(ctx, cfg.Channel.NodeDeadDur())
	// Key predistribution + lease renewal to each group's allocation set (§7.6).
	go reg.RunKeyDistributor(ctx, time.Hour)

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)

	// node-link over channel.tls mTLS when configured (§5.4), else plain h2c.
	ln, err := net.Listen("tcp", cfg.Channel.Listen)
	if err != nil {
		return fmt.Errorf("registry: channel listen %s: %w", cfg.Channel.Listen, err)
	}
	var srv *http.Server
	if cfg.Channel.TLS.Enabled() {
		stls, terr := cfg.Channel.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("registry: channel tls: %w", terr)
		}
		srv = &http.Server{Handler: mux, TLSConfig: stls} // h2 over (m)TLS (§5.4)
	} else {
		srv = &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()

	// Op interface (router / scaler dial, cluster.md §5.2; the cluster e2e drives
	// reserve through it).
	opMux := http.NewServeMux()
	reg.ServeOp(opMux)
	opMux.HandleFunc(routesync.ScalerLinkPath, reg.ServeScalerLink) // scaler reverse-call (§5.2)
	opLn, err := listenOp(cfg.Op.Listen)
	if err != nil {
		return fmt.Errorf("registry: op listen %s: %w", cfg.Op.Listen, err)
	}
	opSrv := &http.Server{Handler: opMux}
	// op-mTLS for the cross-host split (cluster.md §5.4); a UDS (local) stays plain.
	opTLS := cfg.Op.TLS.Enabled() && !strings.HasPrefix(cfg.Op.Listen, "/")
	if opTLS {
		stls, terr := cfg.Op.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("registry: op tls: %w", terr)
		}
		opSrv.TLSConfig = stls
	} else {
		// Plain (UDS/TCP): serve h2c so the scaler-link's full-duplex h2 stream works
		// (h2c.NewHandler still falls through to HTTP/1.1 for the watch GETs).
		opSrv.Handler = h2c.NewHandler(opMux, &http2.Server{})
	}
	go func() {
		<-ctx.Done()
		opSrv.Close()
	}()
	go func() {
		var e error
		if opTLS {
			e = opSrv.ServeTLS(opLn, "", "")
		} else {
			e = opSrv.Serve(opLn)
		}
		if e != nil && e != http.ErrServerClosed {
			log.Error("registry op", "err", e)
		}
	}()

	log.Info("cluster-ctl registry", "channel_listen", cfg.Channel.Listen, "channel_tls", cfg.Channel.TLS.Enabled(), "op_listen", cfg.Op.Listen, "op_tls", opTLS, "store", cfg.Store.DSN)
	var serveErr error
	if cfg.Channel.TLS.Enabled() {
		serveErr = srv.ServeTLS(ln, "", "")
	} else {
		serveErr = srv.Serve(ln)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}
	return nil
}

// listenOp binds the op interface: a unix socket (path starts with "/") at 0600,
// or a TCP address.
func listenOp(addr string) (net.Listener, error) {
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

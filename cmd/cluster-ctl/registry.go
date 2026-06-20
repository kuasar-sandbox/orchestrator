package main

import (
	"context"
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
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/scaler"
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
	reg := registry.New(stores, scaler.New(stores, cfg.Scaler), cfg.Reserve.ParkDur(), log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Dead-node sweep (cluster.md §11): reset the sandboxes of nodes whose
	// node-link dropped and whose last heartbeat predates node_dead_after.
	go reg.RunReaper(ctx, cfg.Channel.NodeDeadDur())
	// Key predistribution + lease renewal to each group's allocation set (§7.6).
	go reg.RunKeyDistributor(ctx, time.Hour)

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)

	// TODO(Phase 7): channel.tls mTLS; Phase 3/4 add the op-listen interface for
	// router/scaler. Phase 2 serves node-link over plain h2c.
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
	// reserve through it). Phase 7 adds mTLS + the resumable watch.
	opMux := http.NewServeMux()
	reg.ServeOp(opMux)
	opLn, err := listenOp(cfg.Op.Listen)
	if err != nil {
		return fmt.Errorf("registry: op listen %s: %w", cfg.Op.Listen, err)
	}
	opSrv := &http.Server{Handler: opMux}
	go func() {
		<-ctx.Done()
		opSrv.Close()
	}()
	go func() {
		if err := opSrv.Serve(opLn); err != nil && err != http.ErrServerClosed {
			log.Error("registry op", "err", err)
		}
	}()

	log.Info("cluster-ctl registry", "channel_listen", cfg.Channel.Listen, "channel_tls", cfg.Channel.TLS.Enabled(), "op_listen", cfg.Op.Listen, "store", cfg.Store.DSN)
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

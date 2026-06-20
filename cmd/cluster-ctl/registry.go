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
	"syscall"

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
	reg := registry.New(registry.NewStores(kv, box), nil, cfg.Reserve.ParkDur(), log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc(routesync.NodeLinkPath, reg.ServeNodeLink)

	// TODO(Phase 7): channel.tls mTLS; Phase 3/4 add the op-listen interface for
	// router/scaler. Phase 2 serves node-link over plain h2c.
	ln, err := net.Listen("tcp", cfg.Channel.Listen)
	if err != nil {
		return fmt.Errorf("registry: channel listen %s: %w", cfg.Channel.Listen, err)
	}
	srv := &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Info("cluster-ctl registry serving node-link", "channel_listen", cfg.Channel.Listen, "store", cfg.Store.DSN)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

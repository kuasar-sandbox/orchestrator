package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/scaler"
)

// runScaler starts the standalone scaler role (scaler.mode=remote): it subscribes
// the registry's node/group view watches over the op channel (a local view) and
// serves placement on /scaler/place, which the registry's remotePlacer calls
// (cluster-scaler.md §5). In the default mode=inprocess the registry runs the same
// algorithm in-process and this command is not used.
func runScaler(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/config.yaml", "config file")
	registryAddr := fs.String("registry", "", "override registry op endpoint (UDS path or host:port)")
	listen := fs.String("listen", "", "override scaler.listen")
	_ = fs.Parse(args)

	cfg, err := clustercfg.Load(*cfgPath)
	if err != nil {
		return err
	}
	opAddr := cfg.Scaler.Registry
	if *registryAddr != "" {
		opAddr = *registryAddr
	}
	if opAddr == "" {
		opAddr = cfg.Op.Listen // co-located: dial the registry's op UDS
	}
	if *listen != "" {
		cfg.Scaler.Listen = *listen
	}
	if cfg.Scaler.Listen == "" {
		return fmt.Errorf("scaler: scaler.listen required (the /scaler/place endpoint the registry dials)")
	}

	// op-mTLS to dial the registry's view watches when remote (cluster.md §5.4).
	var opTLS *tls.Config
	if !strings.HasPrefix(opAddr, "/") && cfg.Op.TLS.Enabled() {
		host := opAddr
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if opTLS, err = cfg.Op.TLS.ClientConfig(host); err != nil {
			return fmt.Errorf("scaler: op tls: %w", err)
		}
	}

	svc := scaler.NewRemote(opAddr, opTLS, cfg.Scaler, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	svc.Start(ctx) // node + group view-watch loops

	ln, err := listenOp(cfg.Scaler.Listen) // UDS (0600) or TCP, shared with the registry op listener
	if err != nil {
		return fmt.Errorf("scaler: listen %s: %w", cfg.Scaler.Listen, err)
	}
	srv := &http.Server{Handler: svc.Handler()}
	useTLS := cfg.Scaler.TLS.Enabled() && !strings.HasPrefix(cfg.Scaler.Listen, "/")
	if useTLS {
		stls, terr := cfg.Scaler.TLS.ServerConfig()
		if terr != nil {
			return fmt.Errorf("scaler: tls: %w", terr)
		}
		srv.TLSConfig = stls
	}
	go func() {
		<-ctx.Done()
		srv.Close()
	}()
	log.Info("cluster-ctl scaler", "listen", cfg.Scaler.Listen, "registry_op", opAddr, "tls", useTLS,
		"place_candidates", cfg.Scaler.PlaceCandidates, "shuffle_rules", len(cfg.Scaler.ShuffleSharding))
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

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/scaler"
)

// runScaler starts the standalone scaler (cluster.md §1.2/§4.2 — always a separate
// process, no in-process mode): it DIALS the registry op endpoint, subscribes the
// node/group view, and answers the registry's reverse placement requests over the
// scaler-link. No listener (the registry reverse-calls it).
func runScaler(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/config.yaml", "config file")
	registryAddr := fs.String("registry", "", "override registry op endpoint (UDS path or host:port)")
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

	// op-mTLS to dial the registry when remote (cluster.md §5.4); UDS stays plain.
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

	deadAfter := int64(cfg.Channel.NodeDeadDur().Seconds())
	svc := scaler.NewRemote(opAddr, opTLS, cfg.Scaler, deadAfter, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	svc.Start(ctx) // node + group view watches + the scaler-link (reverse placement)
	log.Info("cluster-ctl scaler", "registry_op", opAddr, "op_tls", opTLS != nil,
		"place_candidates", cfg.Scaler.PlaceCandidates, "zone_admit_max", cfg.Scaler.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Scaler.ShuffleSharding))
	<-ctx.Done()
	return nil
}

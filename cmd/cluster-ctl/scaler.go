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
// process, no in-process mode): it DIALS the registry control endpoint, subscribes the
// node/group view, and answers the registry's reverse placement requests over the
// scaler-link. No listener (the registry reverse-calls it).
func runScaler(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/scaler.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadScaler(*cfgPath)
	if err != nil {
		return err
	}
	controlAddr := cfg.Registry.Endpoint // registry control_api to dial (default: co-located socket)

	// registry mTLS to dial when remote (cluster.md §5.4); a UDS endpoint stays plain.
	var controlTLS *tls.Config
	if !strings.HasPrefix(controlAddr, "/") && cfg.Registry.TLS.Enabled() {
		host := controlAddr
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if controlTLS, err = cfg.Registry.TLS.ClientConfig(host); err != nil {
			return fmt.Errorf("scaler: registry tls: %w", err)
		}
	}

	deadAfter := int64(cfg.NodeDeadDur().Seconds())
	svc := scaler.NewRemote(controlAddr, controlTLS, cfg.Placement, deadAfter, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	svc.Start(ctx) // node + group view watches + the scaler-link (reverse placement)
	log.Info("cluster-ctl scaler", "registry", controlAddr, "registry_tls", controlTLS != nil,
		"candidates", cfg.Placement.Candidates, "zone_admit_max", cfg.Placement.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Placement.ShuffleSharding))
	<-ctx.Done()
	return nil
}

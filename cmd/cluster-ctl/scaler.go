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
// process, no in-process mode): it DIALS registry scale_link, subscribes
// node_list/group views, and answers the registry's reverse placement requests over
// scale_link. No listener (the registry reverse-calls it).
func runScaler(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/scaler.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadScaler(*cfgPath)
	if err != nil {
		return err
	}
	scaleAddr := cfg.ScaleLink.Endpoint

	// scale_link mTLS to dial when remote (cluster.md §5.4); a UDS endpoint stays plain.
	var scaleTLS *tls.Config
	if !strings.HasPrefix(scaleAddr, "/") && cfg.ScaleLink.TLS.Enabled() {
		host := scaleAddr
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if scaleTLS, err = cfg.ScaleLink.TLS.ClientConfig(host); err != nil {
			return fmt.Errorf("scaler: scale_link tls: %w", err)
		}
	}

	deadAfter := int64(cfg.NodeDeadDur().Seconds())
	svc := scaler.NewRemote(scaleAddr, scaleTLS, cfg.Placement, deadAfter, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	svc.Start(ctx) // node_list + group watches + scale_link reverse placement
	log.Info("cluster-ctl scaler", "scale_link", scaleAddr, "scale_link_tls", scaleTLS != nil,
		"candidates", cfg.Placement.Candidates, "zone_admit_max", cfg.Placement.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Placement.ShuffleSharding))
	<-ctx.Done()
	return nil
}

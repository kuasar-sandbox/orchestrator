package main

import (
	"flag"
	"log/slog"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
)

// runScaler reports the scaler placement config. In this build the placement
// scheduler (internal/scaler: nodeSelectors + shuffle-sharding + P2C) runs
// co-located inside the registry process; the standalone scaler over the op
// channel (cluster.md §4.2 large-scale split) is a Phase 7 deployment of the
// same algorithm.
func runScaler(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/config.yaml", "config file")
	_ = fs.Parse(args)
	cfg, err := clustercfg.Load(*cfgPath)
	if err != nil {
		return err
	}
	log.Info("cluster-ctl scaler",
		"note", "placement runs co-located in the registry in this build (internal/scaler); standalone op-channel scaler is Phase 7",
		"place_candidates", cfg.Scaler.PlaceCandidates,
		"zone_admit_max", cfg.Scaler.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Scaler.ShuffleSharding))
	return nil
}

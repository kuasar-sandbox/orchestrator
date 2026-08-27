package main

import (
	"context"
	"log/slog"

	"github.com/kuasar-sandbox/orchestrator/internal/conductorapp"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
)

type resourceProbe = conductorapp.ResourceProbe

func startResourceController(ctx context.Context, cfg *config.ResourceListenConfig, resolved *nodectl.Resolved, runRoot string, logger *slog.Logger) (orch.ResourceProbe, error) {
	return conductorapp.StartResourceController(ctx, cfg, resolved, runRoot, logger)
}

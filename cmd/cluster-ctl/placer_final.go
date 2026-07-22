package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
	"github.com/kuasar-sandbox/orchestrator/internal/transportauth"
)

func runPlacer(args []string, log *slog.Logger) error {
	flags := flag.NewFlagSet("placer", flag.ContinueOnError)
	configPath := flags.String("config", "/etc/cluster-ctl/placer.yaml", "config file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("placer: unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	config, err := clustercfg.LoadFinalPlacer(*configPath)
	if err != nil {
		return err
	}
	inputs, err := placer.NewConfiguredGroupInputs(config.GroupSources)
	if err != nil {
		return err
	}
	service, err := placer.NewFinalService(inputs.Provider, config.Placement)
	if err != nil {
		return err
	}
	mux := finalPlacerHandler(service)
	server, err := newClusterHTTPServer("placer", config.Placer.Listen, config.Placer.TLS, mux)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("cluster-ctl placer", "id", config.Placer.ID, "listen", config.Placer.Listen,
		"group_sources", len(config.GroupSources), "candidates", config.Placement.Candidates)
	return server.Serve(ctx, log)
}

func finalPlacerHandler(service http.Handler) http.Handler {
	mux := http.NewServeMux()
	registry := transportauth.Middleware(transportauth.RoleRegistry, service)
	mux.Handle(placer.FinalPlanPath, registry)
	mux.Handle(placer.FinalCatalogSyncPath, registry)
	mux.Handle(placer.FinalKeyLeasePath, registry)
	mux.Handle(placer.FinalVerifyKeyPath, transportauth.Middleware(transportauth.RoleRouter, service))
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	return mux
}

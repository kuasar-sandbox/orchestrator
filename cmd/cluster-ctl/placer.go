package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
)

// runPlacer starts the standalone placer: it uses registry membership as the
// bootstrap source, keeps node/group placement views synced, and answers registry
// placement requests.
func runPlacer(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("placer", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/placer.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadPlacer(*cfgPath)
	if err != nil {
		return err
	}
	registryAddr := cfg.Registry.Bootstrap

	// registry mTLS to dial when remote; a UDS endpoint stays plain.
	var registryTLS *tls.Config
	if endpointServerName(registryAddr) != "" && cfg.Registry.TLS.Enabled() {
		if registryTLS, err = cfg.Registry.TLS.ClientConfig(endpointServerName(registryAddr)); err != nil {
			return fmt.Errorf("placer: registry tls: %w", err)
		}
	}

	deadAfter := int64(cfg.NodeDeadDur().Seconds())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	regClient, err := clusterclient.NewRegistry(registryAddr, registryTLS)
	if err != nil {
		return err
	}
	eps, err := regClient.OwnerEndpoints(ctx)
	if err != nil {
		return err
	}
	if len(eps) == 0 {
		return fmt.Errorf("placer: registry membership has no members")
	}
	if len(cfg.ImportGroups) == 0 {
		return fmt.Errorf("placer: standalone mode requires at least one import_groups source")
	}
	nodeListEps, err := regClient.NodeListEndpoints(ctx)
	if err != nil {
		return err
	}
	activeLabel, err := regClient.ActiveLabel(ctx)
	if err != nil {
		return err
	}
	links := registryLinks(eps)
	groupInputs, err := placer.NewConfiguredGroupInputs(cfg.ImportGroups)
	if err != nil {
		return err
	}
	svc := placer.NewRemoteLinksWithGroups(links, groupInputs.Provider, groupInputs.Sources, cfg.Placement, deadAfter, log)
	svc.SetNodeListLinksForLabel(ctx, registryLinks(nodeListEps), activeLabel)
	svc.SetPlacerLinkResolver(newPlacerLinkResolver(regClient))
	svc.SetPlacerLinkRefresher(regClient.Refresh)
	memberHub := membergroup.NewHub()
	memberlistTLS, err := membergroupTLSConfig(cfg.Placer.TLS)
	if err != nil {
		return fmt.Errorf("placer memberlist tls: %w", err)
	}
	placerGroup, err := membergroup.New(membergroup.Options{
		Label: cfg.Placer.MemberlistLabel, Name: cfg.Placer.ID, Hub: memberHub, Log: log,
		TLSConfig: memberlistTLS,
		Meta: membergroup.Meta{
			Role: membergroup.RolePlacer, ID: cfg.Placer.ID, Advertise: cfg.Placer.Advertise,
		},
	})
	if err != nil {
		return err
	}
	defer placerGroup.Shutdown()
	svc.SetImportSourceOwnerSource(cfg.Placer.ID, func() []string {
		if err := regClient.Refresh(ctx); err != nil {
			return []string{cfg.Placer.ID}
		}
		label, err := regClient.ActiveLabel(ctx)
		if err != nil {
			return []string{cfg.Placer.ID}
		}
		metas := placerGroup.ReadyPlacers(label)
		ids := make([]string, 0, len(metas))
		for _, meta := range metas {
			if meta.ID != "" {
				ids = append(ids, meta.ID)
			}
		}
		if len(ids) == 0 {
			ids = append(ids, cfg.Placer.ID)
		}
		return ids
	})
	mux := http.NewServeMux()
	memberHub.Mount(mux)
	svc.ServePlacerLink(mux)
	httpServer, err := newClusterHTTPServer("placer", cfg.Placer.Listen, cfg.Placer.TLS, mux)
	if err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- httpServer.Serve(ctx, log)
	}()
	svc.Start(ctx)
	go runPlacerRegistryLinks(ctx, regClient, svc, log)
	go runPlacerMemberMeta(ctx, placerGroup, svc, regClient, cfg.Placer.Advertise, log)
	go svc.RegisterLoop(ctx, cfg.Placer.ID, cfg.Placer.Advertise, cfg.Placer.MemberlistLabel)
	log.Info("cluster-ctl placer", "registry", registryAddr, "registry_members", len(links), "registry_tls", registryTLS != nil,
		"group_sources", len(cfg.ImportGroups), "candidates", cfg.Placement.Candidates, "zone_admit_max", cfg.Placement.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Placement.ShuffleSharding))
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func runPlacerMemberMeta(ctx context.Context, group *membergroup.Group, svc *placer.Service, regClient *clusterclient.Registry, advertise string, log *slog.Logger) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	update := func() {
		label := ""
		ready := false
		if err := regClient.Refresh(ctx); err != nil {
			log.Warn("placer: memberlist ready label", "err", err)
		} else if got, err := regClient.ActiveLabel(ctx); err != nil {
			log.Warn("placer: memberlist active label", "err", err)
		} else {
			label = got
			ready = svc.ReadyForLabel(label)
		}
		if err := group.UpdateMeta(membergroup.Meta{
			Role: membergroup.RolePlacer, ID: group.Name(), Advertise: advertise,
			Ready: ready, ReadyLabel: label,
		}); err != nil {
			log.Debug("placer: memberlist meta update", "err", err)
		}
	}
	update()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			update()
		}
	}
}

func runPlacerRegistryLinks(ctx context.Context, regClient *clusterclient.Registry, svc *placer.Service, log *slog.Logger) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	refresh := func() {
		if err := regClient.Refresh(ctx); err != nil {
			log.Warn("placer: membership refresh", "err", err)
			return
		}
		eps, err := regClient.OwnerEndpoints(ctx)
		if err != nil {
			log.Warn("placer: owner endpoints", "err", err)
			return
		}
		nodeListEps, err := regClient.NodeListEndpoints(ctx)
		if err != nil {
			log.Warn("placer: node_list endpoints", "err", err)
			return
		}
		label, err := regClient.ActiveLabel(ctx)
		if err != nil {
			log.Warn("placer: active label", "err", err)
			return
		}
		svc.SetRegistryLinks(ctx, registryLinks(eps))
		svc.SetNodeListLinksForLabel(ctx, registryLinks(nodeListEps), label)
	}
	refresh()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			refresh()
		}
	}
}

func newPlacerLinkResolver(regClient *clusterclient.Registry) func(context.Context, string) ([]placer.RegistryLink, error) {
	return func(ctx context.Context, recordKey string) ([]placer.RegistryLink, error) {
		eps, err := regClient.PlacerLinkEndpoints(ctx, recordKey)
		if err != nil {
			return nil, err
		}
		return registryLinks(eps), nil
	}
}

func registryLinks(eps []clusterclient.Endpoint) []placer.RegistryLink {
	links := make([]placer.RegistryLink, 0, len(eps))
	for _, ep := range eps {
		links = append(links, placer.RegistryLink{Name: ep.MemberID, BaseURL: ep.BaseURL, Client: ep.Client})
	}
	return links
}

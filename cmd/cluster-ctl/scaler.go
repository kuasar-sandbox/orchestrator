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

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/membergroup"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/scaler"
)

// runScaler starts the standalone scaler: it uses registry membership as the
// bootstrap source, keeps node/group placement views synced, and answers registry
// placement requests.
func runScaler(args []string, log *slog.Logger) error {
	fs := flag.NewFlagSet("scaler", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/scaler.yaml", "config file")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadScaler(*cfgPath)
	if err != nil {
		return err
	}
	registryAddr := cfg.Registry.Bootstrap

	// registry mTLS to dial when remote; a UDS endpoint stays plain.
	var registryTLS *tls.Config
	if endpointServerName(registryAddr) != "" && cfg.Registry.TLS.Enabled() {
		if registryTLS, err = cfg.Registry.TLS.ClientConfig(endpointServerName(registryAddr)); err != nil {
			return fmt.Errorf("scaler: registry tls: %w", err)
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
		return fmt.Errorf("scaler: registry membership has no members")
	}
	if len(cfg.ImportGroups) == 0 {
		return fmt.Errorf("scaler: standalone mode requires at least one import_groups source")
	}
	nodeListEps, err := regClient.NodeListEndpoints(ctx)
	if err != nil {
		return err
	}
	links := registryLinks(eps)
	groupInputs, err := scaler.NewConfiguredGroupInputs(cfg.ImportGroups)
	if err != nil {
		return err
	}
	svc := scaler.NewRemoteLinksWithGroups(links, groupInputs.Provider, groupInputs.Sources, cfg.Placement, deadAfter, log)
	svc.SetNodeListLinks(ctx, registryLinks(nodeListEps))
	svc.SetScaleLinkResolver(newScaleLinkResolver(regClient))
	memberHub := membergroup.NewHub()
	memberlistTLS, err := membergroupTLSConfig(cfg.Member.TLS)
	if err != nil {
		return fmt.Errorf("scaler memberlist tls: %w", err)
	}
	scalerGroup, err := membergroup.New(membergroup.Options{
		Label: cfg.Memberlist.Label, Name: cfg.Member.ID, Hub: memberHub, Log: log,
		TLSConfig: memberlistTLS,
		Meta: membergroup.Meta{
			Role: membergroup.RoleScaler, ID: cfg.Member.ID,
			APIAdvertise: cfg.Member.Advertise, MemberlistAdvertise: cfg.Member.Advertise,
		},
	})
	if err != nil {
		return err
	}
	defer scalerGroup.Shutdown()
	svc.SetImportSourceOwnerSource(cfg.Member.ID, func() []string {
		if err := regClient.Refresh(ctx); err != nil {
			return []string{cfg.Member.ID}
		}
		label, err := regClient.ActiveLabel(ctx)
		if err != nil {
			return []string{cfg.Member.ID}
		}
		metas := scalerGroup.ReadyScalers(label)
		ids := make([]string, 0, len(metas))
		for _, meta := range metas {
			if meta.ID != "" {
				ids = append(ids, meta.ID)
			}
		}
		if len(ids) == 0 {
			ids = append(ids, cfg.Member.ID)
		}
		return ids
	})
	mux := http.NewServeMux()
	memberHub.Mount(mux)
	svc.ServeScaleLink(mux)
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveClusterHTTP(ctx, "scaler", cfg.Member.Listen, cfg.Member.TLS, mux, log)
	}()
	svc.Start(ctx)
	go runScalerRegistryLinks(ctx, regClient, svc, log)
	go runScalerMemberMeta(ctx, scalerGroup, svc, regClient, cfg.Member.Advertise, log)
	go svc.RegisterLoopDynamic(ctx, cfg.Member.ID, cfg.Member.Advertise, cfg.Memberlist.Label, cfg.Member.Advertise)
	log.Info("cluster-ctl scaler", "registry", registryAddr, "registry_members", len(links), "registry_tls", registryTLS != nil,
		"group_sources", len(cfg.ImportGroups), "candidates", cfg.Placement.Candidates, "zone_admit_max", cfg.Placement.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Placement.ShuffleSharding))
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func runScalerMemberMeta(ctx context.Context, group *membergroup.Group, svc *scaler.Service, regClient *clusterclient.Registry, advertise string, log *slog.Logger) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	update := func() {
		ready := svc.Ready()
		label := ""
		if ready {
			if err := regClient.Refresh(ctx); err != nil {
				log.Warn("scaler: memberlist ready label", "err", err)
				ready = false
			} else if got, err := regClient.ActiveLabel(ctx); err != nil {
				log.Warn("scaler: memberlist active label", "err", err)
				ready = false
			} else {
				label = got
			}
		}
		if err := group.UpdateMeta(membergroup.Meta{
			Role: membergroup.RoleScaler, ID: group.Name(),
			APIAdvertise: advertise, MemberlistAdvertise: advertise,
			Ready: ready, ReadyLabel: label,
		}); err != nil {
			log.Debug("scaler: memberlist meta update", "err", err)
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

func runScalerRegistryLinks(ctx context.Context, regClient *clusterclient.Registry, svc *scaler.Service, log *slog.Logger) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	refresh := func() {
		if err := regClient.Refresh(ctx); err != nil {
			log.Warn("scaler: membership refresh", "err", err)
			return
		}
		eps, err := regClient.OwnerEndpoints(ctx)
		if err != nil {
			log.Warn("scaler: owner endpoints", "err", err)
			return
		}
		nodeListEps, err := regClient.NodeListEndpoints(ctx)
		if err != nil {
			log.Warn("scaler: node_list endpoints", "err", err)
			return
		}
		svc.SetRegistryLinks(ctx, registryLinks(eps))
		svc.SetNodeListLinks(ctx, registryLinks(nodeListEps))
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

func newScaleLinkResolver(regClient *clusterclient.Registry) func(context.Context, string) ([]scaler.RegistryLink, error) {
	return func(ctx context.Context, recordKey string) ([]scaler.RegistryLink, error) {
		eps, err := regClient.ScaleLinkEndpoints(ctx, recordKey)
		if err != nil {
			return nil, err
		}
		return registryLinks(eps), nil
	}
}

func registryLinks(eps []clusterclient.Endpoint) []scaler.RegistryLink {
	links := make([]scaler.RegistryLink, 0, len(eps))
	for _, ep := range eps {
		links = append(links, scaler.RegistryLink{Name: ep.MemberID, BaseURL: ep.BaseURL, Client: ep.Client})
	}
	return links
}

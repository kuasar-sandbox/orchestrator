package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/scaler"
)

// runScaler starts the standalone scaler: it uses registry membership as the
// bootstrap source, keeps node/group placement views synced, and answers registry
// placement requests.
func runScaler(args []string, log *slog.Logger) error {
	if len(args) > 0 && args[0] == "import" {
		return scalerImportCmd(args[1:])
	}
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
	nodeListEps, err := regClient.NodeListEndpoints(ctx)
	if err != nil {
		return err
	}
	links := registryLinks(eps)
	svc := scaler.NewRemoteLinks(links, cfg.Placement, deadAfter, log)
	svc.SetNodeListLinks(ctx, registryLinks(nodeListEps))
	mux := http.NewServeMux()
	svc.ServeScaleLink(mux)
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveClusterHTTP(ctx, "scaler", cfg.Member.Listen, cfg.Member.TLS, mux, log)
	}()
	svc.Start(ctx)
	go runScalerRegistryLinks(ctx, regClient, svc, log)
	go svc.RegisterLoopDynamic(ctx, cfg.Member.ID, cfg.Member.Advertise, func(ctx context.Context) (string, error) {
		if err := regClient.Refresh(ctx); err != nil {
			return "", err
		}
		return regClient.ActiveLabel(ctx)
	})
	log.Info("cluster-ctl scaler", "registry", registryAddr, "registry_members", len(links), "registry_tls", registryTLS != nil,
		"candidates", cfg.Placement.Candidates, "zone_admit_max", cfg.Placement.ZoneAdmitMax,
		"shuffle_rules", len(cfg.Placement.ShuffleSharding))
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
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

func registryLinks(eps []clusterclient.Endpoint) []scaler.RegistryLink {
	links := make([]scaler.RegistryLink, 0, len(eps))
	for _, ep := range eps {
		links = append(links, scaler.RegistryLink{Name: ep.MemberID, BaseURL: ep.BaseURL, Client: ep.Client})
	}
	return links
}

func scalerImportCmd(args []string) error {
	fs := flag.NewFlagSet("scaler import", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/scaler.yaml", "scaler config file")
	endpoint := fs.String("endpoint", "", "scaler endpoint override")
	inPath := fs.String("i", "", "input JSONL file (default stdin)")
	_ = fs.Parse(args)

	cfg, err := clustercfg.LoadScaler(*cfgPath)
	if err != nil {
		return err
	}
	addr := cfg.Member.Listen
	if *endpoint != "" {
		addr = *endpoint
	}
	if addr == "" {
		return fmt.Errorf("scaler endpoint is empty")
	}
	var tlsCfg *tls.Config
	if endpointServerName(addr) != "" && cfg.Member.TLS.Enabled() {
		tlsCfg, err = cfg.Member.TLS.ClientConfig(endpointServerName(addr))
		if err != nil {
			return fmt.Errorf("scaler tls: %w", err)
		}
	}
	base, client := controlHTTPClient(addr, tlsCfg)
	r := io.Reader(os.Stdin)
	var f *os.File
	if *inPath != "" {
		f, err = os.Open(*inPath)
		if err != nil {
			return err
		}
		defer f.Close()
		r = f
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+scaler.GroupImportPath, r)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("scaler import: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	_, err = os.Stdout.Write(body)
	return err
}

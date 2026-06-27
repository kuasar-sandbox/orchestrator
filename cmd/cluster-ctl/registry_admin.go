package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

func registryAdminCmd(args []string) error {
	switch args[0] {
	case "export":
		return registryExportCmd(args[1:])
	case "import":
		return registryImportCmd(args[1:])
	default:
		return fmt.Errorf("usage: cluster-ctl registry {export|import} [flags]")
	}
}

func registryExportCmd(args []string) error {
	fs := flag.NewFlagSet("registry export", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/registry.yaml", "registry config file")
	endpoint := fs.String("endpoint", "", "registry route_link endpoint override")
	kind := fs.String("kind", "all", "records to export: all|groups|routes")
	group := fs.String("group", "", "exact sandbox-group filter")
	includeSecrets := fs.String("include-secrets", registry.SnapshotIncludeSecretsRedacted, "secret export mode: redacted|inline")
	outPath := fs.String("o", "", "output JSONL file (default stdout)")
	_ = fs.Parse(args)

	base, client, err := registryRouteLinkClient(*cfgPath, *endpoint)
	if err != nil {
		return err
	}
	q := url.Values{}
	q.Set("kind", *kind)
	if *group != "" {
		q.Set("group", *group)
	}
	q.Set("include_secrets", *includeSecrets)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+registry.RouteLinkExportPath+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("registry export: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	w := io.Writer(os.Stdout)
	var f *os.File
	if *outPath != "" {
		f, err = os.OpenFile(*outPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	_, err = io.Copy(w, resp.Body)
	return err
}

func registryImportCmd(args []string) error {
	fs := flag.NewFlagSet("registry import", flag.ExitOnError)
	cfgPath := fs.String("config", "/etc/cluster-ctl/registry.yaml", "registry config file")
	endpoint := fs.String("endpoint", "", "registry route_link endpoint override")
	inPath := fs.String("i", "", "input JSONL file (default stdin)")
	_ = fs.Parse(args)

	base, client, err := registryRouteLinkClient(*cfgPath, *endpoint)
	if err != nil {
		return err
	}
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
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, base+registry.RouteLinkImportPath, r)
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
		return fmt.Errorf("registry import: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	_, err = os.Stdout.Write(body)
	return err
}

func registryRouteLinkClient(cfgPath, override string) (string, *http.Client, error) {
	cfg, err := clustercfg.LoadRegistry(cfgPath)
	if err != nil {
		return "", nil, err
	}
	addr := cfg.RouteLink.Listen
	if override != "" {
		addr = override
	}
	if addr == "" {
		return "", nil, fmt.Errorf("registry route_link endpoint is empty")
	}
	var tlsCfg *tls.Config
	if !strings.HasPrefix(addr, "/") && cfg.RouteLink.TLS.Enabled() {
		host := addr
		if i := strings.LastIndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		tlsCfg, err = cfg.RouteLink.TLS.ClientConfig(host)
		if err != nil {
			return "", nil, fmt.Errorf("registry route_link tls: %w", err)
		}
	}
	base, client := controlHTTPClient(addr, tlsCfg)
	return base, client, nil
}

func controlHTTPClient(addr string, tlsCfg *tls.Config) (string, *http.Client) {
	tr := &http.Transport{}
	base := "http://" + addr
	switch {
	case strings.HasPrefix(addr, "/"):
		base = "http://registry"
		tr = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}}
	case tlsCfg != nil:
		base = "https://" + addr
		tr = &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true}
	}
	return base, &http.Client{Timeout: 60 * time.Second, Transport: tr}
}

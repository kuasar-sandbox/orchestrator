package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/finalrouter"
	"github.com/kuasar-sandbox/orchestrator/internal/providerclient"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeclient"
)

func runRouter(args []string, log *slog.Logger) error {
	flags := flag.NewFlagSet("router", flag.ContinueOnError)
	configPath := flags.String("config", "/etc/cluster-ctl/router.yaml", "config file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("router: unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	config, err := clustercfg.LoadConsensusRouter(*configPath)
	if err != nil {
		return err
	}
	chain, err := raftstore.LoadSignedRegistryLayoutChain(config.RegistryLayout.Chain)
	if err != nil {
		return err
	}
	keys, err := raftstore.LoadRegistryLayoutKeyring(config.RegistryLayout.Keys)
	if err != nil {
		return err
	}
	accepted, err := (raftstore.RegistryLayoutGuard{Path: config.RegistryLayout.Guard}).AcceptSignedChain(chain, keys)
	if err != nil {
		return err
	}
	latest := chain[len(chain)-1]
	digest, err := latest.Verify(keys)
	if err != nil {
		return err
	}
	if accepted.RegistryGeneration != latest.RegistryLayout.RegistryGeneration || accepted.RegistryLayoutDigest != digest {
		return fmt.Errorf("router: accepted registryLayout lineage does not end at supplied artifact")
	}
	registryEndpoints := make([]routeclient.Endpoint, 0, len(latest.RegistryLayout.Members))
	for _, member := range latest.RegistryLayout.Members {
		client, clientErr := authenticatedHTTPClient(
			config.RegistryTLS, member.InternalEndpoint, config.RegistryResponseTimeoutDuration(),
		)
		if clientErr != nil {
			return clientErr
		}
		registryEndpoints = append(registryEndpoints, routeclient.Endpoint{
			MemberID: member.MemberID, BaseURL: member.InternalEndpoint, Client: client,
		})
	}
	control, err := routeclient.New(latest.RegistryLayout, digest, registryEndpoints)
	if err != nil {
		return err
	}
	providerEndpoints := make([]providerclient.Endpoint, 0, len(config.Providers.Endpoints))
	for _, endpoint := range config.Providers.Endpoints {
		client, clientErr := authenticatedHTTPClient(config.Providers.TLS, endpoint.Endpoint, defaultInternalResponseTimeout)
		if clientErr != nil {
			return clientErr
		}
		providerEndpoints = append(providerEndpoints, providerclient.Endpoint{
			Name: endpoint.Name, BaseURL: endpoint.Endpoint, Client: client,
		})
	}
	providers, err := providerclient.New(providerEndpoints)
	if err != nil {
		return err
	}
	if _, err := control.RefreshPermit(context.Background()); err != nil {
		return fmt.Errorf("router: initial Serve Permit: %w", err)
	}
	router, err := finalrouter.New(control, providers, config.Domain, config.AuthCacheDuration(), log)
	if err != nil {
		return err
	}
	nodeTLS, err := config.NodeTLS.ClientConfig("")
	if err != nil {
		return fmt.Errorf("router: node mTLS: %w", err)
	}
	if err := router.SetNodeTLS(nodeTLS); err != nil {
		return err
	}
	router.SetAuthMode(config.Auth.APIKey)
	router.SetDataPlaneAuth(config.Auth.DataPlane)
	routeTTL, idleTTL := config.CacheDurations()
	router.SetRouteCache(routeTTL, idleTTL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go router.RunCleanup(ctx)
	go func() {
		if runErr := control.Run(ctx); runErr != nil && ctx.Err() == nil {
			log.Error("router Permit refresh stopped", "err", runErr)
			stop()
		}
	}()
	if config.MetricsListen != "" {
		listener, listenErr := net.Listen("tcp", config.MetricsListen)
		if listenErr != nil {
			return listenErr
		}
		server := &http.Server{Handler: http.HandlerFunc(router.Metrics().Handler())}
		go func() {
			<-ctx.Done()
			_ = server.Close()
		}()
		go func() {
			if serveErr := server.Serve(listener); serveErr != nil && serveErr != http.ErrServerClosed {
				log.Error("router metrics stopped", "err", serveErr)
			}
		}()
	}

	listenConfig := net.ListenConfig{Control: func(_, _ string, connection syscall.RawConn) error {
		var optionErr error
		if err := connection.Control(func(fd uintptr) {
			optionErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
		}); err != nil {
			return err
		}
		return optionErr
	}}
	listener, err := listenConfig.Listen(ctx, "tcp", config.Ingress.Listen)
	if err != nil {
		return err
	}
	server := &http.Server{}
	if config.Ingress.TLS.Enabled() {
		tlsConfig, tlsErr := config.Ingress.TLS.ServerConfig()
		if tlsErr != nil {
			return tlsErr
		}
		server.Handler, server.TLSConfig = router.Handler(), tlsConfig
	} else {
		server.Handler = h2c.NewHandler(router.Handler(), &http2.Server{})
	}
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	log.Info("cluster-ctl router", "listen", config.Ingress.Listen, "domain", config.Domain,
		"cluster", latest.RegistryLayout.ClusterID, "registry_generation", latest.RegistryLayout.RegistryGeneration,
		"registry_layout_version", latest.RegistryLayout.RegistryLayoutVersion)
	var serveErr error
	if config.Ingress.TLS.Enabled() {
		serveErr = server.ServeTLS(listener, "", "")
	} else {
		serveErr = server.Serve(listener)
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}
	return nil
}

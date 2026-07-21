package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/controlplane"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
	"github.com/kuasar-sandbox/orchestrator/internal/transportauth"
)

func runRegistry(args []string, log *slog.Logger) error {
	flags := flag.NewFlagSet("registry", flag.ContinueOnError)
	configPath := flags.String("config", "/etc/cluster-ctl/registry.yaml", "config file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("registry: unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	if err := validateDragonboatSoftSettings(*configPath); err != nil {
		return fmt.Errorf("registry: Dragonboat runtime profile: %w", err)
	}
	config, err := clustercfg.LoadConsensusRegistry(*configPath)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runtime, registryLayout, digest, system, err := openConsensusRuntime(ctx, config)
	if err != nil {
		return err
	}
	defer runtime.Close()

	mux := http.NewServeMux()
	var ready atomic.Bool
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			http.Error(w, "Registry registryLayout transition is not complete", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	transitionHandler, err := controlplane.NewReplicaTransitionHandler(runtime)
	if err != nil {
		return err
	}
	transitionMux := http.NewServeMux()
	transitionHandler.Mount(transitionMux)
	if runtime.HasLocalSystemReplica() {
		systemHandler, handlerErr := controlplane.NewSystemRuntimeHandler(runtime)
		if handlerErr != nil {
			return handlerErr
		}
		transitionMux.Handle(controlplane.SystemRuntimePath, systemHandler)
	}
	trustedRegistry := transportauth.Middleware(transportauth.RoleRegistry, transitionMux)
	for _, path := range []string{
		controlplane.ReplicaCatchUpPath, controlplane.ReplicaAppliedPath,
		controlplane.ReplicaPromotedPath, controlplane.SystemRuntimePath,
	} {
		mux.Handle(path, trustedRegistry)
	}
	server, err := newClusterHTTPServer("registry", config.Member.Listen, config.Member.TLS, mux)
	if err != nil {
		return err
	}
	serverErr := make(chan error, 1)
	backgroundErr := make(chan error, 2)
	go func() {
		serveErr := server.Serve(ctx, log)
		serverErr <- serveErr
		if serveErr != nil {
			stop()
		}
	}()
	if system.Transition != nil {
		if err := controlplane.CompleteRegistryLayoutTransition(ctx, runtime, config.Storage.TransitionWorkers, log); err != nil {
			select {
			case serveErr := <-serverErr:
				if serveErr != nil {
					return serveErr
				}
			default:
			}
			return err
		}
		registryLayout, digest, _ = runtime.RegistryLayoutSnapshot()
	}
	store, err := controlplane.NewRaftStore(runtime, registryLayout, digest)
	if err != nil {
		return err
	}
	if err := runtime.SetTerminalProofVerifier(store); err != nil {
		return err
	}
	if _, err := store.RefreshPermit(ctx); err != nil {
		return fmt.Errorf("registry: initial Serve Permit: %w", err)
	}

	directory := session.NewDirectory(store)
	peers, err := registrySessionPeers(config, registryLayout)
	if err != nil {
		return err
	}
	mesh, err := controlplane.NewSessionMesh(config.Member.ID, directory, peers, log)
	if err != nil {
		return err
	}
	holder, err := session.NewHolder(config.Member.ID, config.Session.MaxNodes, time.Now, store, mesh, store)
	if err != nil {
		return err
	}
	mesh.SetHolder(holder)
	recoveryMesh, err := controlplane.NewRecoveryMesh(config.Member.ID, store, peers)
	if err != nil {
		return err
	}
	converger, err := controlplane.NewEventConverger(store)
	if err != nil {
		return err
	}
	nodeLink, err := controlplane.NewNodeLinkServer(holder, store, converger, log)
	if err != nil {
		return err
	}
	if err := nodeLink.SetLimits(config.Session.EventWorkers, config.Session.ReconnectPerSecond); err != nil {
		return err
	}
	reconnect, err := controlplane.NewRegistryLayoutReconnectRouter(config.Member.ID, registryLayout, mesh.MemberAvailable)
	if err != nil {
		return err
	}
	nodeLink.SetReconnectRouter(reconnect)

	planner, err := registryPlacementPlanner(config.Placers)
	if err != nil {
		return err
	}
	prober, err := session.NewDirectoryProber(directory, mesh)
	if err != nil {
		return err
	}
	dispatcher, err := session.NewDirectoryDispatcher(directory, mesh)
	if err != nil {
		return err
	}
	park, poll, permitRefresh, recoveryScan := config.WorkflowDurations()
	serviceConfig := controlplane.DefaultRegistryServiceConfig()
	serviceConfig.ParkTimeout = park
	serviceConfig.PollInterval = poll
	serviceConfig.PermitRefreshInterval = permitRefresh
	serviceConfig.RecoveryScanInterval = recoveryScan
	serviceConfig.RecoveryShardsPerScan = config.Workflow.RecoveryShardsPerScan
	serviceConfig.RecoveryWorkers = config.Workflow.RecoveryWorkers
	serviceConfig.CompactionWorkers = config.Workflow.CompactionWorkers
	serviceConfig.PendingWorkflowsPerPage = config.Workflow.PendingWorkflowsPerPage
	serviceConfig.SandboxLaunchPerSecond = config.Workflow.SandboxLaunchPerSecond
	serviceConfig.SandboxLaunchBurst = config.Workflow.SandboxLaunchBurst
	serviceConfig.BuildLaunchPerSecond = config.Workflow.BuildLaunchPerSecond
	serviceConfig.BuildLaunchBurst = config.Workflow.BuildLaunchBurst
	service, err := controlplane.NewRegistryService(store, planner, prober, dispatcher, mesh, serviceConfig)
	if err != nil {
		return err
	}
	if err := runtime.SetOutboxAckVerifier(service); err != nil {
		return err
	}
	operator, err := controlplane.NewOperatorService(store, holder)
	if err != nil {
		return err
	}

	routerTrust := func(request *http.Request) error {
		return transportauth.VerifyRequest(request, transportauth.RoleRouter)
	}
	operatorTrust := func(request *http.Request) error {
		return transportauth.VerifyRequest(request, transportauth.RoleOperator)
	}
	readHandler := routeapi.NewHandler(service, routerTrust)
	mutationHandler := routeapi.NewMutationHandler(service, routerTrust)
	operatorHandler := controlplane.NewOperatorHandler(operator, operatorTrust)
	for _, path := range []string{routeapi.ReadRoutePath, routeapi.ReadBuildPath} {
		mux.Handle(path, readHandler)
	}
	for _, path := range []string{
		routeapi.PermitPath, routeapi.ReserveRoutePath, routeapi.ResumeRoutePath,
		routeapi.DeleteRoutePath, routeapi.ListRoutesPath, routeapi.WatchRoutesPath, routeapi.RegisterBuildPath,
	} {
		mux.Handle(path, mutationHandler)
	}
	for _, path := range []string{controlplane.OperatorEnrollNodePath, controlplane.OperatorRetireNodePath} {
		mux.Handle(path, operatorHandler)
	}
	for _, path := range []string{
		controlplane.OperatorBeginRecoveryPath, controlplane.OperatorResolveRecoveryNodePath,
		controlplane.OperatorFenceRoutePath,
		controlplane.OperatorCloseRegistryGenerationPath, controlplane.OperatorConfirmDrainPath,
		controlplane.OperatorActivateRegistryGenerationPath, controlplane.OperatorSystemStatePath,
	} {
		mux.Handle(path, operatorHandler)
	}
	sessionMux := http.NewServeMux()
	mesh.Mount(sessionMux)
	recoveryMesh.Mount(sessionMux)
	sessionHandler := transportauth.Middleware(transportauth.RoleRegistry, sessionMux)
	mux.Handle("/internal/session-directory/", sessionHandler)
	mux.Handle("/internal/session-holder/", sessionHandler)
	mux.Handle("/internal/recovery/", sessionHandler)
	mux.Handle(routesync.NodeLinkPath, transportauth.Middleware(transportauth.RoleNode, nodeLink))

	go mesh.Run(ctx, config.AntiEntropyDuration())
	recoveryConfig := controlplane.DefaultRecoveryCoordinatorConfig()
	recoveryConfig.Interval = recoveryScan
	recoveryConfig.Workers = config.Workflow.RecoveryWorkers
	recoveryConfig.PerNodeWorkers = config.Workflow.RecoveryPerNodeWorkers
	recoveryConfig.PageObjects = config.Workflow.RecoveryPageObjects
	recoveryConfig.PageBytes = config.Workflow.RecoveryPageBytes
	recoveryConfig.MaxNodeReportBytes = config.Workflow.RecoveryMaxReportBytes
	recoveryConfig.BytesPerSecond = config.Workflow.RecoveryBytesPerSecond
	recoveryConfig.LookupPage = config.Workflow.RecoveryLookupPage
	recovery, err := controlplane.NewRecoveryCoordinator(store, recoveryMesh, directory, mesh, recoveryConfig, log)
	if err != nil {
		return err
	}
	reportBackgroundError := func(component string, runErr error) {
		if ctx.Err() != nil {
			return
		}
		if runErr == nil {
			runErr = errors.New("stopped before Registry shutdown")
		}
		fatal := fmt.Errorf("registry %s stopped: %w", component, runErr)
		log.Error("registry background service stopped", "component", component, "err", runErr)
		backgroundErr <- fatal
		stop()
	}
	go func() {
		reportBackgroundError("recovery coordinator", recovery.Run(ctx))
	}()
	go func() {
		reportBackgroundError("workflow service", service.Run(ctx))
	}()
	ready.Store(true)
	log.Info("cluster-ctl registry",
		"member", config.Member.ID, "listen", config.Member.Listen,
		"cluster", registryLayout.ClusterID, "registry_generation", registryLayout.RegistryGeneration,
		"registry_layout_version", registryLayout.RegistryLayoutVersion, "virtual_shards", registryLayout.VirtualShardCount,
	)
	return waitRegistryExit(stop, serverErr, backgroundErr)
}

func waitRegistryExit(stop context.CancelFunc, serverErr, backgroundErr <-chan error) error {
	select {
	case fatal := <-backgroundErr:
		stop()
		return errors.Join(fatal, <-serverErr)
	case serveErr := <-serverErr:
		if serveErr != nil {
			return serveErr
		}
		select {
		case fatal := <-backgroundErr:
			stop()
			return fatal
		default:
			return nil
		}
	}
}

func openConsensusRuntime(
	ctx context.Context,
	config *clustercfg.ConsensusRegistryConfig,
) (*raftstore.Runtime, raftstore.RegistryLayout, string, raftstore.SystemState, error) {
	chain, err := raftstore.LoadSignedRegistryLayoutChain(config.RegistryLayout.Chain)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	keyring, err := raftstore.LoadRegistryLayoutKeyring(config.RegistryLayout.Keys)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	latest := chain[len(chain)-1]
	digest, err := latest.Verify(keyring)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	allPeers, err := registryRegistryLayoutPeers(
		config.Member.TLS, latest.RegistryLayout, registryPeerResponseTimeout(config.Storage),
	)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	transitionClient, err := controlplane.NewReplicaTransitionClient(allPeers)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	systemPeers, err := registrySystemPeers(latest.RegistryLayout, allPeers)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	systemClient, err := controlplane.NewRemoteSystemClient(systemPeers)
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	attestor := raftstore.StorageAttestor(raftstore.LinuxDMStorageAttestor{})
	if config.Storage.StorageProtection == "ephemeral-tmpfs" {
		attestor = raftstore.LinuxEphemeralStorageAttestor{}
	}
	tuning := raftstore.DefaultRuntimeTuning()
	tuning.SnapshotWorkers = uint64(config.Storage.SnapshotWorkers)
	tuning.OperationTimeoutMillis = uint64(config.Storage.OperationTimeoutDuration() / time.Millisecond)
	tuning.FenceRetentionMillis = uint64(config.Storage.FenceRetentionDuration() / time.Millisecond)
	runtimeConfig := raftstore.RuntimeConfig{
		MemberID:    config.Member.ID,
		NodeHostDir: config.Storage.NodeHostDir, WALDir: config.Storage.WALDir,
		StateEngineDir: config.Storage.StateEngineDir, ListenAddress: config.Storage.RaftListen,
		RegistryLayoutGuardPath: config.RegistryLayout.Guard, EnrollmentPath: config.Storage.EnrollmentPath,
		TLS: raftstore.RaftTLS{
			CAFile: config.Storage.TLS.CA, CertFile: config.Storage.TLS.Cert, KeyFile: config.Storage.TLS.Key,
		},
		Tuning: tuning, StorageAttestor: attestor,
	}
	mode := raftstore.RuntimeRestart
	switch config.Storage.OpenMode {
	case "bootstrap":
		mode = raftstore.RuntimeBootstrap
	case "join":
		mode = raftstore.RuntimeJoin
	}
	var bootstrapSecret []byte
	if mode == raftstore.RuntimeBootstrap {
		bootstrapSecret, err = os.ReadFile(config.Storage.BootstrapSecret)
		if err != nil {
			return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
		}
		bootstrapSecret = bytes.TrimSpace(bootstrapSecret)
		if len(bootstrapSecret) == 0 || len(bootstrapSecret) > 4096 {
			return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, errors.New("registry: bootstrap secret has invalid size")
		}
	}
	runtime, err := raftstore.OpenRuntime(runtimeConfig, chain, keyring, raftstore.RuntimeOpenOptions{
		Mode: mode, BootstrapSecret: bootstrapSecret, TransitionClient: transitionClient, SystemClient: systemClient,
	})
	if err != nil {
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	fail := func(err error) (*raftstore.Runtime, raftstore.RegistryLayout, string, raftstore.SystemState, error) {
		runtime.Close()
		return nil, raftstore.RegistryLayout{}, "", raftstore.SystemState{}, err
	}
	if runtime.HasLocalSystemReplica() {
		if err := runtime.StartSystemReplica(); err != nil {
			return fail(err)
		}
	}
	var system raftstore.SystemState
	if mode == raftstore.RuntimeBootstrap && runtime.HasLocalSystemReplica() && latest.RegistryLayout.RegistryLayoutVersion == 1 {
		system, err = runtime.BootstrapSystem(ctx)
	} else {
		system, err = runtime.AwaitOrBeginRegistryLayoutTransition(ctx)
	}
	if err != nil {
		return fail(err)
	}
	if system.Transition != nil && system.Transition.Digest == digest {
		if err := runtime.PlanRegistryLayoutJoins(system); err != nil {
			return fail(err)
		}
		if runtime.HasLocalSystemReplica() {
			if err := runtime.StartSystemReplica(); err != nil {
				return fail(err)
			}
		}
	}
	if err := runtime.StartDataReplicas(system); err != nil {
		return fail(err)
	}
	if mode == raftstore.RuntimeBootstrap {
		if err := runtime.InitializeDataShards(ctx, system, config.Storage.InitializeWorkers); err != nil {
			return fail(err)
		}
	}
	selectedRegistryLayout, selectedDigest, _ := runtime.RegistryLayoutSnapshot()
	return runtime, selectedRegistryLayout, selectedDigest, system, nil
}

func registrySessionPeers(
	config *clustercfg.ConsensusRegistryConfig,
	registryLayout raftstore.RegistryLayout,
) ([]controlplane.SessionPeer, error) {
	peers := make([]controlplane.SessionPeer, 0, len(registryLayout.Members)-1)
	for _, member := range registryLayout.Members {
		if member.MemberID == config.Member.ID {
			continue
		}
		client, err := authenticatedHTTPClient(config.Member.TLS, member.InternalEndpoint, defaultInternalResponseTimeout)
		if err != nil {
			return nil, err
		}
		peers = append(peers, controlplane.SessionPeer{
			MemberID: member.MemberID, Endpoint: member.InternalEndpoint, Client: client,
		})
	}
	return peers, nil
}

func registryRegistryLayoutPeers(
	material clustercfg.TLS,
	registryLayout raftstore.RegistryLayout,
	responseTimeout time.Duration,
) ([]controlplane.SessionPeer, error) {
	peers := make([]controlplane.SessionPeer, 0, len(registryLayout.Members))
	for _, member := range registryLayout.Members {
		client, err := authenticatedHTTPClient(material, member.InternalEndpoint, responseTimeout)
		if err != nil {
			return nil, err
		}
		peers = append(peers, controlplane.SessionPeer{
			MemberID: member.MemberID, Endpoint: member.InternalEndpoint, Client: client,
		})
	}
	return peers, nil
}

func registryPeerResponseTimeout(storage clustercfg.ConsensusStorage) time.Duration {
	return time.Duration(raftstore.MaximumServePermitMillis)*time.Millisecond + storage.OperationTimeoutDuration()
}

func registrySystemPeers(
	registryLayout raftstore.RegistryLayout,
	all []controlplane.SessionPeer,
) ([]controlplane.SessionPeer, error) {
	byID := make(map[string]controlplane.SessionPeer, len(all))
	for _, peer := range all {
		byID[peer.MemberID] = peer
	}
	peers := make([]controlplane.SessionPeer, 0, len(registryLayout.SystemReplicas))
	for _, replica := range registryLayout.SystemReplicas {
		peer, found := byID[replica.MemberID]
		if !found {
			return nil, errors.New("registry: System replica is absent from the verified member clients")
		}
		peers = append(peers, peer)
	}
	return peers, nil
}

func registryPlacementPlanner(set clustercfg.EndpointSet) (*controlplane.HTTPPlacementPlanner, error) {
	endpoints := make([]controlplane.PlannerEndpoint, 0, len(set.Endpoints))
	for _, configured := range set.Endpoints {
		client, err := authenticatedHTTPClient(set.TLS, configured.Endpoint, defaultInternalResponseTimeout)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, controlplane.PlannerEndpoint{
			Name: configured.Name, Endpoint: configured.Endpoint, Client: client,
		})
	}
	return controlplane.NewHTTPPlacementPlanner(endpoints)
}

const defaultInternalResponseTimeout = 5 * time.Second

func authenticatedHTTPClient(material clustercfg.TLS, endpoint string, responseTimeout time.Duration) (*http.Client, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Hostname() == "" {
		return nil, errors.New("cluster-ctl: authenticated endpoint must be an HTTPS URL")
	}
	tlsConfig, err := material.ClientConfig(parsed.Hostname())
	if err != nil {
		return nil, err
	}
	return boundedAuthenticatedHTTPClient(tlsConfig, responseTimeout), nil
}

func boundedAuthenticatedHTTPClient(tlsConfig *tls.Config, responseTimeout time.Duration) *http.Client {
	if responseTimeout <= 0 {
		responseTimeout = defaultInternalResponseTimeout
	}
	dialer := &net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}
	return &http.Client{
		Timeout: responseTimeout + time.Second,
		Transport: &http.Transport{
			DialContext:           dialer.DialContext,
			TLSClientConfig:       tlsConfig,
			TLSHandshakeTimeout:   2 * time.Second,
			ResponseHeaderTimeout: responseTimeout,
			ExpectContinueTimeout: time.Second,
			IdleConnTimeout:       90 * time.Second,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   32,
			ForceAttemptHTTP2:     true,
		},
	}
}

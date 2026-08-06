package orch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func clusterCreateCommand(fingerprint, sid string) *routesync.Command {
	return &routesync.Command{
		CmdID: "create-" + sid, Kind: routesync.CmdCreate, SID: sid,
		TemplateRef: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		Profile:     string(types.ProfileBare), APISecretFingerprint: fingerprint,
		Cluster: &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
	}
}

func TestClusterCreateAckFollowsDurableStartingAcceptance(t *testing.T) {
	cfg := &config.Config{}
	started := make(chan struct{}, 1)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(fingerprint, "stable-g0")
	events, stopEvents := o.Subscribe()
	defer stopEvents()

	ack := o.HandleCommand(ctx, cmd)
	if ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster create ack = %+v", ack)
	}
	stored, err := o.st.Get(ctx, cmd.SID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" || stored.VswitchPort != "" {
		t.Fatalf("row at accepted Ack = %+v, %v", stored, err)
	}
	attempt, found := o.launches.Lookup(cmd.SID)
	if !found || attempt.Kind() != launchCreate {
		t.Fatal("accepted cluster create has no active create attempt")
	}
	initial := <-events
	if initial.Kind != routesync.TypeUpsert || initial.Route.State != routesync.StateStarting || initial.Route.FloatingIP != "" {
		t.Fatalf("initial cluster starting route = %+v", initial)
	}

	waitForLauncherStart(t, started)
	stored, err = o.st.Get(ctx, cmd.SID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" || stored.VswitchPort == "" {
		t.Fatalf("cluster pre-assignment row = %+v, %v", stored, err)
	}
	close(startGate)
	waitForSandbox(t, o, ctx, cmd.SID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning && current.RunID != ""
	}, "cluster create running")
}

func TestClusterCreatePreAssignmentFailurePublishesDelete(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.vs = failingCreateVS{err: errors.New("cluster attach failed")}
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(fingerprint, "stable-g1")
	events, stopEvents := o.Subscribe()
	defer stopEvents()

	if ack := o.HandleCommand(ctx, cmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster create ack = %+v", ack)
	}
	dead := waitForSandbox(t, o, ctx, cmd.SID, func(current *types.Sandbox) bool {
		return current.State == types.StateDead
	}, "cluster create dead after attach failure")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("failed cluster create retained ownership: %+v", dead)
	}
	for _, want := range []string{routesync.StateStarting, routesync.TypeDelete} {
		select {
		case event := <-events:
			if want == routesync.TypeDelete {
				if event.Kind != routesync.TypeDelete || event.SID != cmd.SID {
					t.Fatalf("cluster failure event = %+v", event)
				}
			} else if event.Kind != routesync.TypeUpsert || event.Route.State != want {
				t.Fatalf("cluster failure event = %+v", event)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing cluster %s event", want)
		}
	}
}

func TestConcurrentClusterCreateHasOneLaunchOwner(t *testing.T) {
	cfg := &config.Config{}
	started := make(chan struct{}, 8)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)

	const callers = 8
	results := make(chan *routesync.CmdAck, callers)
	release := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			ready.Done()
			<-release
			cmd := clusterCreateCommand(fingerprint, "stable-g2")
			cmd.CmdID = "concurrent-" + string(rune('a'+i))
			results <- o.HandleCommand(ctx, cmd)
		}(i)
	}
	ready.Wait()
	close(release)
	accepted := 0
	for range callers {
		if ack := <-results; ack.Status == routesync.AckAccepted {
			accepted++
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted concurrent creates = %d, want 1", accepted)
	}
	waitForLauncherStart(t, started)
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("cluster launcher starts = %d, want 1", got)
	}
	if _, found := o.launches.Lookup("stable-g2"); !found {
		t.Fatal("winning cluster create lost its active launch owner")
	}
	close(startGate)
	waitForSandbox(t, o, ctx, "stable-g2", func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "concurrent cluster create winner running")
}

func TestCreateClusterWrapperUsesAcceptedAttempt(t *testing.T) {
	cfg := &config.Config{}
	started := make(chan struct{}, 1)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := clusterCreateCommand(fingerprint, "stable-g3")
	type result struct {
		sb  *types.Sandbox
		err error
	}
	done := make(chan result, 1)
	go func() {
		sb, err := o.CreateCluster(ctx, cmd)
		done <- result{sb: sb, err: err}
	}()
	waitForLauncherStart(t, started)
	stored, err := o.st.Get(ctx, cmd.SID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" {
		t.Fatalf("wrapper accepted row = %+v, %v", stored, err)
	}
	select {
	case got := <-done:
		t.Fatalf("CreateCluster returned before its accepted attempt: %+v", got)
	default:
	}
	close(startGate)
	select {
	case got := <-done:
		if got.err != nil || got.sb == nil || got.sb.State != types.StateRunning {
			t.Fatalf("CreateCluster = %+v, %v", got.sb, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CreateCluster did not observe its launch completion")
	}
}

func TestClusterCreateConnectDeleteShareLaunchOwnerAndCleanupFence(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &blockedKillAttachVS{
		entered: make(chan struct{}), gate: make(chan struct{}), detached: make(chan string, 1),
	}
	o.vs = vs
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	create := clusterCreateCommand(fingerprint, "stable-g4")
	events, stopEvents := o.Subscribe()
	defer stopEvents()
	if ack := o.HandleCommand(ctx, create); ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster create ack = %+v", ack)
	}
	if event := <-events; event.Kind != routesync.TypeUpsert || event.Route.State != routesync.StateStarting {
		t.Fatalf("cluster create initial event = %+v", event)
	}
	select {
	case <-vs.entered:
	case <-time.After(time.Second):
		t.Fatal("cluster create did not enter blocked attach")
	}
	attempt, found := o.launches.Lookup(create.SID)
	if !found || attempt.Kind() != launchCreate {
		t.Fatal("cluster create has no active owner")
	}

	connect := &routesync.Command{
		CmdID: "connect-existing-starting", Kind: routesync.CmdConnect, SID: create.SID,
		Profile: create.Profile, APISecretFingerprint: fingerprint, Cluster: create.Cluster,
	}
	if ack := o.HandleCommand(ctx, connect); ack.Status != routesync.AckAccepted || ack.Connect == nil {
		t.Fatalf("cluster Connect joining starting = %+v", ack)
	}
	if got, ok := o.launches.Lookup(create.SID); !ok || got != attempt {
		t.Fatal("cluster Connect replaced the active create owner")
	}
	if got := lc.starts.Load(); got != 0 {
		t.Fatalf("blocked preparation started %d runners", got)
	}

	deleteCmd := &routesync.Command{
		CmdID: "delete-existing-starting", Kind: routesync.CmdDelete,
		SID: create.SID, APISecretFingerprint: fingerprint,
	}
	if ack := o.HandleCommand(ctx, deleteCmd); ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster Delete ack = %+v", ack)
	}
	select {
	case event := <-events:
		if event.Kind != routesync.TypeDelete || event.SID != create.SID {
			t.Fatalf("cluster Delete event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("cluster Delete did not publish its terminal event")
	}
	if current, err := o.st.Get(ctx, create.SID); err != nil || current != nil {
		t.Fatalf("cluster row after Delete = %+v, %v", current, err)
	}
	if _, err := o.launches.Claim(ctx, create.SID, launchCreate); !errors.Is(err, errLaunchClaimed) {
		t.Fatalf("claim before late attach cleanup = %v, want cleanup fence", err)
	}

	close(vs.gate)
	select {
	case port := <-vs.detached:
		if port != "late-kill-port" {
			t.Fatalf("late detached port = %q", port)
		}
	case <-time.After(time.Second):
		t.Fatal("cluster late attach was not detached")
	}
	if err := attempt.wait(ctx); err == nil {
		t.Fatal("deleted cluster create reported launch success")
	}
	if stored, err := o.st.Get(ctx, create.SID); err != nil || stored != nil {
		t.Fatalf("cluster launch resurrected after Delete: %+v, %v", stored, err)
	}
	next, err := o.launches.Claim(ctx, create.SID, launchCreate)
	if err != nil {
		t.Fatalf("claim after cluster cleanup: %v", err)
	}
	o.launches.Finish(next, nil)
}

func TestPrecheckClusterRejectsInvalidRestore(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	cmd := &routesync.Command{
		TemplateRef:          types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		Config: map[string]string{
			sandboxcfg.NsRestore: `{"prefetch":"disk"}`,
		},
	}
	if _, _, _, err := o.precheckCluster(context.Background(), cmd); err == nil {
		t.Fatal("cluster create accepted invalid restore policy")
	}
}

func TestPrecheckClusterCheckpointPolicy(t *testing.T) {
	newCommand := func(fingerprint, raw string) *routesync.Command {
		return &routesync.Command{
			SID:                  "stable-g0",
			TemplateRef:          types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
			Profile:              string(types.ProfileBare),
			APISecretFingerprint: fingerprint,
			Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
			Config:               map[string]string{sandboxcfg.NsCheckpoint: raw},
		}
	}

	localCfg := &config.Config{}
	localCfg.Checkpoint.Mode = config.CheckpointLocal
	local := testOrchCfg(t, localCfg)
	_, _, localFingerprint := allowlistedBuildIdentity(t, local)
	cmd := newCommand(localFingerprint, ` { "merge_ref" : false, "drop_caches" : null } `)
	if _, _, _, err := local.precheckCluster(context.Background(), cmd); err != nil {
		t.Fatalf("local checkpoint policy rejected: %v", err)
	}
	if cmd.Config[sandboxcfg.NsCheckpoint] != `{"merge_ref":false}` {
		t.Fatalf("cluster checkpoint policy was not canonicalized: %+v", cmd.Config)
	}
	if _, _, _, err := local.precheckCluster(context.Background(), newCommand(localFingerprint, `{"merge_ref":0}`)); err == nil {
		t.Fatal("malformed cluster checkpoint policy was accepted")
	}

	remoteCfg := &config.Config{}
	remoteCfg.Checkpoint.Mode = config.CheckpointRemote
	remote := testOrchCfg(t, remoteCfg)
	_, _, remoteFingerprint := allowlistedBuildIdentity(t, remote)
	if _, _, _, err := remote.precheckCluster(context.Background(), newCommand(remoteFingerprint, `{"drop_caches":false}`)); err == nil || !strings.Contains(err.Error(), "checkpoint.mode=local") {
		t.Fatalf("remote cluster checkpoint policy error = %v", err)
	}
}

func TestPrecheckClusterRequiresConsistentProfileAndContext(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	templateRef := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
	valid := func() *routesync.Command {
		return &routesync.Command{
			SID:                  "stable-g0",
			TemplateRef:          templateRef,
			Profile:              string(types.ProfileBare),
			APISecretFingerprint: fingerprint,
			Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
		}
	}

	if _, tmpl, _, err := o.precheckCluster(context.Background(), valid()); err != nil {
		t.Fatalf("valid cluster create rejected: %v", err)
	} else if tmpl.Profile != types.ProfileBare {
		t.Fatalf("template profile = %q, want bare", tmpl.Profile)
	}

	tests := map[string]func(*routesync.Command){
		"missing profile": func(cmd *routesync.Command) { cmd.Profile = "" },
		"invalid profile": func(cmd *routesync.Command) { cmd.Profile = "other" },
		"missing context": func(cmd *routesync.Command) { cmd.Cluster = nil },
		"missing group":   func(cmd *routesync.Command) { cmd.Cluster.Group = "" },
		"missing route":   func(cmd *routesync.Command) { cmd.Cluster.RouteKey = "" },
		"profile mismatch": func(cmd *routesync.Command) {
			cmd.Profile = string(types.ProfileE2B)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			cmd := valid()
			mutate(cmd)
			if _, _, _, err := o.precheckCluster(context.Background(), cmd); err == nil {
				t.Fatal("invalid cluster create was accepted")
			}
		})
	}
	if _, _, _, err := o.precheckCluster(context.Background(), nil); err == nil {
		t.Fatal("nil cluster create command was accepted")
	}
}

func TestClusterSandboxMetadataExcludesLegacyClusterIdentity(t *testing.T) {
	input := map[string]string{
		"user-key":                     "user-value",
		clusterstate.ObjectMetadataKey: `{"group":"legacy","route_key":"legacy"}`,
		sandboxcfg.NsCredentials:       `{"service_secret":"` + strings.Repeat("1", 64) + `"}`,
	}
	metadata := clusterSandboxMetadata(input)
	if metadata["user-key"] != "user-value" {
		t.Fatalf("user metadata = %q", metadata["user-key"])
	}
	if _, found := metadata[clusterstate.ObjectMetadataKey]; found {
		t.Fatal("legacy cluster identity remained in sandbox metadata")
	}
	if _, found := metadata[sandboxcfg.NsCredentials]; found {
		t.Fatal("sandbox credentials remained in user metadata")
	}
	if _, found := input[clusterstate.ObjectMetadataKey]; !found {
		t.Fatal("metadata filtering mutated the command config")
	}
}

func TestPrecheckClusterExtractsCredentials(t *testing.T) {
	o := testOrch(t)
	_, _, fingerprint := allowlistedBuildIdentity(t, o)
	secret := strings.Repeat("1", 64)
	cmd := &routesync.Command{
		SID: "stable-g0", TemplateRef: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(), Profile: "e2b",
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
		Config: map[string]string{
			sandboxcfg.NsCredentials: `{"service_secret":"` + secret + `","envd_access_token":"envd","traffic_access_token":"traffic"}`,
			"keep":                   "value",
		},
	}
	_, _, credentials, err := o.precheckCluster(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if credentials.ServiceSecret != secret || credentials.EnvdAccessToken != "envd" || credentials.TrafficAccessToken != "traffic" {
		t.Fatalf("cluster credentials = %+v", credentials)
	}
	if _, found := cmd.Config[sandboxcfg.NsCredentials]; found || cmd.Config["keep"] != "value" {
		t.Fatalf("cluster command config was not separated: %+v", cmd.Config)
	}
}

func TestValidateClusterSandboxContext(t *testing.T) {
	stored := &types.Sandbox{
		ID:                 "stable-g0",
		Profile:            types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		AuthSandboxIDValue: "stable",
	}
	valid := func() *routesync.Command {
		return &routesync.Command{
			Profile: string(types.ProfileBare),
			Cluster: &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
		}
	}
	if err := validateClusterSandboxContext(stored, valid()); err != nil {
		t.Fatalf("matching context rejected: %v", err)
	}

	tests := map[string]func(*types.Sandbox, *routesync.Command){
		"profile": func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Profile = string(types.ProfileE2B) },
		"group":   func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Cluster.Group = "group-b" },
		"route":   func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Cluster.RouteKey = "route-b" },
		"auth id": func(_ *types.Sandbox, cmd *routesync.Command) { cmd.Cluster.AuthSandboxID = "other" },
		"missing stored context": func(sb *types.Sandbox, _ *routesync.Command) {
			sb.Cluster = nil
		},
		"missing command context": func(_ *types.Sandbox, cmd *routesync.Command) {
			cmd.Cluster = nil
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			sb := *stored
			cmd := valid()
			mutate(&sb, cmd)
			if err := validateClusterSandboxContext(&sb, cmd); err == nil {
				t.Fatal("mismatched cluster context was accepted")
			}
		})
	}
}

func TestHandleClusterConnectRejectsContextMismatch(t *testing.T) {
	o := testOrch(t)
	_, manifestKey, fingerprint := allowlistedBuildIdentity(t, o)
	sb := &types.Sandbox{
		ID:                 "stable-g0",
		Profile:            types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		AuthSandboxIDValue: "stable",
		State:              types.StatePaused,
		APISecret:          deriveTestAPISecret(t, manifestKey),
		ManifestKey:        manifestKey,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		CmdID:                "connect-1",
		Kind:                 routesync.CmdConnect,
		SID:                  sb.ID,
		Profile:              string(types.ProfileBare),
		APISecretFingerprint: fingerprint,
		Cluster:              &routesync.ClusterSandboxContext{Group: "other", RouteKey: "route-a", AuthSandboxID: "stable"},
	})
	if ack.Status != routesync.AckRejected || !strings.Contains(ack.Reason, "context mismatch") {
		t.Fatalf("mismatched connect ack = %+v", ack)
	}
}

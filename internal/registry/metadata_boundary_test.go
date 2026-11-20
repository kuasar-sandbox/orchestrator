package registry

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func registryWithTypedFixture(t *testing.T) (*Registry, *metadatastore.Store) {
	t.Helper()
	db := metadatastore.New()
	stores, err := NewStores(db.Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	reg := New(stores, placementWithToken("n1"), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	enableTestCreateAuth(t, reg)
	if err := stores.PutNode(context.Background(), &NodeRecord{NodeID: "n1", APIEndpoint: "node-api", DataEndpoint: "node-data", LastHeartbeatUnix: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	return reg, db
}

type unknownRouteRepository struct {
	metadata.RouteRepository
	intentUnknown, reportUnknown, deleteUnknown bool
	intents, reports, deletes                   int
}

func (f *unknownRouteRepository) CommitIntentAndOwnership(ctx context.Context, expected metadata.Version, record metadata.SandboxRecord, ref metadata.NodeSandboxRef) (metadata.Version, error) {
	f.intents++
	v, err := f.RouteRepository.CommitIntentAndOwnership(ctx, expected, record, ref)
	if err != nil {
		return v, err
	}
	if f.intentUnknown {
		return metadata.Version{}, &metadata.Unknown{Cause: errors.New("reply lost after commit")}
	}
	return v, nil
}
func (f *unknownRouteRepository) ApplyRouteReport(ctx context.Context, registration metadata.Registration, ref metadata.NodeSandboxRef, event routesync.RouteEntry) (metadata.RouteReportResult, error) {
	f.reports++
	v, err := f.RouteRepository.ApplyRouteReport(ctx, registration, ref, event)
	if err != nil {
		return v, err
	}
	if f.reportUnknown {
		return metadata.RouteReportResult{}, &metadata.Unknown{Cause: errors.New("reply lost after commit")}
	}
	return v, nil
}
func (f *unknownRouteRepository) ApplyRouteDelete(ctx context.Context, registration metadata.Registration, expected metadata.Version, ref metadata.NodeSandboxRef, restore *metadata.SandboxRecord) error {
	f.deletes++
	if err := f.RouteRepository.ApplyRouteDelete(ctx, registration, expected, ref, restore); err != nil {
		return err
	}
	if f.deleteUnknown {
		return &metadata.Unknown{Cause: errors.New("reply lost after commit")}
	}
	return nil
}

type unknownBuildRepository struct {
	metadata.BuildRepository
	intentUnknown, ackUnknown bool
	intents, acks             int
}

func (f *unknownBuildRepository) CommitRegistrationIntent(ctx context.Context, b metadata.BuildRecord, ref metadata.NodeBuildRef) (metadata.Version, error) {
	f.intents++
	v, err := f.BuildRepository.CommitRegistrationIntent(ctx, b, ref)
	if err != nil {
		return v, err
	}
	if f.intentUnknown {
		return metadata.Version{}, &metadata.Unknown{Cause: errors.New("intent reply lost")}
	}
	return v, nil
}
func (f *unknownBuildRepository) AcceptBuildRegistration(ctx context.Context, key metadata.BuildKey, node, template string, target *types.BuildTarget) (metadata.Versioned[metadata.BuildRecord], error) {
	f.acks++
	v, err := f.BuildRepository.AcceptBuildRegistration(ctx, key, node, template, target)
	if err != nil {
		return v, err
	}
	if f.ackUnknown {
		return metadata.Versioned[metadata.BuildRecord]{}, &metadata.Unknown{Cause: errors.New("ACK commit reply lost")}
	}
	return v, nil
}

func TestCreateIntentUnknownDoesNotDispatchOrCompensate(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	owner := &recordingNodeOwner{}
	reg.SetNodeOwner(owner)
	faults := &unknownRouteRepository{RouteRepository: reg.stores.routes, intentUnknown: true}
	reg.stores.routes = faults
	_, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "unknown", nil))
	if !errors.Is(err, metadata.ErrUnknown) {
		t.Fatalf("reserve err=%v", err)
	}
	if faults.intents != 1 || len(owner.commands) != 0 {
		t.Fatalf("unknown retried/dispatched: intents=%d commands=%d", faults.intents, len(owner.commands))
	}
	route, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "unknown")
	if err != nil || !found || route.State != StateReserved {
		t.Fatalf("unknown intent compensated: route=%+v found=%v err=%v", route, found, err)
	}
	ref, found, err := reg.stores.GetNodeSandboxRef(context.Background(), "n1", route.NodeSandboxID)
	if err != nil || !found || ref.SandboxID != route.SandboxID {
		t.Fatalf("unknown intent lost ownership: %+v %v %v", ref, found, err)
	}
	if used := db.ObservationReservations(); used != 0 {
		t.Fatalf("observation reservation leaked: %d", used)
	}
}

type notifyingNodeOwner struct {
	*recordingNodeOwner
	command chan *routesync.Command
}

func (o *notifyingNodeOwner) SendCommandAndWait(ctx context.Context, nodeID string, cmd *routesync.Command, timeout time.Duration) (*routesync.CmdAck, error) {
	ack, err := o.recordingNodeOwner.SendCommandAndWait(ctx, nodeID, cmd, timeout)
	o.command <- cmd
	return ack, err
}

func TestCreateIntentUnknownNextRequestRecoversWithoutRebindingOrStaleDelete(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	owner := &notifyingNodeOwner{recordingNodeOwner: &recordingNodeOwner{}, command: make(chan *routesync.Command, 1)}
	reg.SetNodeOwner(owner)
	faults := &unknownRouteRepository{RouteRepository: reg.stores.routes, intentUnknown: true}
	reg.stores.routes = faults
	req := testCreateReserve("/g", "unknown-recovery", nil)
	if _, err := reg.ReserveSandbox(ctx, req); !errors.Is(err, metadata.ErrUnknown) {
		t.Fatalf("first result=%v", err)
	}
	committed, _, found, err := reg.stores.GetSandbox(ctx, req.Group, req.RouteKey)
	if err != nil || !found || len(owner.commands) != 0 {
		t.Fatalf("ambiguous intent missing or dispatched: found=%v commands=%d err=%v", found, len(owner.commands), err)
	}
	faults.intentUnknown = false
	type response struct {
		result *ReserveResult
		err    error
	}
	done := make(chan response, 1)
	go func() { result, err := reg.ReserveSandbox(ctx, req); done <- response{result, err} }()
	var command *routesync.Command
	select {
	case command = <-owner.command:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if command.Kind != routesync.CmdCreate || command.Cluster.StableID != committed.SandboxID || command.SID != EncodeNodeSandboxID(committed.SandboxID, committed.NextSandboxGeneration) {
		t.Fatalf("recovery changed lineage or reused ambiguous generation: %+v", command)
	}
	reported := testE2BRoute(committed.SandboxID, routesync.StateRunning)
	reported.SandboxID = command.SID
	frameCtx := testNodeSession(ctx, reg.stores, committed.NodeID)
	reg.applyRoute(frameCtx, committed.NodeID, &reported)
	var result response
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if result.err != nil || result.result == nil || result.result.Route.SandboxID != committed.SandboxID || result.result.Route.NodeSandboxID != command.SID || faults.intents != 2 || len(owner.commands) != 1 || db.ObservationReservations() != 0 {
		t.Fatalf("recovery result=%+v err=%v intents=%d commands=%d waits=%d", result.result, result.err, faults.intents, len(owner.commands), db.ObservationReservations())
	}
	// The older intent's exact ref remains until its own lifecycle cleanup.
	// Even if that instance later reports running, it cannot overwrite or
	// delete the successfully recovered higher generation.
	late := testE2BRoute(committed.SandboxID, routesync.StateRunning)
	late.SandboxID = committed.NodeSandboxID
	reg.applyRoute(frameCtx, committed.NodeID, &late)
	reg.applyDeleteBySID(frameCtx, committed.NodeID, committed.NodeSandboxID)
	current, revision, found, err := reg.stores.GetSandbox(ctx, req.Group, req.RouteKey)
	if err != nil || !found || current.State != StateReady || current.NodeSandboxID != command.SID || revision != result.result.Route.RouteRevision {
		t.Fatalf("late old generation changed recovered route: %+v %v %v %v", current, revision, found, err)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, committed.NodeID, committed.NodeSandboxID); err != nil || found {
		t.Fatalf("old generation was not cleaned: %v %v", found, err)
	}
	if _, found, err := reg.stores.GetNodeSandboxRef(ctx, committed.NodeID, command.SID); err != nil || !found {
		t.Fatalf("new generation ownership lost: %v %v", found, err)
	}
	if len(owner.deleted) != 1 || owner.deleted[0] != committed.NodeID+"/"+committed.NodeSandboxID+"/"+committed.APISecretFingerprint {
		t.Fatalf("orphan deletion targeted wrong generation: %v", owner.deleted)
	}
}

func TestReadyUnknownDoesNotFinishOrDeleteOrphan(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	owner := &recordingNodeOwner{}
	reg.SetNodeOwner(owner)
	route := testSandboxIdentityRecord("/g", "ready-unknown", "sb-ready", "n1", StateReserved)
	if _, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{}); err != nil || !ok {
		t.Fatalf("seed intent: %v %v", ok, err)
	}
	call := &reserveCall{done: make(chan struct{}), group: route.Group, routeKey: route.RouteKey, expectedSandboxID: route.SandboxID}
	reg.inflight[flightKey(route.Group, route.RouteKey)] = call
	faults := &unknownRouteRepository{RouteRepository: reg.stores.routes, reportUnknown: true}
	reg.stores.routes = faults
	report := testE2BRoute(route.SandboxID, routesync.StateRunning)
	reg.applyRoute(testNodeSession(ctx, reg.stores, "n1"), "n1", &report)
	select {
	case <-call.done:
		t.Fatal("unknown READY finished request")
	default:
	}
	if faults.reports != 1 || len(owner.deleted) != 0 {
		t.Fatalf("unknown READY replayed or deleted orphan: reports=%d deleted=%v", faults.reports, owner.deleted)
	}
	actual, _, found, err := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey)
	if err != nil || !found || actual.State != StateReady {
		t.Fatalf("committed READY lost: %+v %v %v", actual, found, err)
	}
	recovered, err := reg.ReserveSandbox(ctx, testCreateReserve(route.Group, route.RouteKey, nil))
	if err != nil || recovered == nil || recovered.Route.SandboxID != route.SandboxID || len(owner.commands) != 0 || faults.reports != 1 {
		t.Fatalf("next request did not recover committed READY: %+v %v", recovered, err)
	}
}

func TestDeleteUnknownDoesNotRetryIntoLaterIdentity(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	route := testSandboxIdentityRecord("/g", "delete-unknown", "sb-delete", "n1", StateReserved)
	if _, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{}); err != nil || !ok {
		t.Fatalf("seed: %v %v", ok, err)
	}
	faults := &unknownRouteRepository{RouteRepository: reg.stores.routes, deleteUnknown: true}
	reg.stores.routes = faults
	if reg.applyDelete(testNodeSession(ctx, reg.stores, route.NodeID), route.NodeID, route.NodeSandboxID, route.Group, route.RouteKey, route.APISecretFingerprint) {
		t.Fatal("unknown delete reported definite completion")
	}
	if faults.deletes != 1 {
		t.Fatalf("delete replayed %d times", faults.deletes)
	}
	successor := testSandboxIdentityRecord(route.Group, route.RouteKey, "successor", "n1", StateReserved)
	if _, ok, err := reg.stores.commitSandboxIntent(ctx, successor, metadata.Version{}); err != nil || !ok {
		t.Fatalf("successor: %v %v", ok, err)
	}
	got, _, found, err := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey)
	if err != nil || !found || got.SandboxID != successor.SandboxID {
		t.Fatalf("successor lost: %+v %v %v", got, found, err)
	}
}

func TestBuildIntentAndAcceptedUnknownDoNotReplay(t *testing.T) {
	for _, stage := range []string{"intent", "ack"} {
		t.Run(stage, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			owner := &recordingNodeOwner{runtime: map[string]*NodeRecord{"n1": {NodeID: "n1", APIEndpoint: "node-api", DataEndpoint: "node-data"}}}
			reg.SetNodeOwner(owner)
			faults := &unknownBuildRepository{BuildRepository: reg.stores.builds, intentUnknown: stage == "intent", ackUnknown: stage == "ack"}
			reg.stores.builds = faults
			req := BuildReserveReq{Group: "/g", BuildID: "b-unknown", TemplateID: "transient-unknown", Profile: types.ProfileE2B, Resources: testWireBuildResources()}
			_, err := reg.ReserveBuild(context.Background(), req)
			if !errors.Is(err, metadata.ErrUnknown) {
				t.Fatalf("reserve error=%v", err)
			}
			wantCommands := 0
			if stage == "ack" {
				wantCommands = 1
			}
			if len(owner.commands) != wantCommands || faults.intents != 1 {
				t.Fatalf("unexpected replay: commands=%d intents=%d", len(owner.commands), faults.intents)
			}
			record, found, err := reg.stores.GetBuildInGroup(context.Background(), "/g", "b-unknown")
			if err != nil || !found {
				t.Fatalf("unknown build compensated: %v %v", found, err)
			}
			if stage == "ack" && (!record.RegistrationTargetSet || record.State != BuildRegistered) {
				t.Fatalf("accepted identity lost: %+v", record)
			}
			ref, found, err := reg.stores.GetNodeBuildRef(context.Background(), "n1", "b-unknown")
			if err != nil || !found || ref.TemplateID != "transient-unknown" {
				t.Fatalf("owner lost: %+v %v %v", ref, found, err)
			}
			faults.intentUnknown, faults.ackUnknown = false, false
			reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
				t.Fatal("recovery re-placed an immutable Build intent")
				return nil, ErrNoNode
			}))
			recovered, err := reg.ReserveBuild(context.Background(), req)
			if err != nil || recovered == nil || recovered.NodeID != record.NodeID || recovered.BuildID != record.BuildID || recovered.TemplateID != record.TemplateID {
				t.Fatalf("next request did not recover immutable Build: %+v %v", recovered, err)
			}
			if faults.intents != 1 || len(owner.commands) != 1 || faults.acks != 1 {
				t.Fatalf("recovery repeated accepted work: intents=%d commands=%d ACKs=%d", faults.intents, len(owner.commands), faults.acks)
			}
			resolved, found, err := reg.ResolveBuildByTemplate(context.Background(), req.Group, recovered.TemplateID)
			if err != nil || !found || resolved.BuildID != recovered.BuildID || resolved.NodeID != recovered.NodeID {
				t.Fatalf("recovered Build index is not resolvable: %+v %v %v", resolved, found, err)
			}
		})
	}
}

func TestTerminalBuildStoreNeverRetriesUnknown(t *testing.T) {
	calls := 0
	err := retryTerminalBuildStore(context.Background(), func(context.Context) error { calls++; return &metadata.Unknown{Cause: errors.New("reply lost")} })
	if !errors.Is(err, metadata.ErrUnknown) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestObservationCapacityRejectedBeforePlacementAndMutation(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	db.SetObservationLimit(0)
	owner := &recordingNodeOwner{}
	reg.SetNodeOwner(owner)
	placed := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) { placed++; return nil, ErrNoNode }))
	_, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "capacity", nil))
	if !errors.Is(err, metadata.ErrCapacity) {
		t.Fatalf("reserve error=%v", err)
	}
	if placed != 0 || len(owner.commands) != 0 {
		t.Fatalf("capacity side effect: placed=%d commands=%d", placed, len(owner.commands))
	}
	if _, _, found, _ := reg.stores.GetSandbox(context.Background(), "/g", "capacity"); found {
		t.Fatal("capacity created intent")
	}
}

type observedWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *observedWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestSingleFlightJoinersShareObservationReservation(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	db.SetObservationLimit(1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	command := make(chan *routesync.Command, 1)
	var count atomic.Int32
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Kind == routesync.CmdCreate {
			count.Add(1)
			command <- cmd
			reg.ackCommand(&routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted})
		}
	}})
	result := make(chan error, 2)
	go func() { _, err := reg.ReserveSandbox(ctx, testCreateReserve("/g", "shared", nil)); result <- err }()
	var cmd *routesync.Command
	select {
	case cmd = <-command:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	waiterCtx := &observedWaitContext{Context: ctx, waiting: make(chan struct{})}
	go func() {
		// The reserved-row path reaches Done only after joining waitCall. This
		// makes the shared-capacity assertion independent of goroutine timing.
		_, err := reg.reserveCreate(waiterCtx, "/g", "shared", nil, false, false)
		result <- err
	}()
	select {
	case <-waiterCtx.waiting:
	case err := <-result:
		t.Fatalf("joiner did not wait on leader: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if len(reg.reserveLeaders) != 1 {
		t.Fatalf("leader reservations=%d", len(reg.reserveLeaders))
	}
	if used := db.ObservationReservations(); used != 1 {
		t.Fatalf("leader reservations=%d", used)
	}
	route := testE2BRoute(cmd.Cluster.StableID, routesync.StateRunning)
	reg.applyRoute(testNodeSession(ctx, reg.stores, "n1"), "n1", &route)
	for i := 0; i < 2; i++ {
		select {
		case err := <-result:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if len(reg.reserveLeaders) != 0 {
		t.Fatalf("leader reservation leaked: %d", len(reg.reserveLeaders))
	}
	if count.Load() != 1 || db.ObservationReservations() != 0 {
		t.Fatalf("dispatch=%d reservations=%d", count.Load(), db.ObservationReservations())
	}
}

func TestNewRegistrationRejectsOriginalStreamHeartbeat(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	old, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "old"})
	if err != nil {
		t.Fatal(err)
	}
	oldCtx := withNodeRegistration(ctx, old.Registration)
	fresh, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Registration.Fence == old.Registration.Fence {
		t.Fatal("same-owner reconnect reused registration fence")
	}
	reg.updateHeartbeat(oldCtx, "n1", &routesync.Heartbeat{Allocated: 99})
	reg.updateNodeResume(oldCtx, "n1", "stale-token")
	n, _, err := reg.stores.GetNodeProfile(ctx, "n1")
	if err != nil || n.Allocated != 0 || n.ResumeToken != "" || n.APIEndpoint != "new" {
		t.Fatalf("stale stream changed successor: %+v err=%v", n, err)
	}
	reg.updateHeartbeat(withNodeRegistration(ctx, fresh.Registration), "n1", &routesync.Heartbeat{Allocated: 7})
	n, _, _ = reg.stores.GetNodeProfile(ctx, "n1")
	if n.Allocated != 7 {
		t.Fatal("new stream heartbeat rejected")
	}
}

func TestCrossRegistryRegistrationFencesEveryOldStreamProjection(t *testing.T) {
	ctx := context.Background()
	old, db := registryWithTypedFixture(t)
	oldOwner := &recordingNodeOwner{}
	old.SetNodeOwner(oldOwner)
	registered, err := old.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "old-api"})
	if err != nil {
		t.Fatal(err)
	}
	oldCtx := withNodeRegistration(ctx, registered.Registration)
	route := testSandboxIdentityRecord("/g", "stream-fence", "sb-stream-fence", "n1", StateReserved)
	revision, ok, err := old.stores.commitSandboxIntent(ctx, route, metadata.Version{})
	if err != nil || !ok {
		t.Fatalf("seed route: %v %v", ok, err)
	}
	routeRef := metadata.SandboxRef(*route)
	buildKey := metadata.BuildKey{Group: "/g", BuildID: "build-stream-fence"}
	build := metadata.BuildRecord{Group: buildKey.Group, BuildID: buildKey.BuildID, NodeID: "n1", TemplateID: "transient-stream-fence", Profile: types.ProfileE2B, State: metadata.BuildStarting}
	buildRef := metadata.NodeBuildRef{Group: build.Group, BuildID: build.BuildID, TemplateID: build.TemplateID}
	if _, err := old.stores.builds.CommitRegistrationIntent(ctx, build, buildRef); err != nil {
		t.Fatal(err)
	}
	if _, err := old.stores.builds.AcceptBuildRegistration(ctx, buildKey, "n1", build.TemplateID, nil); err != nil {
		t.Fatal(err)
	}
	stores, err := NewStores(db.Repositories(), metadatastore.Owners{ID: "registry-new"})
	if err != nil {
		t.Fatal(err)
	}
	fresh := New(stores, nil, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	fresh.SetNodeOwner(&recordingNodeOwner{})
	reconnected, err := fresh.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "new-api", DataEndpoint: "new-data"})
	if err != nil {
		t.Fatal(err)
	}
	freshCtx := withNodeRegistration(ctx, reconnected.Registration)
	if registered.Registration == reconnected.Registration {
		t.Fatal("reconnect did not change stream authority")
	}
	// Re-registering the exact Build/Template identity refreshes its ownership
	// stamp. An old full report must not treat that refreshed ref as its own.
	if err := fresh.stores.builds.EnsureRegistrationOwnership(ctx, buildKey, "n1", build.TemplateID); err != nil {
		t.Fatal(err)
	}
	baselineBuild, err := fresh.stores.builds.ReadBuild(ctx, buildKey)
	if err != nil {
		t.Fatal(err)
	}
	baselineOwner, err := fresh.stores.ownership.ReadBuildOwner(ctx, "n1", build.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	assertPreserved := func(t *testing.T) {
		t.Helper()
		got, v, found, err := stores.GetSandbox(ctx, route.Group, route.RouteKey)
		if err != nil || !found || v != revision || got.State != StateReserved {
			t.Fatalf("old stream changed route: %+v %v %v %v", got, v, found, err)
		}
		owner, found, err := stores.GetNodeSandboxRef(ctx, "n1", route.NodeSandboxID)
		if err != nil || !found || owner != routeRef {
			t.Fatalf("old stream changed route owner: %+v %v %v", owner, found, err)
		}
		gotBuild, err := stores.builds.ReadBuild(ctx, buildKey)
		if err != nil || !gotBuild.Exists || gotBuild.Version != baselineBuild.Version {
			t.Fatalf("old stream changed Build: %+v %v", gotBuild, err)
		}
		gotOwner, err := stores.ownership.ReadBuildOwner(ctx, "n1", build.BuildID)
		if err != nil || gotOwner != baselineOwner {
			t.Fatalf("old stream changed refreshed Build owner: %+v %v", gotOwner, err)
		}
		if len(oldOwner.deleted) != 0 {
			t.Fatalf("old stream dispatched orphan deletion: %v", oldOwner.deleted)
		}
	}
	for _, tc := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"ready", func(t *testing.T) {
			call := &reserveCall{done: make(chan struct{}), group: route.Group, routeKey: route.RouteKey, expectedSandboxID: route.SandboxID}
			old.inflight[flightKey(route.Group, route.RouteKey)] = call
			event := testE2BRoute(route.SandboxID, routesync.StateRunning)
			old.applyRoute(oldCtx, "n1", &event)
			select {
			case <-call.done:
				t.Fatal("stale READY completed waiter")
			default:
			}
		}},
		{"profile_mismatch", func(t *testing.T) {
			event := testE2BRoute(route.SandboxID, routesync.StateRunning)
			event.Profile = string(types.ProfileBare)
			old.applyRoute(oldCtx, "n1", &event)
		}},
		{"route_delete", func(t *testing.T) {
			if old.applyDelete(oldCtx, "n1", route.NodeSandboxID, route.Group, route.RouteKey, route.APISecretFingerprint) {
				t.Fatal("old route delete succeeded")
			}
		}},
		{"route_delete_by_sid", func(t *testing.T) { old.applyDeleteBySID(oldCtx, "n1", route.NodeSandboxID) }},
		{"route_full_snapshot", func(t *testing.T) { old.applyNodeFullSnapshot(oldCtx, "n1", []metadata.NodeSandboxRef{routeRef}, nil) }},
		{"failed_create_rollback", func(t *testing.T) {
			old.inflight[flightKey(route.Group, route.RouteKey)] = &reserveCall{done: make(chan struct{}), group: route.Group, routeKey: route.RouteKey, rollbackFence: reserveRollbackFence{SandboxID: route.SandboxID, NodeSandboxID: route.NodeSandboxID, SandboxGeneration: route.SandboxGeneration, Revision: revision}}
			if old.applyDelete(oldCtx, "n1", route.NodeSandboxID, route.Group, route.RouteKey, route.APISecretFingerprint) {
				t.Fatal("old failed-create event rolled back successor")
			}
		}},
		{"build_upsert", func(t *testing.T) {
			if err := old.applyBuildUpsert(oldCtx, "n1", &routesync.BuildEvent{BuildID: build.BuildID, TemplateID: build.TemplateID, State: string(BuildBuilding)}); !errors.Is(err, metadata.ErrConflict) {
				t.Fatalf("old Build report: %v", err)
			}
		}},
		{"build_delete", func(t *testing.T) {
			if err := old.applyBuildDelete(oldCtx, "n1", build.BuildID, build.TemplateID); !errors.Is(err, metadata.ErrConflict) {
				t.Fatalf("old Build delete: %v", err)
			}
		}},
		{"build_full_snapshot", func(t *testing.T) {
			if err := old.applyNodeBuildFullSnapshot(oldCtx, "n1", []metadata.NodeBuildRef{buildRef}, nil); !errors.Is(err, metadata.ErrConflict) {
				t.Fatalf("old Build full report: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t); assertPreserved(t) })
	}
	// The new registration remains functional with exactly the same durable
	// identities; rejection above is caused by the original stream fence.
	event := testE2BRoute(route.SandboxID, routesync.StateRunning)
	fresh.applyRoute(freshCtx, "n1", &event)
	if got, _, found, err := stores.GetSandbox(ctx, route.Group, route.RouteKey); err != nil || !found || got.State != StateReady {
		t.Fatalf("new READY rejected: %+v %v %v", got, found, err)
	}
	if err := fresh.applyBuildDelete(freshCtx, "n1", build.BuildID, build.TemplateID); err != nil {
		t.Fatal(err)
	}
	if got, err := stores.builds.ReadBuild(ctx, buildKey); err != nil || got.Exists {
		t.Fatalf("new Build deletion rejected: %+v %v", got, err)
	}
	if !fresh.applyDelete(freshCtx, "n1", route.NodeSandboxID, route.Group, route.RouteKey, route.APISecretFingerprint) {
		t.Fatal("new route deletion rejected")
	}
}

func TestNodeProjectionRequiresCapturedRegistration(t *testing.T) {
	for _, mode := range []string{"missing", "zero", "different_node"} {
		t.Run(mode, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			ctx := context.Background()
			route := testSandboxIdentityRecord("/g", "no-session", "sb-no-session", "n1", StateReserved)
			revision, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{})
			if err != nil || !ok {
				t.Fatalf("seed route: %v %v", ok, err)
			}
			frameCtx := ctx
			if mode == "zero" {
				frameCtx = withNodeRegistration(ctx, metadata.Registration{NodeID: "n1", LinkOwner: "registry"})
			} else if mode == "different_node" {
				profile, _ := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
				registration := profile.Registration
				registration.NodeID = "n2"
				frameCtx = withNodeRegistration(ctx, registration)
			}
			event := testE2BRoute(route.SandboxID, routesync.StateRunning)
			reg.applyRoute(frameCtx, "n1", &event)
			if reg.applyDelete(frameCtx, "n1", route.NodeSandboxID, route.Group, route.RouteKey, route.APISecretFingerprint) {
				t.Fatal("invalid session deleted route")
			}
			if got, v, found, err := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey); err != nil || !found || v != revision || got.State != StateReserved {
				t.Fatalf("invalid session changed route: %+v %v %v %v", got, v, found, err)
			}
			if err := reg.applyBuildDelete(frameCtx, "n1", "build", "transient-build"); !errors.Is(err, metadata.ErrConflict) {
				t.Fatalf("invalid Build session: %v", err)
			}
		})
	}
}

type interceptProfileUpdate struct {
	metadata.NodeRepository
	before func()
	calls  int
}

func (p *interceptProfileUpdate) UpdateProfile(ctx context.Context, registration metadata.Registration, expected metadata.Version, next metadata.NodeProfileRecord) (metadata.Version, error) {
	p.calls++
	if p.calls == 1 {
		p.before()
	}
	return p.NodeRepository.UpdateProfile(ctx, registration, expected, next)
}

func TestRuntimeProfileWriterDoesNotRetryIntoReconnectGeneration(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	old, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	intercept := &interceptProfileUpdate{NodeRepository: reg.stores.nodes, before: func() {
		if _, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "new-generation"}); err != nil {
			t.Fatal(err)
		}
	}}
	reg.stores.nodes = intercept
	_, _, err = reg.updateNodeProfile(withNodeRegistration(ctx, old.Registration), "n1", false, func(node *NodeRecord) { node.Allocated = 99 })
	if !errors.Is(err, metadata.ErrConflict) || intercept.calls != 1 {
		t.Fatalf("stale writer retried: writes=%d err=%v", intercept.calls, err)
	}
	got, found, err := reg.stores.GetNodeProfile(ctx, "n1")
	if err != nil || !found || got.APIEndpoint != "new-generation" || got.Allocated != 0 {
		t.Fatalf("stale writer changed new profile: %+v %v %v", got, found, err)
	}
}

func TestClaimedNodeProfileRejectsStaleRuntimeWriters(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	old, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.stores.nodes.BeginReap(ctx, "n1", old.Version); err != nil {
		t.Fatal(err)
	}
	_, _, err = reg.updateNodeProfile(withNodeRegistration(ctx, old.Registration), "n1", false, func(node *NodeRecord) { node.Allocated = 99 })
	if !errors.Is(err, metadata.ErrConflict) {
		t.Fatalf("reaping accepted stale writer: %v", err)
	}
	got, found, err := reg.stores.GetNodeProfile(ctx, "n1")
	if err != nil || !found || !got.Reaping || got.Allocated != 0 {
		t.Fatalf("stale writer revived reaping profile: %+v %v %v", got, found, err)
	}
}

type failedNodeHello struct{}

func (failedNodeHello) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestFailedNodeHelloSchedulesCleanupAndPreservesReconnect(t *testing.T) {
	for _, reconnect := range []bool{false, true} {
		name := "cleanup"
		if reconnect {
			name = "reconnect"
		}
		t.Run(name, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			reg.reaperCtx, reg.deadAfter = ctx, 20*time.Millisecond
			if err := reg.serveNodeLinkLocal(ctx, failedNodeHello{}, func() {}, strings.NewReader(""), &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "failed-hello"}); err != nil {
				t.Fatal(err)
			}
			if reconnect {
				next, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "reconnected"})
				if err != nil {
					t.Fatal(err)
				}
				reg.addNode(&fakeConn{nodeID: "n1"})
				<-time.After(2 * reg.deadAfter)
				got, found, err := reg.stores.GetNodeProfile(ctx, "n1")
				if err != nil || !found || got.Registration != next.Registration || got.Reaping {
					t.Fatalf("failed Hello cleanup removed reconnect: %+v %v %v", got, found, err)
				}
				return
			}
			for {
				_, found, err := reg.stores.GetNodeProfile(ctx, "n1")
				if err != nil {
					t.Fatal(err)
				}
				if !found {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("failed Hello left a permanent profile/directory entry")
				case <-time.After(time.Millisecond):
				}
			}
			view, err := reg.stores.directory.Load(ctx)
			if err != nil || len(view.Nodes) != 0 {
				t.Fatalf("failed Hello left catalog entry: %+v %v", view, err)
			}
		})
	}
}

func TestReapClaimHidesDirectoryAndReconnectFencesCleanup(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	route := testSandboxIdentityRecord("/g", "reap", "sb-reap", "n1", StateReserved)
	rev, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{})
	if err != nil || !ok {
		t.Fatalf("seed %v %v", ok, err)
	}
	snapshot, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := reg.stores.ownership.ReadSandboxOwner(ctx, "n1", route.NodeSandboxID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := reg.stores.nodes.BeginReap(ctx, "n1", snapshot.Version)
	if err != nil {
		t.Fatal(err)
	}
	view, err := reg.stores.directory.Load(ctx)
	if err != nil || len(view.Nodes) != 0 {
		t.Fatalf("reaping node remained visible: %+v %v", view, err)
	}
	reaping, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
	if err != nil || !reaping.Exists || !reaping.Reaping {
		t.Fatalf("reap marker lost: %+v %v", reaping, err)
	}
	if _, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "reconnected"}); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.nodes.CleanupSandbox(ctx, claim, owner, rev, true); !errors.Is(err, metadata.ErrConflict) {
		t.Fatalf("old claim cleanup=%v", err)
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey); !found {
		t.Fatal("old claim deleted successor's route")
	}
}

func TestReaperResumesDurableClaimAfterRegistryRestart(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	ctx := context.Background()
	route := testSandboxIdentityRecord("/g", "restart-reap", "sb-restart", "n1", StateReserved)
	if _, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{}); err != nil || !ok {
		t.Fatalf("seed %v %v", ok, err)
	}
	profile, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.stores.nodes.BeginReap(ctx, "n1", profile.Version); err != nil {
		t.Fatal(err)
	}
	stores, err := NewStores(db.Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(stores, nil, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reapCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() { restarted.RunReaper(reapCtx, time.Second); close(done) }()
	deadline := time.After(time.Second)
	for {
		_, found, err := stores.GetNodeProfile(ctx, "n1")
		if err != nil {
			t.Fatal(err)
		}
		if !found {
			break
		}
		select {
		case <-deadline:
			t.Fatal("durable reap was not resumed")
		case <-time.After(time.Millisecond):
		}
	}
	if _, _, found, _ := stores.GetSandbox(ctx, route.Group, route.RouteKey); found {
		t.Fatal("route survived completed reap")
	}
	cancel()
	<-done
}

func TestOwnershipCollisionDoesNotLeavePartialIntent(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	ref := testNodeSandboxRef("/other", "other", "collision", "e2b", testAPIFingerprint)
	if err := reg.stores.AddNodeSandboxRef(ctx, "n1", ref); err != nil {
		t.Fatal(err)
	}
	route := testSandboxIdentityRecord("/g", "collision", "collision", "n1", StateReserved)
	_, _, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{})
	if !errors.Is(err, metadata.ErrOwnershipConflict) {
		t.Fatalf("intent err=%v", err)
	}
	if _, _, found, _ := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey); found {
		t.Fatal("failed ownership left a partial route")
	}
}

func TestLeaderCapacityRejectedBeforePlacementAndObservation(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	reg.reserveLeaders = make(chan struct{})
	owner := &recordingNodeOwner{}
	reg.SetNodeOwner(owner)
	placed := 0
	reg.SetPlacer(placementFunc(func(context.Context, PlaceRequest) (*Placement, error) { placed++; return nil, ErrNoNode }))
	_, err := reg.ReserveSandbox(context.Background(), testCreateReserve("/g", "leader-capacity", nil))
	if !errors.Is(err, metadata.ErrCapacity) {
		t.Fatalf("reserve error=%v", err)
	}
	if placed != 0 || len(owner.commands) != 0 || db.ObservationReservations() != 0 {
		t.Fatalf("leader cap side effects: place=%d commands=%d observations=%d", placed, len(owner.commands), db.ObservationReservations())
	}
	if _, _, found, _ := reg.stores.GetSandbox(context.Background(), "/g", "leader-capacity"); found {
		t.Fatal("leader capacity created intent")
	}
}

func TestReadyDataDoesNotConsumeObservationOrLeaderCapacity(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	db.SetObservationLimit(0)
	reg.reserveLeaders = make(chan struct{})
	route := testE2BSandboxRecord("/g", "ready-data", "sb-ready-data", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(context.Background(), route); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})
	result, err := reg.ReserveSandbox(context.Background(), testDataReserve(route, 8080, route.ForwardAccessToken))
	if err != nil || result == nil || result.Route.DataEndpoint != "node-data" {
		t.Fatalf("ready lookup was capacity gated: %+v %v", result, err)
	}
	if db.ObservationReservations() != 0 || len(reg.reserveLeaders) != 0 {
		t.Fatal("ready lookup acquired wait capacity")
	}
}

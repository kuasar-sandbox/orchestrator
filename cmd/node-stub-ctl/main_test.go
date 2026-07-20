package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestFinalBuildDispatchReplayPreservesDurableDecision(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node, err := newStubNode(stubNodeOptions{
		ID: "n1", DataEndpoint: "127.0.0.1:8443", StatePath: filepath.Join(t.TempDir(), "node.db"),
		Capacity: 4, BuildCapacity: &routesync.BuildResources{Slots: 1, CPU: 1000, Mem: 1 << 30},
	}, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer node.close()
	tuple, err := node.NextSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	installStubKeyLease(t, node, tuple, "group-1")
	first := finalBuildCommand(t, tuple, "build-1", "template-1")
	ack := node.HandleCommand(context.Background(), first)
	if ack.Status != routesync.AckAccepted || ack.Outcome != routesync.DispatchAcceptedAdmitted {
		t.Fatalf("first dispatch = %+v", ack)
	}
	replay := *first
	replay.CmdID = "dispatch-retry"
	ack = node.HandleCommand(context.Background(), &replay)
	if ack.Status != routesync.AckAccepted || ack.Outcome != routesync.DispatchAcceptedAdmitted {
		t.Fatalf("retry dispatch = %+v", ack)
	}
	conflict := finalBuildCommand(t, tuple, "build-1", "template-2")
	ack = node.HandleCommand(context.Background(), conflict)
	if ack.Status != routesync.AckAccepted || ack.Outcome != routesync.DispatchConflict {
		t.Fatalf("conflicting dispatch = %+v", ack)
	}
}

func TestFinalBuildTriggerAndLifecycleStayOnBoundNode(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node, err := newStubNode(stubNodeOptions{
		ID: "n1", DataEndpoint: "127.0.0.1:8443", StatePath: filepath.Join(t.TempDir(), "node.db"),
		Capacity: 4, BuildCapacity: &routesync.BuildResources{Slots: 1, CPU: 1000, Mem: 1 << 30},
		BuildDelay: time.Millisecond,
	}, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer node.close()
	svc.addNode(node)
	tuple, err := node.NextSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	installStubKeyLease(t, node, tuple, "group-1")
	command := finalBuildCommand(t, tuple, "build-1", "template-1")
	ack := node.HandleCommand(context.Background(), command)
	if ack.Status != routesync.AckAccepted || ack.Outcome != routesync.DispatchAcceptedAdmitted {
		t.Fatalf("Build registration = %+v", ack)
	}

	trigger := newDirectBuildRequest(t, command, http.MethodPost,
		"/v2/templates/template-1/builds/build-1", `{"fromImage":"registry.local/base:latest"}`)
	recorder := httptest.NewRecorder()
	svc.serveControlStub(recorder, trigger)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("trigger status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	if err := node.reconcileWorkflows(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		build, getErr := node.store.GetBuild(context.Background(), command.BuildID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if build != nil && build.Status == types.BuildReady {
			break
		}
		time.Sleep(time.Millisecond)
	}
	build, err := node.store.GetBuild(context.Background(), command.BuildID)
	if err != nil || build == nil || build.Status != types.BuildReady || !strings.HasPrefix(build.PersistID, "e2b-img-") {
		t.Fatalf("node-local Build = %+v, %v", build, err)
	}
	events, _, err := node.store.PendingExecutionEvents(context.Background(), "n1", tuple.NodeEpoch, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || len(events) != 0 {
		t.Fatalf("Build lifecycle outbox = %+v, %v", events, err)
	}

	retry := newDirectBuildRequest(t, command, http.MethodPost,
		"/v2/templates/template-1/builds/build-1", `{"fromImage":"registry.local/base:latest"}`)
	recorder = httptest.NewRecorder()
	svc.serveControlStub(recorder, retry)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("trigger retry status = %d body=%s", recorder.Code, recorder.Body.String())
	}
	conflict := newDirectBuildRequest(t, command, http.MethodPost,
		"/v2/templates/template-1/builds/build-1", `{"fromImage":"registry.local/other:latest"}`)
	recorder = httptest.NewRecorder()
	svc.serveControlStub(recorder, conflict)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("trigger conflict status = %d body=%s", recorder.Code, recorder.Body.String())
	}

	status := newDirectBuildRequest(t, command, http.MethodGet,
		"/templates/template-1/builds/build-1/status", "")
	recorder = httptest.NewRecorder()
	svc.serveControlStub(recorder, status)
	var response map[string]any
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &response) != nil ||
		response["status"] != string(types.BuildReady) || response["templateID"] != build.PersistID {
		t.Fatalf("Build status = %d %s", recorder.Code, recorder.Body.String())
	}
}

func newDirectBuildRequest(t *testing.T, command *routesync.Command, method, path, body string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(method, "https://api.cluster.stub"+path, bytes.NewBufferString(body))
	request.Host = "api.cluster.stub"
	request.Header.Set(clusterstate.DirectHeaderExecutionKind, "build")
	request.Header.Set(clusterstate.DirectHeaderObjectID, command.BuildID)
	request.Header.Set(clusterstate.DirectHeaderGroup, command.Group)
	request.Header.Set(proxypkg.HeaderNodeID, "n1")
	request.Header.Set(proxypkg.HeaderNodeEpoch, "1")
	request.Header.Set(proxypkg.HeaderRegistryGeneration, command.RegistryGeneration)
	request.Header.Set(proxypkg.HeaderBindingDigest, command.BindingDigest)
	return request
}

func TestFinalSandboxDispatchDrivesDurableEventRecoveryAndProxyFence(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node, err := newStubNode(stubNodeOptions{
		ID: "n1", DataEndpoint: "127.0.0.1:8443", StatePath: filepath.Join(t.TempDir(), "node.db"),
		Capacity: 4, BuildCapacity: &routesync.BuildResources{Slots: 1, CPU: 1000, Mem: 1 << 30},
		CreateDelay: time.Millisecond,
	}, svc)
	if err != nil {
		t.Fatal(err)
	}
	defer node.close()
	tuple, err := node.NextSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	installStubKeyLease(t, node, tuple, "group-1")
	command := finalSandboxCommand(t, tuple, "sandbox-1")
	ack := node.HandleCommand(context.Background(), command)
	if ack.Status != routesync.AckAccepted || ack.Outcome != routesync.DispatchAcceptedAdmitted {
		t.Fatalf("dispatch = %+v", ack)
	}
	if err := node.reconcileWorkflows(context.Background()); err != nil {
		t.Fatal(err)
	}
	var sandbox *stubSandbox
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		sandbox = node.getSandbox(command.SID)
		if sandbox != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if sandbox == nil || sandbox.BindingDigest != command.BindingDigest || sandbox.RegistryGeneration != command.RegistryGeneration {
		t.Fatalf("ready sandbox = %+v", sandbox)
	}
	events, _, err := node.store.PendingExecutionEvents(context.Background(), "n1", tuple.NodeEpoch, routesync.EventCursor{}, 10, 1<<20)
	if err != nil || len(events) != 1 || events[0].State != string(clusterstate.WorkflowRouteReady) {
		t.Fatalf("pending events = %+v, %v", events, err)
	}

	request, _ := http.NewRequest(http.MethodConnect, "http://sandbox:8080", nil)
	request.Header.Set(proxypkg.HeaderNodeID, "n1")
	request.Header.Set(proxypkg.HeaderNodeEpoch, "1")
	request.Header.Set(proxypkg.HeaderRegistryGeneration, command.RegistryGeneration)
	request.Header.Set(proxypkg.HeaderBindingDigest, command.BindingDigest)
	request.Header.Set(proxypkg.HeaderAccessToken, sandbox.AccessToken)
	if status, kind, err := node.validateDataFence(request, sandbox); err != nil || status != 0 || kind != "" {
		t.Fatalf("valid proxy fence = %d %q %v", status, kind, err)
	}
	request.Header.Set(proxypkg.HeaderBindingDigest, strings.Repeat("f", 64))
	if status, kind, err := node.validateDataFence(request, sandbox); err == nil || status != http.StatusConflict || kind != proxypkg.ProxyErrorWrongBinding {
		t.Fatalf("stale proxy fence = %d %q %v", status, kind, err)
	}

	recovery := &routesync.Command{
		CmdID: "recovery-1", Kind: routesync.CmdCollectRecovery,
		NodeEpoch: tuple.NodeEpoch, SessionSeq: tuple.SessionSeq,
		Recovery: &routesync.RecoveryReportRequest{
			RecoveryEpoch: 1, SourceClusterID: "cluster-1", SourceRegistryGeneration: command.RegistryGeneration,
			SourceRegistryLayoutDigest: strings.Repeat("a", 64), TargetRegistryGeneration: "generation-2",
			TargetRegistryLayoutDigest: strings.Repeat("b", 64), Limit: 16, MaxBytes: routesync.MaxRecoveryPageBytes,
		},
	}
	ack = node.HandleCommand(context.Background(), recovery)
	if ack.Status != routesync.AckAccepted || ack.Recovery == nil || len(ack.Recovery.Objects) != 1 ||
		ack.Recovery.Objects[0].Object.ObjectID != command.SID {
		t.Fatalf("recovery report = %+v", ack)
	}

	target, err := clusterstate.DecodeExecutionBinding(command.Binding)
	if err != nil {
		t.Fatal(err)
	}
	target.RegistryGeneration = "generation-2"
	targetOpaque, err := clusterstate.EncodeExecutionBinding(target)
	if err != nil {
		t.Fatal(err)
	}
	targetDigest, err := clusterstate.ExecutionBindingDigest(targetOpaque)
	if err != nil {
		t.Fatal(err)
	}
	rebind := &routesync.Command{
		CmdID: "rebind-1", Kind: routesync.CmdRebindExecution, SID: command.SID,
		NodeEpoch: tuple.NodeEpoch, SessionSeq: tuple.SessionSeq,
		RegistryGeneration: target.RegistryGeneration, Binding: targetOpaque, BindingDigest: targetDigest,
		OldBindingDigest: command.BindingDigest,
	}
	ack = node.HandleCommand(context.Background(), rebind)
	if ack.Status != routesync.AckAccepted || ack.RebindObject == nil ||
		ack.RebindObject.RegistryGeneration != target.RegistryGeneration || ack.RebindObject.BindingDigest != targetDigest {
		t.Fatalf("rebind = %+v", ack)
	}
	rebindObject := *ack.RebindObject
	eventAck := &routesync.Command{
		CmdID: "recovery-ack-1", Kind: routesync.CmdAckRecoveryEvent, SID: command.SID,
		NodeEpoch: tuple.NodeEpoch, SessionSeq: tuple.SessionSeq,
		RegistryGeneration: target.RegistryGeneration, BindingDigest: targetDigest,
		EventAck: &routesync.EventAck{
			ObjectKind: "sandbox", ObjectID: command.SID, EventSeq: rebindObject.EventSeq,
		},
	}
	ack = node.HandleCommand(context.Background(), eventAck)
	if ack.Status != routesync.AckAccepted || ack.RebindObject == nil ||
		ack.RebindObject.RegistryGeneration != target.RegistryGeneration || ack.RebindObject.BindingDigest != targetDigest {
		t.Fatalf("recovery event ACK = %+v", ack)
	}
	events, _, err = node.store.PendingExecutionEvents(
		context.Background(), "n1", tuple.NodeEpoch, routesync.EventCursor{}, 10, 1<<20,
	)
	if err != nil || len(events) != 0 {
		t.Fatalf("events after recovery ACK = %+v, %v", events, err)
	}
}

func finalBuildCommand(
	t *testing.T,
	tuple routesync.SessionTuple,
	buildID, templateID string,
) *routesync.Command {
	t.Helper()
	demand, err := placement.NormalizeBuildDemand(placement.BuildDemand{Slots: 1, CPU: 100, Memory: 64 << 20})
	if err != nil {
		t.Fatal(err)
	}
	lease := stubTestKeyLease("group-1")
	request, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/v3/templates", "", nil,
		[]byte(`{"name":"","tags":null,"profile":"bare","cpuCount":1,"memoryMB":64,"metadata":null}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateID: templateID,
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
		Profile: types.ProfileBare, CPUCount: 1, MemoryMB: 64, Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	demandDigest, _ := hex.DecodeString(intent.DemandDigest)
	specDigest, _ := hex.DecodeString(intent.DispatchSpecDigest)
	binding := clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-1", Kind: clusterstate.ExecutionKindBuild,
		ObjectID: buildID, Group: "group-1", NodeID: "n1", NodeEpoch: tuple.NodeEpoch,
	}
	copy(binding.DemandDigest[:], demandDigest)
	copy(binding.DispatchSpecDigest[:], specDigest)
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return &routesync.Command{
		CmdID: "dispatch-1", Kind: routesync.CmdBuildAdmitDispatch, BuildID: buildID,
		NodeEpoch: tuple.NodeEpoch, SessionSeq: tuple.SessionSeq, RegistryGeneration: "generation-1",
		Binding: opaque, BindingDigest: bindingDigest, Group: "group-1",
		NormalizedDemand: demand, DemandDigest: intent.DemandDigest,
		DispatchSpec: spec, DispatchSpecDigest: intent.DispatchSpecDigest, ProviderPolicy: intent.ProviderPolicyVersion,
	}
}

func finalSandboxCommand(t *testing.T, tuple routesync.SessionTuple, sandboxID string) *routesync.Command {
	t.Helper()
	demand, err := placement.NormalizeSandboxDemand(placement.SandboxDemand{
		SlotUnits: 1, StartupBudgetMemory: 64 << 20, FloorMemory: 32 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	lease := stubTestKeyLease("group-1")
	templateRef := "e2b-img-" + strings.Repeat("1", 64)
	request, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/sandboxes", "", nil,
		[]byte(`{"templateID":"`+templateRef+`","timeout":0,"metadata":{"stub.http_status":"204"}}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
		AccessToken: "access-1", TargetPort: 8080,
		Config: map[string]string{"stub.http_status": "204"}, Request: request,
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	demandDigest, _ := hex.DecodeString(intent.DemandDigest)
	specDigest, _ := hex.DecodeString(intent.DispatchSpecDigest)
	binding := clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-1", Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: sandboxID, Group: "group-1", RouteKey: "route-1", NodeID: "n1", NodeEpoch: tuple.NodeEpoch,
	}
	copy(binding.DemandDigest[:], demandDigest)
	copy(binding.DispatchSpecDigest[:], specDigest)
	opaque, err := clusterstate.EncodeExecutionBinding(binding)
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return &routesync.Command{
		CmdID: "dispatch-sandbox-1", Kind: routesync.CmdSandboxAdmitDispatch, SID: sandboxID,
		NodeEpoch: tuple.NodeEpoch, SessionSeq: tuple.SessionSeq, RegistryGeneration: binding.RegistryGeneration,
		Binding: opaque, BindingDigest: bindingDigest, Group: binding.Group, RouteKey: binding.RouteKey,
		NormalizedDemand: demand, DemandDigest: intent.DemandDigest,
		DispatchSpec: spec, DispatchSpecDigest: intent.DispatchSpecDigest, ProviderPolicy: intent.ProviderPolicyVersion,
	}
}

func installStubKeyLease(
	t *testing.T,
	node *stubNode,
	tuple routesync.SessionTuple,
	group string,
) {
	t.Helper()
	lease := stubTestKeyLease(group)
	ack := node.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "key-put-" + group, Kind: routesync.CmdKeyPut,
		NodeEpoch: tuple.NodeEpoch, SessionSeq: tuple.SessionSeq,
		RegistryGeneration: "generation-1",
		AuthKeyFingerprint: lease.AuthKey.Fingerprint, ManifestKeyFingerprint: lease.ManifestKey.Fingerprint,
		KeyLease: &lease,
	})
	if ack.Status != routesync.AckAccepted || ack.KeyLeaseRef == nil {
		t.Fatalf("key_put = %+v", ack)
	}
}

func stubTestKeyLease(group string) routesync.NodeKeyLeaseV1 {
	material := func(value string) routesync.NodeKeyMaterialV1 {
		raw, _ := hex.DecodeString(value)
		digest := sha256.Sum256(raw)
		return routesync.NodeKeyMaterialV1{
			Type: routesync.KeyMaterialInline, Value: value, Fingerprint: hex.EncodeToString(digest[:12]),
		}
	}
	return routesync.NodeKeyLeaseV1{
		Version: routesync.NodeKeyLeaseVersionV1, Group: group,
		AuthKey: material(strings.Repeat("a", 64)), ManifestKey: material(strings.Repeat("b", 64)),
		ExpiresUnix: time.Now().Add(time.Hour).Unix(),
	}
}

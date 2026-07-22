package raftstore

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestDataShardBootstrapRequiresExactRegistryLayoutIdentity(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	identity := routeShardIdentity(t, registryLayout, "/g", "rk")
	replicas := replicaIDsForPlacement(registryLayout.DataShards[identity.ShardID])
	bootstrap, _ := NewDataShardBootstrap(registryLayout, identity.ShardID)
	state := DataState{}

	wrong := identity
	wrong.RegistryLayoutDigest = digestFor("another-registryLayout")
	result := ApplyDataCommand(&state, 1, DataCommand{
		Type: DataInitializeShard, Identity: wrong, Bootstrap: &bootstrap, ReplicaIDs: replicas,
	})
	if !result.Conflict || state.Initialized || state.LastApplied != 0 {
		t.Fatalf("wrong registryLayout bootstrap = %+v, state=%+v", result, state)
	}

	result = ApplyDataCommand(&state, 2, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap, ReplicaIDs: replicas,
	})
	if !result.Applied || state.LastApplied != 2 || !state.Accepts(identity) {
		t.Fatalf("exact bootstrap = %+v, state=%+v", result, state)
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}

	corrupt := state
	corrupt.Routes = map[string]clusterstate.RouteWorkflowRecord{"wrong-key": routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, false)}
	corrupt.Routes["wrong-key"] = withRevision(corrupt.Routes["wrong-key"], corrupt, 2)
	if err := corrupt.Validate(); err == nil {
		t.Fatal("snapshot validation accepted a Route under the wrong map key")
	}
}

func TestDataCASConflictAdvancesAppliedIndexWithoutChangingRow(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, false)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})

	starting.Starting.CandidatePool[0].NodeID = "caller-mutated"
	stored := state.Routes[routeMapKey("/g", "rk")]
	if stored.Starting.CandidatePool[0].NodeID == "caller-mutated" {
		t.Fatal("stored Route aliases command-owned slices")
	}

	update := cloneRouteRecord(stored)
	result := ApplyDataCommand(&state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 99}, Route: &update,
	})
	stored = state.Routes[routeMapKey("/g", "rk")]
	if !result.Conflict || result.CurrentRevision != 2 || state.LastApplied != 3 || stored.Revision.LogIndex != 2 {
		t.Fatalf("stale CAS = %+v, applied=%d, row=%+v", result, state.LastApplied, stored.Revision)
	}

	selected := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &selected,
	})
	if got := state.Routes[routeMapKey("/g", "rk")].Revision.LogIndex; got != 4 {
		t.Fatalf("Route revision = %d, want committed log index 4", got)
	}
}

func TestCloneRouteRecordDeepCopiesReadyExecutionIntents(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-clone-ready")
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	ready := readyRecord(starting, 1)
	paused := pausedRecord(ready, 2)
	resuming := clusterstate.RouteWorkflowRecord{
		Group: ready.Group, RouteKey: ready.RouteKey, State: clusterstate.WorkflowRouteResuming,
		Resuming: &clusterstate.ResumingRouteState{
			Execution: paused.Paused.Execution,
			Intent:    paused.Paused.ResumeIntent,
		},
	}
	deleting := deletingRecord(ready)

	tests := []struct {
		name   string
		record clusterstate.RouteWorkflowRecord
		intent func(*clusterstate.RouteWorkflowRecord) *clusterstate.DispatchIntent
	}{
		{name: "ready", record: ready, intent: func(record *clusterstate.RouteWorkflowRecord) *clusterstate.DispatchIntent {
			return &record.Ready.Intent
		}},
		{name: "paused", record: paused, intent: func(record *clusterstate.RouteWorkflowRecord) *clusterstate.DispatchIntent {
			return &record.Paused.Execution.Intent
		}},
		{name: "resuming", record: resuming, intent: func(record *clusterstate.RouteWorkflowRecord) *clusterstate.DispatchIntent {
			return &record.Resuming.Execution.Intent
		}},
		{name: "deleting", record: deleting, intent: func(record *clusterstate.RouteWorkflowRecord) *clusterstate.DispatchIntent {
			return &record.Deleting.Execution.Intent
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cloned := cloneRouteRecord(test.record)
			clonedIntent := test.intent(&cloned)
			wantDemand := append([]byte(nil), clonedIntent.NormalizedDemand...)
			wantSpec := append([]byte(nil), clonedIntent.DispatchSpec...)
			sourceIntent := test.intent(&test.record)
			sourceIntent.NormalizedDemand[0] ^= 0xff
			sourceIntent.DispatchSpec[0] ^= 0xff
			if !bytes.Equal(clonedIntent.NormalizedDemand, wantDemand) || !bytes.Equal(clonedIntent.DispatchSpec, wantSpec) {
				t.Fatal("cloned READY execution retained command-owned dispatch slices")
			}
		})
	}
}

func TestCloneBuildRecordDeepCopiesProjectionIntent(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-clone-build")
	starting := buildStarting(t, registryLayout, "/g", "build-1", true)
	registered := buildRegistrationRecord(starting)
	cloned := cloneBuildRecord(registered)
	wantDemand := append([]byte(nil), cloned.Projection.Intent.NormalizedDemand...)
	wantSpec := append([]byte(nil), cloned.Projection.Intent.DispatchSpec...)
	registered.Projection.Intent.NormalizedDemand[0] ^= 0xff
	registered.Projection.Intent.DispatchSpec[0] ^= 0xff
	if !bytes.Equal(cloned.Projection.Intent.NormalizedDemand, wantDemand) ||
		!bytes.Equal(cloned.Projection.Intent.DispatchSpec, wantSpec) {
		t.Fatal("cloned Build projection retained command-owned dispatch slices")
	}
}

func TestMutationLookupInheritsFinalizationsAndIgnoresFenceCompaction(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-mutation-lookup")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	selected := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &selected,
	})
	rejected := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	binding := *rejected.Starting.Binding
	rejected.Starting.SelectedCandidate = nil
	rejected.Starting.Binding = nil
	rejected.Starting.DefinitivelyRejected = []uint32{0}
	rejected.Finalizations = []clusterstate.WorkflowFinalizationIntent{
		workflowFinalization(t, rejected.Starting.SandboxID, binding, nil),
	}
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &rejected,
	})

	wanted := cloneRouteRecord(rejected)
	wanted.Finalizations = nil
	wanted.Revision = clusterstate.Revision{}
	status, err := LookupDataMutation(state, DataMutationLookup{Command: DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &wanted,
	}})
	if err != nil || !status.Committed || status.Revision != 3 {
		t.Fatalf("inherited-finalization mutation lookup = %+v, %v", status, err)
	}
	buildState, buildIdentity := initializedBuildShard(t, registryLayout, "/g", "build-1")
	selectedBuild := buildStarting(t, registryLayout, "/g", "build-1", true)
	applyDataOK(t, &buildState, 2, DataCommand{
		Type: DataPutBuild, Identity: buildIdentity, Expect: RevisionExpectation{Absent: true}, Build: &selectedBuild,
	})
	rejectedBuild := cloneBuildRecord(buildState.Builds[buildMapKey("/g", "build-1")])
	buildBinding := *rejectedBuild.Starting.Binding
	rejectedBuild.Starting.SelectedCandidate = nil
	rejectedBuild.Starting.Binding = nil
	rejectedBuild.Starting.DefinitivelyRejected = []uint32{0}
	rejectedBuild.Finalizations = []clusterstate.WorkflowFinalizationIntent{
		workflowFinalization(t, rejectedBuild.BuildID, buildBinding, nil),
	}
	applyDataOK(t, &buildState, 3, DataCommand{
		Type: DataPutBuild, Identity: buildIdentity, Expect: RevisionExpectation{LogIndex: 2}, Build: &rejectedBuild,
	})
	wantedBuild := cloneBuildRecord(rejectedBuild)
	wantedBuild.Finalizations = nil
	wantedBuild.Revision = clusterstate.Revision{}
	status, err = LookupDataMutation(buildState, DataMutationLookup{Command: DataCommand{
		Type: DataPutBuild, Identity: buildIdentity, Expect: RevisionExpectation{LogIndex: 2}, Build: &wantedBuild,
	}})
	if err != nil || !status.Committed || status.Revision != 3 {
		t.Fatalf("inherited Build finalization mutation lookup = %+v, %v", status, err)
	}

	compacted := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	compacted.State = clusterstate.WorkflowRouteTombstone
	compacted.Starting = nil
	compacted.Finalizations = nil
	compacted.Tombstone = &clusterstate.RouteTombstoneState{
		PlacementFailure: &clusterstate.RoutePlacementFailureState{
			SandboxID: "sandbox-1", PlacementRound: 1, CandidatePool: selected.Starting.CandidatePool,
			DefinitivelyRejected: []uint32{0, 1}, Intent: selected.Starting.Intent, Reason: "exhausted",
		},
		FenceCompacted: true,
	}
	compacted.Revision = revisionFor(state, 5)
	state.Routes[routeMapKey("/g", "rk")] = compacted
	wanted = cloneRouteRecord(compacted)
	wanted.Revision = clusterstate.Revision{}
	wanted.Tombstone.FenceCompacted = false
	status, err = LookupDataMutation(state, DataMutationLookup{Command: DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &wanted,
	}})
	if err != nil || !status.Committed || status.Revision != 5 {
		t.Fatalf("compacted-tombstone mutation lookup = %+v, %v", status, err)
	}
}

func TestStartingCandidateRejectionAndSandboxRoundAdvance(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	selected := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &selected,
	})

	rejectedFirst := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	firstBinding := *rejectedFirst.Starting.Binding
	rejectedFirst.Starting.SelectedCandidate = nil
	rejectedFirst.Starting.Binding = nil
	rejectedFirst.Starting.DefinitivelyRejected = []uint32{0}
	rejectedFirst.Finalizations = []clusterstate.WorkflowFinalizationIntent{
		workflowFinalization(t, rejectedFirst.Starting.SandboxID, firstBinding, nil),
	}
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &rejectedFirst,
	})

	selectedSecond := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	second := uint32(1)
	binding := testBinding(t, registryLayout, clusterstate.ExecutionKindSandbox, "sandbox-1", "/g", "rk", "node-2", selectedSecond.Starting.Intent)
	selectedSecond.Starting.SelectedCandidate = &second
	selectedSecond.Starting.Binding = &binding
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &selectedSecond,
	})

	exhausted := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	secondBinding := *exhausted.Starting.Binding
	exhausted.Starting.SelectedCandidate = nil
	exhausted.Starting.Binding = nil
	exhausted.Starting.DefinitivelyRejected = []uint32{0, 1}
	exhausted.Finalizations = append(exhausted.Finalizations,
		workflowFinalization(t, exhausted.Starting.SandboxID, secondBinding, nil))
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &exhausted,
	})

	failure := clusterstate.RoutePlacementFailureState{
		SandboxID: "sandbox-1", PlacementRound: 1,
		CandidatePool:        append([]clusterstate.PlacementCandidate(nil), exhausted.Starting.CandidatePool...),
		DefinitivelyRejected: append([]uint32(nil), exhausted.Starting.DefinitivelyRejected...),
		Intent:               exhausted.Starting.Intent, Reason: "placement candidate pool exhausted",
	}
	tombstone := clusterstate.RouteWorkflowRecord{
		Group: "/g", RouteKey: "rk", State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{PlacementFailure: &failure},
	}
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 5}, Route: &tombstone,
	})
	fence, err := clusterstate.NewPlacementFailureFence("/g", "rk", registryLayout.RegistryGeneration, failure)
	if err != nil {
		t.Fatal(err)
	}
	applyDataOK(t, &state, 7, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	nextRound := routeStarting(t, registryLayout, "/g", "rk", "sandbox-2", 2, false)
	applyDataOK(t, &state, 8, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 6}, Route: &nextRound,
	})

	skipped := cloneRouteRecord(state.Routes[routeMapKey("/g", "rk")])
	skipped.Starting.DefinitivelyRejected = []uint32{0}
	result := ApplyDataCommand(&state, 9, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 8}, Route: &skipped,
	})
	if !result.Conflict {
		t.Fatal("unselected candidate was marked rejected without a dispatch result")
	}
}

func TestCompactedPlacementFenceStillAuthorizesNextRound(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk-compacted-placement")
	starting := routeStarting(t, registryLayout, "/g", "rk-compacted-placement", "sandbox-1", 1, false)
	starting.Starting.DefinitivelyRejected = []uint32{0, 1}
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	failure := clusterstate.RoutePlacementFailureState{
		SandboxID: starting.Starting.SandboxID, PlacementRound: starting.Starting.PlacementRound,
		CandidatePool:        append([]clusterstate.PlacementCandidate(nil), starting.Starting.CandidatePool...),
		DefinitivelyRejected: append([]uint32(nil), starting.Starting.DefinitivelyRejected...),
		Intent:               starting.Starting.Intent, Reason: "placement candidate pool exhausted",
	}
	tombstone := clusterstate.RouteWorkflowRecord{
		Group: starting.Group, RouteKey: starting.RouteKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{PlacementFailure: &failure, FenceCompacted: true},
	}
	if result := ApplyDataCommand(&state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &tombstone,
	}); !result.Conflict {
		t.Fatal("caller precompacted a placement-failure tombstone")
	}
	tombstone.Tombstone.FenceCompacted = false
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &tombstone,
	})
	fence, err := clusterstate.NewPlacementFailureFence(
		starting.Group, starting.RouteKey, registryLayout.RegistryGeneration, failure,
	)
	if err != nil {
		t.Fatal(err)
	}
	applyDataOK(t, &state, 5, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	storedFence := state.Fences[fenceMapKey(starting.Group, starting.RouteKey, failure.SandboxID)]
	authorization := fenceCompaction(storedFence, state.ReplicaIDs)
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataCompactFence, Identity: identity, Compaction: &authorization,
	})
	compacted := state.Routes[routeMapKey(starting.Group, starting.RouteKey)]
	if compacted.Tombstone == nil || !compacted.Tombstone.FenceCompacted || len(state.Fences) != 0 {
		t.Fatalf("compacted placement state = %+v, fences=%+v", compacted, state.Fences)
	}
	if replayed := ApplyDataCommand(&state, 7, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &storedFence,
	}); !replayed.Conflict {
		t.Fatal("compacted placement fence was replayed")
	}
	next := routeStarting(t, registryLayout, starting.Group, starting.RouteKey, "sandbox-2", 2, false)
	applyDataOK(t, &state, 8, DataCommand{
		Type: DataPutRoute, Identity: identity,
		Expect: RevisionExpectation{LogIndex: compacted.Revision.LogIndex}, Route: &next,
	})
}

func TestBuildStartingCommitsDefinitiveCandidateRejection(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedBuildShard(t, registryLayout, "/g", "build-reject")
	selected := buildStarting(t, registryLayout, "/g", "build-reject", true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &selected,
	})
	rejected := cloneBuildRecord(state.Builds[buildMapKey("/g", "build-reject")])
	binding := *rejected.Starting.Binding
	rejected.Starting.SelectedCandidate = nil
	rejected.Starting.Binding = nil
	rejected.Starting.DefinitivelyRejected = []uint32{0}
	rejected.Finalizations = []clusterstate.WorkflowFinalizationIntent{
		workflowFinalization(t, rejected.BuildID, binding, nil),
	}
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &rejected,
	})
}

func TestBoundStartingExecutionCannotBecomePlacementFailureWithoutRejections(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk-bound")
	bound := routeStarting(t, registryLayout, "/g", "rk-bound", "sandbox-bound", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &bound,
	})

	failure := clusterstate.RouteWorkflowRecord{
		Group: bound.Group, RouteKey: bound.RouteKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{PlacementFailure: &clusterstate.RoutePlacementFailureState{
			SandboxID: bound.Starting.SandboxID, PlacementRound: bound.Starting.PlacementRound,
			CandidatePool: bound.Starting.CandidatePool, DefinitivelyRejected: []uint32{0, 1},
			Intent: bound.Starting.Intent, Reason: "placement exhausted",
		}},
	}
	result := ApplyDataCommand(&state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &failure,
	})
	if !result.Conflict {
		t.Fatal("bound execution was converted to a placement-failure tombstone")
	}
}

func TestRegistryResumeUsesCommittedPausedIntent(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk-resume")
	starting := routeStarting(t, registryLayout, "/g", "rk-resume", "sandbox-resume", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	paused := pausedRecord(ready, 2)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &paused,
	})

	resuming := clusterstate.RouteWorkflowRecord{
		Group: paused.Group, RouteKey: paused.RouteKey, State: clusterstate.WorkflowRouteResuming,
		Resuming: &clusterstate.ResumingRouteState{
			Execution: paused.Paused.Execution,
			Intent:    paused.Paused.ResumeIntent,
		},
	}
	resuming.Resuming.Intent.ProviderPolicyVersion = "substituted-policy"
	result := ApplyDataCommand(&state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &resuming,
	})
	if !result.Conflict {
		t.Fatal("PAUSED resume intent was replaced during RESUMING transition")
	}
	resuming.Resuming.Intent = paused.Paused.ResumeIntent
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &resuming,
	})
}

func TestCallerCannotPrecompactExecutionTombstone(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk-precompact")
	starting := routeStarting(t, registryLayout, "/g", "rk-precompact", "sandbox-precompact", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	deleting := deletingRecord(ready)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &deleting,
	})
	tombstone, _ := terminalRouteAndFence(deleting, 2)
	tombstone.Tombstone.FenceCompacted = true
	result := ApplyDataCommand(&state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &tombstone,
	})
	if !result.Conflict {
		t.Fatal("caller-created execution tombstone bypassed fence compaction")
	}
}

func TestRouteAutoResumeReplacementAndFenceCompaction(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})

	ready := readyRecord(starting, 1)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	paused := pausedRecord(ready, 2)
	applyDataOK(t, &state, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &paused,
	})

	wrongResume := readyRecord(starting, 3)
	wrongResume.Ready.NodeEpoch++
	result := ApplyDataCommand(&state, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &wrongResume,
	})
	if !result.Conflict || state.LastApplied != 5 || state.Routes[routeMapKey("/g", "rk")].State != clusterstate.WorkflowRoutePaused {
		t.Fatalf("wrong auto-resume = %+v", result)
	}

	resumed := readyRecord(starting, 3)
	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &resumed,
	})
	deleting := deletingRecord(resumed)
	applyDataOK(t, &state, 7, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 6}, Route: &deleting,
	})
	tombstone, fence := terminalRouteAndFence(deleting, 4)
	applyDataOK(t, &state, 8, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 7}, Route: &tombstone,
	})

	replacement := routeStarting(t, registryLayout, "/g", "rk", "sandbox-2", 2, false)
	result = ApplyDataCommand(&state, 9, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 8}, Route: &replacement,
	})
	if !result.Conflict {
		t.Fatal("replacement was committed without an old-execution fence")
	}

	applyDataOK(t, &state, 10, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	applyDataOK(t, &state, 11, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 8}, Route: &replacement,
	})

	fenceKey := fenceMapKey("/g", "rk", "sandbox-1")
	storedFence := state.Fences[fenceKey]
	incomplete := fenceCompaction(storedFence, state.ReplicaIDs[:2])
	result = ApplyDataCommand(&state, 12, DataCommand{
		Type: DataCompactFence, Identity: identity, Compaction: &incomplete,
	})
	if !result.Conflict || state.LastApplied != 12 {
		t.Fatalf("incomplete fence compaction = %+v", result)
	}
	if _, found := state.Fences[fenceKey]; !found {
		t.Fatal("incomplete proof compacted the execution fence")
	}

	complete := fenceCompaction(storedFence, state.ReplicaIDs)
	complete.FinalOutboxWatermarkAcked = false
	complete.NodeEpochFence = &NodeEpochFenceEvidence{
		PermitIdentity: identity.PermitIdentity, SystemCommitIndex: 20,
		EnrollmentID: "enrollment-1", EnrollmentCommitIndex: 18,
		NodeID: storedFence.NodeID, FencedNodeEpoch: storedFence.NodeEpoch,
		ObservedNodeEpoch: storedFence.NodeEpoch + 1, ObservedDataEndpoint: "10.0.0.1:8443",
	}
	complete.NodeEpochFence.ProofDigest, _ = nodeEpochFenceEvidenceDigest(*complete.NodeEpochFence)
	forged := complete
	forgedEvidence := *complete.NodeEpochFence
	forgedEvidence.ObservedNodeEpoch = storedFence.NodeEpoch
	forged.NodeEpochFence = &forgedEvidence
	result = ApplyDataCommand(&state, 13, DataCommand{Type: DataCompactFence, Identity: identity, Compaction: &forged})
	if !result.Conflict {
		t.Fatalf("forged NodeEpoch compaction = %+v", result)
	}
	applyDataOK(t, &state, 14, DataCommand{Type: DataCompactFence, Identity: identity, Compaction: &complete})
	if _, found := state.Fences[fenceKey]; found {
		t.Fatal("complete proof did not compact the execution fence")
	}
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBuildRegistrationBindingIsImmutable(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedBuildShard(t, registryLayout, "/g", "build-1")
	starting := buildStarting(t, registryLayout, "/g", "build-1", true)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{Absent: true}, Build: &starting,
	})
	registered := buildRegistrationRecord(starting)
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Build: &registered,
	})

	moved := registered
	moved.Projection = cloneBuildProjection(registered.Projection)
	moved.Projection.NodeID = "node-2"
	result := ApplyDataCommand(&state, 4, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Build: &moved,
	})
	if !result.Conflict {
		t.Fatal("committed Build registration moved to another node")
	}

	changedTemplate := registered
	changedTemplate.Projection = cloneBuildProjection(registered.Projection)
	changedTemplate.Projection.TemplateRef = "another-template"
	result = ApplyDataCommand(&state, 5, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Build: &changedTemplate,
	})
	if !result.Conflict {
		t.Fatal("committed Build registration changed immutable template reference")
	}

	applyDataOK(t, &state, 6, DataCommand{
		Type: DataPutBuild, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Build: &registered,
	})
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDataServingEpochTransitionPersistsBothReplicaSets(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	next := identity.PermitIdentity
	next.SystemEpoch++
	next.RegistryLayoutDigest = digestFor("registryLayout-next")
	nextReplicas := []uint64{2, 3, 4}
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPrepareEpoch, Identity: identity, Epoch: &next, ReplicaIDs: nextReplicas,
	})
	nextIdentity := ShardRequestIdentity{PermitIdentity: next, ShardID: identity.ShardID}
	if !state.Accepts(identity) || !state.Accepts(nextIdentity) {
		t.Fatal("prepared transition did not accept both committed serving epochs")
	}
	if got := compactionReplicaIDs(state); len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("transition compaction replicas = %+v", got)
	}
	old := identity.PermitIdentity
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataRetireEpoch, Identity: nextIdentity, Epoch: &old,
	})
	if state.Accepts(identity) || !state.Accepts(nextIdentity) || len(state.PreparedReplicaIDs) != 0 {
		t.Fatalf("retired epoch state = %+v", state)
	}
	if got := state.ReplicaIDs; len(got) != 3 || got[0] != 2 || got[2] != 4 {
		t.Fatalf("active replicas after retirement = %+v", got)
	}
}

func TestDataServingEpochCanAdvanceWithinOneRegistryLayoutForRecovery(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	state, identity := initializedRouteShard(t, registryLayout, "/g", "rk")
	recovery := identity.PermitIdentity
	recovery.SystemEpoch++
	replicas := append([]uint64(nil), state.ReplicaIDs...)
	applyDataOK(t, &state, 2, DataCommand{
		Type: DataPrepareEpoch, Identity: identity, Epoch: &recovery, ReplicaIDs: replicas,
	})
	if len(state.ServingEpochs) != 2 || state.ServingEpochs[1].RegistryLayoutDigest != identity.RegistryLayoutDigest {
		t.Fatalf("recovery epoch was not prepared under the active registryLayout: %+v", state.ServingEpochs)
	}
	recoveryIdentity := ShardRequestIdentity{PermitIdentity: recovery, ShardID: identity.ShardID}
	previous := identity.PermitIdentity
	applyDataOK(t, &state, 3, DataCommand{
		Type: DataRetireEpoch, Identity: recoveryIdentity, Epoch: &previous,
	})
	if len(state.ServingEpochs) != 1 || state.ServingEpochs[0] != recovery {
		t.Fatalf("pre-recovery epoch remained active: %+v", state.ServingEpochs)
	}
}

func initializedRouteShard(t *testing.T, registryLayout RegistryLayout, group, routeKey string) (DataState, ShardRequestIdentity) {
	t.Helper()
	identity := routeShardIdentity(t, registryLayout, group, routeKey)
	return initializeDataShard(t, registryLayout, identity), identity
}

func initializedBuildShard(t *testing.T, registryLayout RegistryLayout, group, buildID string) (DataState, ShardRequestIdentity) {
	t.Helper()
	_, shardID, err := clusterstate.BuildShardFor(group, buildID, registryLayout.BuildBucketCount, registryLayout.VirtualShardCount)
	if err != nil {
		t.Fatal(err)
	}
	identity := registryLayoutShardIdentity(t, registryLayout, shardID)
	return initializeDataShard(t, registryLayout, identity), identity
}

func routeShardIdentity(t *testing.T, registryLayout RegistryLayout, group, routeKey string) ShardRequestIdentity {
	t.Helper()
	_, shardID, err := clusterstate.RouteShardFor(group, routeKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount)
	if err != nil {
		t.Fatal(err)
	}
	return registryLayoutShardIdentity(t, registryLayout, shardID)
}

func registryLayoutShardIdentity(t *testing.T, registryLayout RegistryLayout, shardID uint32) ShardRequestIdentity {
	t.Helper()
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: digest,
	}, ShardID: shardID}
}

func initializeDataShard(t *testing.T, registryLayout RegistryLayout, identity ShardRequestIdentity) DataState {
	t.Helper()
	state := DataState{}
	bootstrap, err := NewDataShardBootstrap(registryLayout, identity.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	applyDataOK(t, &state, 1, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: replicaIDsForPlacement(registryLayout.DataShards[identity.ShardID]),
	})
	return state
}

func applyDataOK(t *testing.T, state *DataState, index uint64, command DataCommand) {
	t.Helper()
	result := ApplyDataCommand(state, index, command)
	if !result.Applied || result.Conflict || result.Revision != index {
		t.Fatalf("apply index %d command %s = %+v", index, command.Type, result)
	}
}

func routeStarting(t *testing.T, registryLayout RegistryLayout, group, routeKey, sandboxID string, round uint64, selected bool) clusterstate.RouteWorkflowRecord {
	t.Helper()
	intent := testDispatchIntent(t)
	candidates := []clusterstate.PlacementCandidate{{NodeID: "node-1"}, {NodeID: "node-2"}}
	starting := &clusterstate.RouteStartingState{
		SandboxID: sandboxID, PlacementRound: round, CandidatePool: candidates, Intent: intent,
	}
	if selected {
		index := uint32(0)
		binding := testBinding(t, registryLayout, clusterstate.ExecutionKindSandbox, sandboxID, group, routeKey, "node-1", intent)
		starting.SelectedCandidate, starting.Binding = &index, &binding
	}
	return clusterstate.RouteWorkflowRecord{Group: group, RouteKey: routeKey, State: clusterstate.WorkflowRouteStarting, Starting: starting}
}

func withRevision(record clusterstate.RouteWorkflowRecord, state DataState, index uint64) clusterstate.RouteWorkflowRecord {
	record.Revision = revisionFor(state, index)
	return record
}

func readyRecord(starting clusterstate.RouteWorkflowRecord, eventSeq uint64) clusterstate.RouteWorkflowRecord {
	binding := starting.Starting.Binding
	spec, err := clusterstate.ParseSandboxDispatchSpec(starting.Starting.Intent.DispatchSpec)
	if err != nil {
		panic(err)
	}
	return clusterstate.RouteWorkflowRecord{
		Group: starting.Group, RouteKey: starting.RouteKey, State: clusterstate.WorkflowRouteReady,
		Ready: &clusterstate.ReadyRoute{
			SandboxID: starting.Starting.SandboxID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
			DataEndpoint: binding.DataEndpoint, TargetPort: spec.TargetPort, AccessToken: spec.AccessToken,
			TrafficAccessToken: "traffic-token", TemplateRef: spec.TemplateRef, RegistryGeneration: binding.RegistryGeneration,
			BindingDigest: binding.BindingDigest, LastEventSeq: eventSeq, Intent: starting.Starting.Intent,
			Presentation: testSandboxPresentation(),
		},
	}
}

func testSandboxPresentation() clusterstate.SandboxPresentationV1 {
	return clusterstate.SandboxPresentationV1{
		CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1", StartedAt: 1, EndAt: 2,
	}
}

func pausedRecord(ready clusterstate.RouteWorkflowRecord, eventSeq uint64) clusterstate.RouteWorkflowRecord {
	projection := *ready.Ready
	projection.LastEventSeq = eventSeq
	intent := testDispatchIntentNoFail()
	return clusterstate.RouteWorkflowRecord{
		Group: ready.Group, RouteKey: ready.RouteKey, State: clusterstate.WorkflowRoutePaused,
		Paused: &clusterstate.PausedRouteState{Execution: projection, SnapshotRef: "snapshot-1", ResumeIntent: intent},
	}
}

func deletingRecord(ready clusterstate.RouteWorkflowRecord) clusterstate.RouteWorkflowRecord {
	spec := []byte(`{"delete":true}`)
	digest := sha256.Sum256(spec)
	return clusterstate.RouteWorkflowRecord{
		Group: ready.Group, RouteKey: ready.RouteKey, State: clusterstate.WorkflowRouteDeleting,
		Deleting: &clusterstate.DeletingRouteState{
			Execution: *ready.Ready, DeleteSpec: spec, DeleteSpecDigest: hexDigestBytes(digest),
			LastEventSeq: ready.Ready.LastEventSeq,
		},
	}
}

func terminalRouteAndFence(deleting clusterstate.RouteWorkflowRecord, eventSeq uint64) (clusterstate.RouteWorkflowRecord, clusterstate.ExecutionFence) {
	execution := deleting.Deleting.Execution
	proof := clusterstate.TerminalProof{
		Kind: clusterstate.ProofNodeTerminal, FencedNodeID: execution.NodeID, FencedNodeEpoch: execution.NodeEpoch,
	}
	digest, err := clusterstate.NodeTerminalProofDigest(
		proof, execution.RegistryGeneration, execution.SandboxID, execution.BindingDigest, eventSeq,
	)
	if err != nil {
		panic(err)
	}
	proof.ProofDigest = digest
	tombstone := clusterstate.RouteWorkflowRecord{
		Group: deleting.Group, RouteKey: deleting.RouteKey, State: clusterstate.WorkflowRouteTombstone,
		Tombstone: &clusterstate.RouteTombstoneState{
			SandboxID: execution.SandboxID, NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch,
			RegistryGeneration: execution.RegistryGeneration, BindingDigest: execution.BindingDigest,
			LastEventSeq: eventSeq, Proof: proof, TerminalReason: "deleted",
		},
	}
	fence := clusterstate.ExecutionFence{
		Group: deleting.Group, RouteKey: deleting.RouteKey, SandboxID: execution.SandboxID,
		NodeID: execution.NodeID, NodeEpoch: execution.NodeEpoch, RegistryGeneration: execution.RegistryGeneration,
		BindingDigest: execution.BindingDigest, LastEventSeq: eventSeq, FinalOutboxWatermark: eventSeq, Proof: proof,
	}
	return tombstone, fence
}

func fenceCompaction(fence clusterstate.ExecutionFence, replicas []uint64) FenceCompactionAuthorization {
	proofs := make([]ReplicaAppliedProof, len(replicas))
	for index, replicaID := range replicas {
		proofs[index] = ReplicaAppliedProof{ReplicaID: replicaID, AppliedIndex: fence.Revision.LogIndex + 1}
	}
	return FenceCompactionAuthorization{
		Group: fence.Group, RouteKey: fence.RouteKey, SandboxID: fence.SandboxID,
		FenceRevision: fence.Revision.LogIndex, TerminalProofDigest: fence.ProofDigest(),
		FinalOutboxWatermarkAcked: true, ReplicaApplied: proofs, RetentionProofDigest: digestFor("retention-window"),
	}
}

func buildStarting(t *testing.T, registryLayout RegistryLayout, group, buildID string, selected bool) clusterstate.BuildRecord {
	t.Helper()
	intent := testBuildDispatchIntent(t)
	starting := &clusterstate.BuildStartingState{
		BuildID: buildID, CandidatePool: []clusterstate.PlacementCandidate{{NodeID: "node-1"}, {NodeID: "node-2"}}, Intent: intent,
	}
	if selected {
		index := uint32(0)
		binding := testBinding(t, registryLayout, clusterstate.ExecutionKindBuild, buildID, group, "", "node-1", intent)
		starting.SelectedCandidate, starting.Binding = &index, &binding
	}
	return clusterstate.BuildRecord{Group: group, BuildID: buildID, State: clusterstate.BuildStarting, Starting: starting}
}

func buildRegistrationRecord(starting clusterstate.BuildRecord) clusterstate.BuildRecord {
	binding := starting.Starting.Binding
	spec, err := clusterstate.ParseBuildDispatchSpec(starting.Starting.Intent.DispatchSpec)
	if err != nil {
		panic(err)
	}
	record := clusterstate.BuildRecord{
		Group: starting.Group, BuildID: starting.BuildID, State: clusterstate.BuildRegistered,
		Projection: &clusterstate.BuildProjection{
			BuildID: starting.BuildID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch, DataEndpoint: binding.DataEndpoint,
			RegistryGeneration: binding.RegistryGeneration, BindingDigest: binding.BindingDigest,
			Intent: starting.Starting.Intent, TemplateRef: spec.TemplateID,
		},
	}
	return record
}

func cloneBuildProjection(projection *clusterstate.BuildProjection) *clusterstate.BuildProjection {
	cloned := *projection
	return &cloned
}

func workflowFinalization(
	t *testing.T,
	objectID string,
	binding clusterstate.ExecutionBindingIntent,
	proof *clusterstate.TerminalProof,
) clusterstate.WorkflowFinalizationIntent {
	if t != nil {
		t.Helper()
	}
	intent, err := clusterstate.NewWorkflowFinalizationIntent(objectID, binding, proof)
	if err != nil {
		if t == nil {
			panic(err)
		}
		t.Fatal(err)
	}
	return intent
}

func testDispatchIntent(t *testing.T) clusterstate.DispatchIntent {
	t.Helper()
	demand, err := placement.NormalizeSandboxDemand(placement.SandboxDemand{SlotUnits: 1})
	if err != nil {
		t.Fatal(err)
	}
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "access-token", TargetPort: 8080,
		Request: clusterstate.NodeRequestEnvelopeV1{Version: clusterstate.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes", Body: []byte(`{"metadata":null,"templateID":"` + templateRef + `","timeout":0}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func testDispatchIntentNoFail() clusterstate.DispatchIntent {
	demand, err := placement.NormalizeSandboxDemand(placement.SandboxDemand{SlotUnits: 1})
	if err != nil {
		panic(err)
	}
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "access-token", TargetPort: 8080,
		Request: clusterstate.NodeRequestEnvelopeV1{Version: clusterstate.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes", Body: []byte(`{"metadata":null,"templateID":"` + templateRef + `","timeout":0}`)},
	})
	if err != nil {
		panic(err)
	}
	intent, err := clusterstate.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		panic(err)
	}
	return intent
}

func testBuildDispatchIntent(t *testing.T) clusterstate.DispatchIntent {
	t.Helper()
	demand, err := placement.NormalizeBuildDemand(placement.BuildDemand{Slots: 1})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateID: "template-1",
		AuthKeyFingerprint: strings.Repeat("b", 24), ManifestKeyFingerprint: strings.Repeat("c", 24),
		Profile: types.ProfileBare, CPUCount: 1, MemoryMB: 512,
		Request: clusterstate.NodeRequestEnvelopeV1{Version: clusterstate.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/v3/templates", Body: []byte(`{"cpuCount":1,"memoryMB":512,"metadata":null,"name":"","profile":"bare","tags":null}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := clusterstate.NewDispatchIntent(demand, spec, "provider-v1/policy-v1")
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func testBinding(
	t *testing.T,
	registryLayout RegistryLayout,
	kind clusterstate.ExecutionKind,
	objectID, group, routeKey, nodeID string,
	intent clusterstate.DispatchIntent,
) clusterstate.ExecutionBindingIntent {
	t.Helper()
	demandDigest := sha256.Sum256(intent.NormalizedDemand)
	specDigest := sha256.Sum256(intent.DispatchSpec)
	opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		RegistryGeneration: registryLayout.RegistryGeneration, Kind: kind, ObjectID: objectID, Group: group,
		RouteKey: routeKey, NodeID: nodeID, NodeEpoch: 7, DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		t.Fatal(err)
	}
	return clusterstate.ExecutionBindingIntent{
		NodeID: nodeID, NodeEpoch: 7, DataEndpoint: nodeID + ":8443",
		RegistryGeneration: registryLayout.RegistryGeneration, OpaqueBinding: opaque, BindingDigest: digest,
	}
}

func hexDigestBytes(value [sha256.Size]byte) string {
	const alphabet = "0123456789abcdef"
	encoded := make([]byte, len(value)*2)
	for index, b := range value {
		encoded[index*2] = alphabet[b>>4]
		encoded[index*2+1] = alphabet[b&0x0f]
	}
	return string(encoded)
}

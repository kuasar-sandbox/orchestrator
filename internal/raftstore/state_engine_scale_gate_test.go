package raftstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	stateScaleGateEnvironment = "KUASAR_RAFT_STATE_GATE"
	stateScaleRouteCount      = 1_000_000
	stateScaleReadCount       = 50_000
	stateScaleLoadMax         = 5 * time.Minute
	stateScaleRSSMax          = uint64(16 << 30)
)

// TestPebbleMillionReadyRouteGate validates the state-engine half of the RFC
// capacity target. The separate Dragonboat gate validates 4,097 live groups;
// this gate validates one million final READY rows, restart, and exact-key
// local reads without retaining whole-shard maps in memory.
func TestPebbleMillionReadyRouteGate(t *testing.T) {
	if os.Getenv(stateScaleGateEnvironment) != "1" {
		t.Skipf("set %s=1 to run the million-Route state-engine gate", stateScaleGateEnvironment)
	}
	startedAt := time.Now()
	registryLayout := testRegistryLayout(DefaultVirtualShards, "generation-state-scale")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	identity := PermitIdentity{
		ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: digest,
	}
	const groupName = "/state-scale"
	shardRoutes := make([][]uint32, DefaultVirtualShards)
	routeShards := make([]uint32, stateScaleRouteCount)
	for routeID := uint32(0); routeID < stateScaleRouteCount; routeID++ {
		routeKey := stateScaleRouteKey(routeID)
		_, shardID, err := clusterstate.RouteShardFor(
			groupName, routeKey, registryLayout.RouteBucketCount, registryLayout.VirtualShardCount,
		)
		if err != nil {
			t.Fatal(err)
		}
		shardRoutes[shardID] = append(shardRoutes[shardID], routeID)
		routeShards[routeID] = shardID
	}

	root := t.TempDir()
	engine, err := OpenPebbleStateEngine(root)
	if err != nil {
		t.Fatal(err)
	}
	machines := make([]sm.IOnDiskStateMachine, DefaultVirtualShards)
	intent := testDispatchIntentNoFail()
	loadGroup, loadContext := errgroup.WithContext(context.Background())
	loadGroup.SetLimit(64)
	for shardID := uint32(0); shardID < DefaultVirtualShards; shardID++ {
		shardID := shardID
		loadGroup.Go(func() error {
			select {
			case <-loadContext.Done():
				return loadContext.Err()
			default:
			}
			machine := engine.NewStateMachine(DataRaftShardID(shardID), 1)
			if index, err := machine.Open(make(chan struct{})); err != nil || index != 0 {
				return fmt.Errorf("open shard %d at %d: %w", shardID, index, err)
			}
			bootstrap, err := NewDataShardBootstrap(registryLayout, shardID)
			if err != nil {
				return err
			}
			shardIdentity := ShardRequestIdentity{PermitIdentity: identity, ShardID: shardID}
			bootstrapRaw, err := EncodeDataCommand(DataCommand{
				Type: DataInitializeShard, Identity: shardIdentity, Bootstrap: &bootstrap,
				ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
			})
			if err != nil {
				return err
			}
			bootstrapEntries, err := machine.Update([]sm.Entry{{Index: 1, Cmd: bootstrapRaw}})
			if err != nil {
				return fmt.Errorf("bootstrap shard %d: %w", shardID, err)
			}
			if len(bootstrapEntries) != 1 || bootstrapEntries[0].Result.Value != 1 {
				return fmt.Errorf("bootstrap shard %d returned %+v", shardID, bootstrapEntries)
			}

			entries := make([]sm.Entry, 0, len(shardRoutes[shardID])*2)
			for position, routeID := range shardRoutes[shardID] {
				starting, ready, err := stateScaleReadyRoute(registryLayout, groupName, stateScaleRouteKey(routeID), routeID, intent)
				if err != nil {
					return err
				}
				startIndex := uint64(position*2 + 2)
				startRaw, err := EncodeDataCommand(DataCommand{
					Type: DataPutRoute, Identity: shardIdentity,
					Expect: RevisionExpectation{Absent: true}, Route: &starting,
				})
				if err != nil {
					return err
				}
				readyRaw, err := EncodeDataCommand(DataCommand{
					Type: DataPutRoute, Identity: shardIdentity,
					Expect: RevisionExpectation{LogIndex: startIndex}, Route: &ready,
				})
				if err != nil {
					return err
				}
				entries = append(entries,
					sm.Entry{Index: startIndex, Cmd: startRaw},
					sm.Entry{Index: startIndex + 1, Cmd: readyRaw},
				)
			}
			entries, err = machine.Update(entries)
			if err != nil {
				return fmt.Errorf("load shard %d: %w", shardID, err)
			}
			for _, entry := range entries {
				if entry.Result.Value != 1 {
					return fmt.Errorf("load shard %d index %d was rejected", shardID, entry.Index)
				}
			}
			machines[shardID] = machine
			return nil
		})
	}
	if err := loadGroup.Wait(); err != nil {
		engine.Close()
		t.Fatal(err)
	}
	if err := engine.sync(); err != nil {
		engine.Close()
		t.Fatal(err)
	}
	loadDuration := time.Since(startedAt)
	if loadDuration > stateScaleLoadMax {
		t.Errorf("million-Route load took %s; limit %s", loadDuration, stateScaleLoadMax)
	}
	for _, machine := range machines {
		if err := machine.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	engine, err = OpenPebbleStateEngine(root)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for shardID := uint32(0); shardID < DefaultVirtualShards; shardID++ {
		machine := engine.NewStateMachine(DataRaftShardID(shardID), 1)
		wantIndex := uint64(len(shardRoutes[shardID])*2 + 1)
		if index, err := machine.Open(make(chan struct{})); err != nil || index != wantIndex {
			t.Fatalf("restart shard %d at %d, want %d: %v", shardID, index, wantIndex, err)
		}
		machines[shardID] = machine
	}

	for index := 0; index < 10_000; index++ {
		routeID := uint32((uint64(index) * 7919) % stateScaleRouteCount)
		if err := stateScaleLookupReady(machines, identity, routeShards, groupName, routeID); err != nil {
			t.Fatal(err)
		}
	}
	latencies := make([]time.Duration, stateScaleReadCount)
	for index := range latencies {
		routeID := uint32((uint64(index+10_000) * 7919) % stateScaleRouteCount)
		started := time.Now()
		err := stateScaleLookupReady(machines, identity, routeShards, groupName, routeID)
		latencies[index] = time.Since(started)
		if err != nil {
			t.Fatal(err)
		}
	}
	slices.Sort(latencies)
	p50 := percentileDuration(latencies, 50)
	p99 := percentileDuration(latencies, 99)
	p999 := percentileDuration(latencies, 99.9)
	if p99 > 5*time.Millisecond {
		t.Errorf("million-row local READY read P99 = %s; limit <=5ms", p99)
	}

	runtime.GC()
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	rssBytes := uint64(usage.Maxrss) * 1024
	if rssBytes > stateScaleRSSMax {
		t.Errorf("maximum RSS = %.2f GiB; limit 16 GiB", float64(rssBytes)/(1<<30))
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf(
		"routes=%d shards=%d load=%s ready_read_p50=%s ready_read_p99=%s ready_read_p99.9=%s max_rss=%.2fGiB heap_sys=%.2fGiB",
		stateScaleRouteCount, DefaultVirtualShards, loadDuration, p50, p99, p999,
		float64(rssBytes)/(1<<30), float64(memory.HeapSys)/(1<<30),
	)
}

func stateScaleRouteKey(routeID uint32) string { return fmt.Sprintf("route-%07d", routeID) }

func stateScaleReadyRoute(
	registryLayout RegistryLayout,
	group, routeKey string,
	routeID uint32,
	intent clusterstate.DispatchIntent,
) (clusterstate.RouteWorkflowRecord, clusterstate.RouteWorkflowRecord, error) {
	sandboxID := fmt.Sprintf("sandbox-%07d", routeID)
	nodeID := fmt.Sprintf("node-%04d", routeID%5000)
	demandDigest := sha256.Sum256(intent.NormalizedDemand)
	specDigest := sha256.Sum256(intent.DispatchSpec)
	opaque, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		RegistryGeneration: registryLayout.RegistryGeneration, Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: sandboxID, Group: group, RouteKey: routeKey, NodeID: nodeID, NodeEpoch: 7,
		DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, clusterstate.RouteWorkflowRecord{}, err
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, clusterstate.RouteWorkflowRecord{}, err
	}
	selected := uint32(0)
	binding := clusterstate.ExecutionBindingIntent{
		NodeID: nodeID, NodeEpoch: 7, DataEndpoint: nodeID + ":8443",
		RegistryGeneration: registryLayout.RegistryGeneration, OpaqueBinding: opaque, BindingDigest: bindingDigest,
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return clusterstate.RouteWorkflowRecord{}, clusterstate.RouteWorkflowRecord{}, err
	}
	starting := clusterstate.RouteWorkflowRecord{
		Group: group, RouteKey: routeKey, State: clusterstate.WorkflowRouteStarting,
		Starting: &clusterstate.RouteStartingState{
			SandboxID: sandboxID, PlacementRound: 1,
			CandidatePool:     []clusterstate.PlacementCandidate{{NodeID: nodeID}},
			SelectedCandidate: &selected, Intent: intent, Binding: &binding,
		},
	}
	ready := clusterstate.RouteWorkflowRecord{
		Group: group, RouteKey: routeKey, State: clusterstate.WorkflowRouteReady,
		Ready: &clusterstate.ReadyRoute{
			SandboxID: sandboxID, NodeID: nodeID, NodeEpoch: 7, DataEndpoint: nodeID + ":8443",
			TargetPort: spec.TargetPort, AccessToken: spec.AccessToken, TrafficAccessToken: "traffic-token",
			TemplateRef:        spec.TemplateRef,
			RegistryGeneration: registryLayout.RegistryGeneration, BindingDigest: bindingDigest, LastEventSeq: 1, Intent: intent,
		},
	}
	return starting, ready, nil
}

func stateScaleLookupReady(
	machines []sm.IOnDiskStateMachine,
	identity PermitIdentity,
	routeShards []uint32,
	group string,
	routeID uint32,
) error {
	shardID := routeShards[routeID]
	request := routeapi.ReadRouteRequest{
		RequestIdentity: routeapi.RequestIdentity{
			ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
			SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest, ShardID: shardID,
		},
		Group: group, RouteKey: stateScaleRouteKey(routeID),
	}
	value, err := machines[shardID].Lookup(DataLookup{Route: &request})
	if err != nil {
		return err
	}
	result, ok := value.(DataLookupResult)
	if !ok || result.Route == nil || result.Route.Outcome != routeapi.ReadReady || result.Route.Route == nil {
		return fmt.Errorf("route %d lookup returned %#v", routeID, value)
	}
	return nil
}

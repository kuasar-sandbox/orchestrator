package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	scaleGateEnvironment = "KUASAR_RAFT_SCALE_GATE"
	scaleGateStartupMax  = 2 * time.Minute
	scaleGateReadCount   = 20_000
	scaleGateRSSMax      = uint64(8 << 30)
)

func TestDragonboat4097GroupScaleGate(t *testing.T) {
	if os.Getenv(scaleGateEnvironment) != "1" {
		t.Skipf("set %s=1 to run the Dragonboat capacity gate", scaleGateEnvironment)
	}
	for _, name := range []string{"dragonboat", "config", "logdb", "raft", "rsm", "transport"} {
		log := logger.GetLogger(name)
		log.SetLevel(logger.CRITICAL)
		t.Cleanup(func() { log.SetLevel(logger.INFO) })
	}

	addresses := make([]string, DefaultReplication)
	nodeHosts := make([]*dragonboat.NodeHost, DefaultReplication)
	for index := range addresses {
		addresses[index] = freeTCPAddress(t)
	}
	for index, address := range addresses {
		expert := dbconfig.GetDefaultExpertConfig()
		expert.LogDB = dbconfig.GetTinyMemLogDBConfig()
		expert.Engine.SnapshotShards = 4
		nodeHost, err := dragonboat.NewNodeHost(dbconfig.NodeHostConfig{
			DeploymentID: 0x46_4097, NodeHostDir: t.TempDir(), RTTMillisecond: 2,
			RaftAddress: address, Expert: expert,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodeHosts[index] = nodeHost
		t.Cleanup(nodeHost.Close)
	}

	manifest := testManifest(DefaultVirtualShards, "generation-scale-gate")
	for index := range manifest.Members {
		manifest.Members[index].RaftEndpoint = addresses[index]
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	initial := map[uint64]dragonboat.Target{
		1: addresses[0],
		2: addresses[1],
		3: addresses[2],
	}

	startedAt := time.Now()
	for logicalShardID := int64(-1); logicalShardID < int64(DefaultVirtualShards); logicalShardID++ {
		raftShardID := SystemRaftShardID
		factory := NewSystemStateMachine
		if logicalShardID >= 0 {
			raftShardID = DataRaftShardID(uint32(logicalShardID))
			factory = NewDataStateMachine
		}
		for index, nodeHost := range nodeHosts {
			if err := nodeHost.StartReplica(
				initial, false, factory, scaleGateRaftConfig(raftShardID, uint64(index+1)),
			); err != nil {
				t.Fatalf("start shard %d replica %d: %v", raftShardID, index+1, err)
			}
		}
	}

	deadline := startedAt.Add(scaleGateStartupMax)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	bootstrapRaw, err := EncodeSystemCommand(SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := proposeScaleGate(ctx, nodeHosts, SystemRaftShardID, bootstrapRaw)
	if err != nil {
		t.Fatalf("initialize System Group: %v", err)
	}
	var systemResult SystemApplyResult
	if err := json.Unmarshal(result.Data, &systemResult); err != nil || !systemResult.Applied {
		t.Fatalf("System Group initialization = %+v, %v", systemResult, err)
	}

	group, groupContext := errgroup.WithContext(ctx)
	group.SetLimit(128)
	for logicalShardID := uint32(0); logicalShardID < DefaultVirtualShards; logicalShardID++ {
		logicalShardID := logicalShardID
		group.Go(func() error {
			bootstrap, err := dataShardBootstrap(manifest, digest, logicalShardID)
			if err != nil {
				return err
			}
			identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
				ClusterID: manifest.ClusterID, StorageGeneration: manifest.StorageGeneration,
				SystemEpoch: 1, ManifestDigest: digest,
			}, ShardID: logicalShardID}
			command, err := EncodeDataCommand(DataCommand{
				Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
				ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
			})
			if err != nil {
				return err
			}
			result, err := proposeScaleGate(groupContext, nodeHosts, DataRaftShardID(logicalShardID), command)
			if err != nil {
				return fmt.Errorf("initialize data shard %d: %w", logicalShardID, err)
			}
			var applied DataApplyResult
			if err := json.Unmarshal(result.Data, &applied); err != nil {
				return err
			}
			if !applied.Applied {
				return fmt.Errorf("initialize data shard %d: %s", logicalShardID, applied.Reason)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	startupDuration := time.Since(startedAt)
	if startupDuration > scaleGateStartupMax {
		t.Fatalf("4097-group startup and initialization took %s; limit %s", startupDuration, scaleGateStartupMax)
	}

	for logicalShardID := uint32(0); logicalShardID < DefaultVirtualShards; logicalShardID++ {
		if _, err := nodeHosts[0].StaleRead(DataRaftShardID(logicalShardID), DataStateLookup{}); err != nil {
			t.Fatalf("warm local read for shard %d: %v", logicalShardID, err)
		}
	}
	latencies := make([]time.Duration, scaleGateReadCount)
	for index := range latencies {
		logicalShardID := uint32(index % int(DefaultVirtualShards))
		started := time.Now()
		value, err := nodeHosts[0].StaleRead(DataRaftShardID(logicalShardID), DataStateLookup{})
		latencies[index] = time.Since(started)
		if err != nil {
			t.Fatalf("local read for shard %d: %v", logicalShardID, err)
		}
		state, ok := value.(DataState)
		if !ok || !state.Initialized || state.ShardID != logicalShardID {
			t.Fatalf("local read for shard %d returned %#v", logicalShardID, value)
		}
	}
	slices.Sort(latencies)
	p50 := percentileDuration(latencies, 50)
	p99 := percentileDuration(latencies, 99)
	p999 := percentileDuration(latencies, 99.9)
	if p99 > 5*time.Millisecond {
		t.Errorf("local state read P99 = %s; limit <=5ms", p99)
	}

	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	rssBytes := uint64(usage.Maxrss) * 1024
	if rssBytes > scaleGateRSSMax {
		t.Errorf("maximum RSS = %.2f GiB; limit 8 GiB", float64(rssBytes)/(1<<30))
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf(
		"groups=%d replicas=%d startup=%s local_state_read_p50=%s local_state_read_p99=%s local_state_read_p99.9=%s max_rss=%.2fGiB heap_sys=%.2fGiB",
		DefaultVirtualShards+1, (DefaultVirtualShards+1)*DefaultReplication, startupDuration,
		p50, p99, p999, float64(rssBytes)/(1<<30), float64(memory.HeapSys)/(1<<30),
	)
}

func scaleGateRaftConfig(shardID, replicaID uint64) dbconfig.Config {
	return dbconfig.Config{
		ShardID: shardID, ReplicaID: replicaID, CheckQuorum: true, PreVote: true,
		ElectionRTT: 20, HeartbeatRTT: 2, OrderedConfigChange: true,
		SnapshotEntries: 0, CompactionOverhead: 0, MaxInMemLogSize: 4 << 20,
	}
}

func proposeScaleGate(
	ctx context.Context,
	nodeHosts []*dragonboat.NodeHost,
	shardID uint64,
	command []byte,
) (sm.Result, error) {
	var last error
	for {
		for _, nodeHost := range nodeHosts {
			attemptContext, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			result, err := nodeHost.SyncPropose(
				attemptContext, nodeHost.GetNoOPSession(shardID), command,
			)
			cancel()
			if err == nil {
				return result, nil
			}
			last = err
		}
		select {
		case <-ctx.Done():
			if last == nil {
				last = errors.New("no proposal target available")
			}
			return sm.Result{}, fmt.Errorf("%w: %v", ctx.Err(), last)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func percentileDuration(sorted []time.Duration, percentile float64) time.Duration {
	index := int(float64(len(sorted)-1) * percentile / 100)
	return sorted[index]
}

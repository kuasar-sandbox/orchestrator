package controlplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

type registryLayoutTransitionRuntime interface {
	LocalSystemLeader() (bool, error)
	ReadSystemStrong(context.Context) (raftstore.SystemState, error)
	ReconcileRegistryLayoutTransitionShard(context.Context, uint64) (raftstore.SystemState, error)
	ActivateRegistryLayoutTransition(context.Context) (raftstore.SystemState, error)
	ConfirmRegistryLayoutTransitionPermitDrain(context.Context) (raftstore.SystemState, error)
	FinalizeRegistryLayoutTransition(context.Context) (raftstore.SystemState, error)
	RefreshPermit(context.Context) (raftstore.PermitGrant, error)
}

// CompleteRegistryLayoutTransition runs only on the current System leader. Every
// edge is proof-driven and idempotent, so a leader or process restart resumes
// from committed progress without a second transition history.
func CompleteRegistryLayoutTransition(
	ctx context.Context,
	runtime registryLayoutTransitionRuntime,
	workers int,
	log *slog.Logger,
) error {
	if runtime == nil {
		return errors.New("controlplane: registryLayout transition runtime is required")
	}
	if workers <= 0 || workers > 256 {
		return errors.New("controlplane: registryLayout transition workers must be in [1,256]")
	}
	if log == nil {
		log = slog.Default()
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastLogged time.Time
	for {
		state, err := runtime.ReadSystemStrong(ctx)
		if err == nil && state.Transition == nil {
			return nil
		}
		if err == nil {
			leader, leaderErr := runtime.LocalSystemLeader()
			if leaderErr != nil {
				err = leaderErr
			} else if leader {
				err = advanceRegistryLayoutTransition(ctx, runtime, state, workers)
			}
		}
		if err != nil && time.Since(lastLogged) >= time.Second {
			log.Warn("Registry registryLayout transition is waiting", "err", err)
			lastLogged = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func advanceRegistryLayoutTransition(
	ctx context.Context,
	runtime registryLayoutTransitionRuntime,
	state raftstore.SystemState,
	workers int,
) error {
	transition := state.Transition
	if transition == nil {
		return nil
	}
	if !transition.Activated {
		if transitionShardsAt(transition.Shards, raftstore.TransitionComplete) {
			if _, err := runtime.ActivateRegistryLayoutTransition(ctx); err != nil {
				return err
			}
			_, err := runtime.RefreshPermit(ctx)
			return err
		}
		return reconcileTransitionShards(ctx, runtime, transition.Shards, workers, false)
	}
	if !transition.PreviousPermitDrainComplete {
		if _, err := runtime.RefreshPermit(ctx); err != nil {
			return err
		}
		_, err := runtime.ConfirmRegistryLayoutTransitionPermitDrain(ctx)
		return err
	}
	if !transitionShardsAt(transition.Shards, raftstore.TransitionEpochRetired) {
		if _, err := runtime.RefreshPermit(ctx); err != nil {
			return err
		}
		return reconcileTransitionShards(ctx, runtime, transition.Shards, workers, true)
	}
	_, err := runtime.FinalizeRegistryLayoutTransition(ctx)
	return err
}

func reconcileTransitionShards(
	ctx context.Context,
	runtime registryLayoutTransitionRuntime,
	progress []raftstore.ShardTransition,
	workers int,
	retiring bool,
) error {
	jobs := make(chan uint64)
	var group sync.WaitGroup
	var resultMu sync.Mutex
	var joined error
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for shardID := range jobs {
				if _, err := runtime.ReconcileRegistryLayoutTransitionShard(ctx, shardID); err != nil {
					resultMu.Lock()
					joined = errors.Join(joined, fmt.Errorf("Raft shard %d: %w", shardID, err))
					resultMu.Unlock()
				}
			}
		}()
	}
	for _, shard := range progress {
		if !retiring && shard.Stage == raftstore.TransitionComplete ||
			retiring && shard.Stage == raftstore.TransitionEpochRetired {
			continue
		}
		raftShardID := raftstore.DataRaftShardID(shard.ShardID)
		if shard.ShardID == ^uint32(0) {
			raftShardID = raftstore.SystemRaftShardID
		}
		select {
		case jobs <- raftShardID:
		case <-ctx.Done():
			close(jobs)
			group.Wait()
			return errors.Join(ctx.Err(), joined)
		}
	}
	close(jobs)
	group.Wait()
	return joined
}

func transitionShardsAt(shards []raftstore.ShardTransition, stage raftstore.TransitionStage) bool {
	for _, shard := range shards {
		if shard.Stage != stage {
			return false
		}
	}
	return true
}

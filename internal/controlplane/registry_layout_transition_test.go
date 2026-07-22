package controlplane

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

type transitionRuntimeStub struct {
	mu        sync.Mutex
	state     raftstore.SystemState
	refreshes int
}

func (s *transitionRuntimeStub) LocalSystemLeader() (bool, error) { return true, nil }

func (s *transitionRuntimeStub) ReadSystemStrong(context.Context) (raftstore.SystemState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.state
	if s.state.Transition != nil {
		transition := *s.state.Transition
		transition.Shards = append([]raftstore.ShardTransition(nil), s.state.Transition.Shards...)
		state.Transition = &transition
	}
	return state, nil
}

func (s *transitionRuntimeStub) ReconcileRegistryLayoutTransitionShard(
	_ context.Context,
	raftShardID uint64,
) (raftstore.SystemState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logical := uint32(0)
	if raftShardID == raftstore.SystemRaftShardID {
		logical = ^uint32(0)
	} else {
		logical, _ = raftstore.LogicalShardID(raftShardID)
	}
	for index := range s.state.Transition.Shards {
		shard := &s.state.Transition.Shards[index]
		if shard.ShardID != logical {
			continue
		}
		switch shard.Stage {
		case raftstore.TransitionPending:
			shard.Stage = raftstore.TransitionCatchingUp
		case raftstore.TransitionCatchingUp:
			shard.Stage = raftstore.TransitionPromoted
		case raftstore.TransitionPromoted:
			shard.Stage = raftstore.TransitionComplete
		case raftstore.TransitionComplete:
			shard.Stage = raftstore.TransitionEpochRetired
		}
		break
	}
	return s.state, nil
}

func (s *transitionRuntimeStub) ActivateRegistryLayoutTransition(context.Context) (raftstore.SystemState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Transition.Activated = true
	return s.state, nil
}

func (s *transitionRuntimeStub) ConfirmRegistryLayoutTransitionPermitDrain(context.Context) (raftstore.SystemState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Transition.PreviousPermitDrainComplete = true
	return s.state, nil
}

func (s *transitionRuntimeStub) FinalizeRegistryLayoutTransition(context.Context) (raftstore.SystemState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state.Transition = nil
	return s.state, nil
}

func (s *transitionRuntimeStub) RefreshPermit(context.Context) (raftstore.PermitGrant, error) {
	s.mu.Lock()
	s.refreshes++
	s.mu.Unlock()
	return raftstore.PermitGrant{}, nil
}

func TestRegistryLayoutTransitionCoordinatorCompletesEveryCommittedEdge(t *testing.T) {
	runtime := &transitionRuntimeStub{state: raftstore.SystemState{
		Transition: &raftstore.RegistryLayoutTransition{Shards: []raftstore.ShardTransition{
			{ShardID: ^uint32(0), Stage: raftstore.TransitionPending},
			{ShardID: 0, Stage: raftstore.TransitionPending},
		}},
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := CompleteRegistryLayoutTransition(ctx, runtime, 2, nil); err != nil {
		t.Fatal(err)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.state.Transition != nil || runtime.refreshes < 2 {
		t.Fatalf("final transition = %+v, Permit refreshes = %d", runtime.state.Transition, runtime.refreshes)
	}
}

package raftstore

import (
	"context"
	"errors"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

const startupRetryInterval = 10 * time.Millisecond

func (r *Runtime) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	timeoutMillis := r.config.Tuning.OperationTimeoutMillis
	if timeoutMillis == 0 {
		timeoutMillis = DefaultRuntimeTuning().OperationTimeoutMillis
	}
	timeout := time.Duration(timeoutMillis) * time.Millisecond
	return context.WithTimeout(ctx, timeout)
}

// retryStartupOperation is only for idempotent Registry History Generation startup and Permit
// refresh commands. Ordinary mutations must surface ambiguous outcomes.
func (r *Runtime) retryStartupOperation(
	ctx context.Context,
	attempt func(context.Context) (bool, error),
) error {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	var lastErr error
	for {
		done, err := attempt(operation)
		if done {
			return nil
		}
		if err != nil {
			if !dragonboat.IsTempError(err) {
				return err
			}
			lastErr = err
		}
		timer := time.NewTimer(startupRetryInterval)
		select {
		case <-operation.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if lastErr != nil {
				return errors.Join(operation.Err(), lastErr)
			}
			return operation.Err()
		case <-timer.C:
		}
	}
}

func (r *Runtime) syncPropose(
	ctx context.Context,
	session *client.Session,
	command []byte,
) (sm.Result, error) {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncPropose(operation, session, command)
}

func (r *Runtime) syncRead(ctx context.Context, shardID uint64, query any) (any, error) {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncRead(operation, shardID, query)
}

func (r *Runtime) syncGetShardMembership(ctx context.Context, shardID uint64) (*dragonboat.Membership, error) {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncGetShardMembership(operation, shardID)
}

func (r *Runtime) syncRequestAddNonVoting(
	ctx context.Context,
	shardID, replicaID uint64,
	target string,
	configChangeID uint64,
) error {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncRequestAddNonVoting(operation, shardID, replicaID, target, configChangeID)
}

func (r *Runtime) syncRequestAddReplica(
	ctx context.Context,
	shardID, replicaID uint64,
	target string,
	configChangeID uint64,
) error {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncRequestAddReplica(operation, shardID, replicaID, target, configChangeID)
}

func (r *Runtime) syncRequestDeleteReplica(
	ctx context.Context,
	shardID, replicaID, configChangeID uint64,
) error {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncRequestDeleteReplica(operation, shardID, replicaID, configChangeID)
}

func (r *Runtime) syncRemoveData(ctx context.Context, shardID, replicaID uint64) error {
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.nodeHost.SyncRemoveData(operation, shardID, replicaID)
}

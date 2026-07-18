package raftstore

import (
	"fmt"
	"sync"

	"github.com/lni/dragonboat/v4/raftio"
)

// runtimeSystemEvents turns Dragonboat's committed self-removal event into a
// durable local restart fence. Other events are observed by higher-level
// metrics and do not alter enrollment authority.
type runtimeSystemEvents struct {
	mu      sync.Mutex
	runtime *Runtime
	pending []raftio.NodeInfo
	err     error
}

func (e *runtimeSystemEvents) bind(runtime *Runtime) {
	e.mu.Lock()
	e.runtime = runtime
	pending := append([]raftio.NodeInfo(nil), e.pending...)
	e.pending = nil
	e.mu.Unlock()
	for _, info := range pending {
		e.recordDeleted(info)
	}
}

func (e *runtimeSystemEvents) unbind() {
	e.mu.Lock()
	e.runtime = nil
	e.mu.Unlock()
}

func (e *runtimeSystemEvents) Err() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

func (e *runtimeSystemEvents) recordDeleted(info raftio.NodeInfo) {
	e.mu.Lock()
	runtime := e.runtime
	if runtime == nil {
		e.pending = append(e.pending, info)
		e.mu.Unlock()
		return
	}
	e.mu.Unlock()
	if err := runtime.markReplicaRemoving(info.ShardID, info.ReplicaID); err != nil {
		e.mu.Lock()
		if e.err == nil {
			e.err = fmt.Errorf("raftstore: persist removed replica fence: %w", err)
		}
		e.mu.Unlock()
	}
}

func (*runtimeSystemEvents) NodeHostShuttingDown()                       {}
func (*runtimeSystemEvents) NodeUnloaded(raftio.NodeInfo)                {}
func (e *runtimeSystemEvents) NodeDeleted(info raftio.NodeInfo)          { e.recordDeleted(info) }
func (*runtimeSystemEvents) NodeReady(raftio.NodeInfo)                   {}
func (*runtimeSystemEvents) MembershipChanged(raftio.NodeInfo)           {}
func (*runtimeSystemEvents) ConnectionEstablished(raftio.ConnectionInfo) {}
func (*runtimeSystemEvents) ConnectionFailed(raftio.ConnectionInfo)      {}
func (*runtimeSystemEvents) SendSnapshotStarted(raftio.SnapshotInfo)     {}
func (*runtimeSystemEvents) SendSnapshotCompleted(raftio.SnapshotInfo)   {}
func (*runtimeSystemEvents) SendSnapshotAborted(raftio.SnapshotInfo)     {}
func (*runtimeSystemEvents) SnapshotReceived(raftio.SnapshotInfo)        {}
func (*runtimeSystemEvents) SnapshotRecovered(raftio.SnapshotInfo)       {}
func (*runtimeSystemEvents) SnapshotCreated(raftio.SnapshotInfo)         {}
func (*runtimeSystemEvents) SnapshotCompacted(raftio.SnapshotInfo)       {}
func (*runtimeSystemEvents) LogCompacted(raftio.EntryInfo)               {}
func (*runtimeSystemEvents) LogDBCompacted(raftio.EntryInfo)             {}

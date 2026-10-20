package proxyapp

import (
	"context"
	"errors"
	"fmt"
)

func (i *Instance) attachWorker(index int, epoch uint64, peer *controlPeer) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.stopping {
		return false
	}
	i.workers[index] = workerState{epoch: epoch, peer: peer, alive: true}
	i.recalculateLocked()
	i.notifyLocked()
	return true
}

func (i *Instance) workerExited(index int, epoch uint64, exitErr error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.workers[index].epoch != epoch {
		return
	}
	if i.stopping && i.workers[index].alive && !i.workers[index].stopped {
		i.err = errors.Join(i.err, ErrForcedClose, exitErr)
	}
	i.workers[index].alive = false
	i.workers[index].ready = false
	i.recalculateLocked()
	i.notifyLocked()
}

func (i *Instance) workerFault(index int, epoch uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.workers[index].epoch != epoch {
		return
	}
	i.workers[index].ready = false
	i.recalculateLocked()
	i.notifyLocked()
}

func (i *Instance) workerEvent(index int, epoch uint64, message controlMessage) error {
	i.mu.Lock()
	w := &i.workers[index]
	if w.epoch != epoch || !w.alive {
		i.mu.Unlock()
		return nil
	}
	var start bool
	switch message.Kind {
	case "prepared":
		if !w.prepared {
			w.prepared = true
			w.canShutdown = message.CanShutdown
			start = !i.stopping && !w.stopping
		}
	case "serving":
		if !w.prepared {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker serving before prepared")
		}
		if !i.stopping && !w.stopping {
			w.ready = true
		}
	case "stop_requested":
		// A worker's own termination signal stops only that worker. Preserve
		// normal supervision and the identity/ordering checks for its drain.
		w.stopping = true
		w.ready = false
	case "data_draining", "data_drained":
		if !i.stopping && !w.stopping {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker drained without a stop request")
		}
		w.dataDone = w.dataDone || message.Kind == "data_drained"
		w.ready = false
	case "mmds_drained":
		w.mmdsDone = true
	case "stopped":
		if !w.dataDone || !w.mmdsDone {
			i.mu.Unlock()
			return fmt.Errorf("proxy worker stopped before endpoints drained")
		}
		w.stopped = true
	default:
		i.mu.Unlock()
		return fmt.Errorf("proxy worker invalid lifecycle event %q", message.Kind)
	}
	// A separately signaled worker reports its own result on exit. Only errors
	// during this instance's drain belong to its eventual shutdown result.
	if i.stopping {
		if message.Forced {
			i.err = errors.Join(i.err, ErrForcedClose)
		}
		if message.Error != "" {
			i.err = errors.Join(i.err, fmt.Errorf("worker %d: %s", index, message.Error))
		}
	}
	peer := w.peer
	i.recalculateLocked()
	i.notifyLocked()
	i.mu.Unlock()
	if start {
		return peer.send(controlMessage{Kind: "start"})
	}
	return nil
}

func (i *Instance) workerFinishing(index int, epoch uint64) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	w := i.workers[index]
	return w.epoch == epoch && (i.stopping || w.stopping) && w.dataDone && w.mmdsDone
}

// workerStreamFinishing allows a validated stats Goodbye to arrive before the
// independent control reader applies a stopping instance's queued lifecycle
// acknowledgments. This only defers the stream-fault decision: worker exit,
// terminal control messages and the original instance deadline still govern
// completion. EOF and protocol errors remain faults before endpoint completion.
func (i *Instance) workerStreamFinishing(index int, epoch uint64, streamErr error) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	w := i.workers[index]
	return w.epoch == epoch && ((i.stopping || w.stopping) && w.dataDone && w.mmdsDone ||
		streamErr == nil && i.stopping)
}

// drainWorkers stops both endpoints independently, then ends workers only
// after both have drained. The instance shares one deadline across all workers.
func (i *Instance) drainWorkers() error {
	i.mu.Lock()
	deadline := i.deadline
	i.mu.Unlock()
	ctx, cancel := drainContext(deadline)
	defer cancel()
	for _, phase := range []string{"drain", "stop"} {
		i.mu.Lock()
		type recipient struct {
			index int
			epoch uint64
			peer  *controlPeer
		}
		peers := make([]recipient, 0, len(i.workers))
		for index, w := range i.workers {
			if w.alive && !w.stopping {
				peers = append(peers, recipient{index, w.epoch, w.peer})
			}
		}
		i.mu.Unlock()
		// Send independently: an unresponsive worker cannot delay another worker's
		// stop-accepting command or consume a fresh per-worker budget.
		type sendResult struct {
			recipient
			err error
		}
		sent := make(chan sendResult, len(peers))
		for _, target := range peers {
			go func() { sent <- sendResult{target, target.peer.send(controlMessage{Kind: phase, Deadline: deadline})} }()
		}
		for range peers {
			select {
			case result := <-sent:
				if result.err != nil && !i.joinSignaledWorker(ctx, result.index, result.epoch) {
					return errors.Join(ErrForcedClose, result.err)
				}
			case <-ctx.Done():
				return fmt.Errorf("proxy %s: %w", phase, errors.Join(ErrForcedClose, ctx.Err()))
			}
		}
		for {
			i.mu.Lock()
			complete := true
			for _, w := range i.workers {
				if w.alive && ((phase == "drain" && (!w.dataDone || !w.mmdsDone)) || phase == "stop") {
					complete = false
				}
			}
			changed := i.changed
			i.mu.Unlock()
			if complete {
				break
			}
			select {
			case <-changed:
			case <-ctx.Done():
				return fmt.Errorf("proxy %s: %w", phase, errors.Join(ErrForcedClose, ctx.Err()))
			}
		}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.err
}

// A worker may receive TERM between selection above and delivery of a command.
// A failed send is harmless only after its own stop announcement, complete
// endpoint/stopped acknowledgments, and actual process exit are all observed.
func (i *Instance) joinSignaledWorker(ctx context.Context, index int, epoch uint64) bool {
	for {
		i.mu.Lock()
		w, changed := i.workers[index], i.changed
		i.mu.Unlock()
		if w.epoch != epoch {
			return false
		}
		if !w.alive {
			return w.stopping && w.stopped
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return false
		}
	}
}

func (i *Instance) stopped() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.stopping
}

func (i *Instance) extensionCompatible() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, w := range i.workers {
		if !w.alive || !w.ready || !w.canShutdown {
			return false
		}
	}
	return true
}

func (i *Instance) remainingContext() (context.Context, context.CancelFunc) {
	i.mu.Lock()
	deadline := i.deadline
	i.mu.Unlock()
	return drainContext(deadline)
}

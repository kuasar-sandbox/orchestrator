package conductorext

import (
	"context"
	"errors"
	"sync/atomic"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type sandboxChange struct {
	kind      conductorextension.SandboxEventKind
	sandboxID string
	view      conductorextension.SandboxView
}

type buildChange struct {
	kind    conductorextension.BuildEventKind
	buildID string
	view    conductorextension.BuildView
	reason  string
}

type sandboxSource struct {
	store      *store.Store
	hub        *hub[sandboxChange]
	generation atomic.Uint64
}

type buildSource struct {
	store      *store.Store
	hub        *hub[buildChange]
	generation atomic.Uint64
}

// Host is the internal implementation of the public conductor Extension Host.
type Host struct {
	sandboxes *sandboxSource
	builds    *buildSource
	stats     conductorextension.StatsReader
}

// Observer is attached to the core before Extension.Start. Its methods project
// immutable values and perform only bounded, non-blocking publication.
type Observer struct {
	sandboxes *hub[sandboxChange]
	builds    *hub[buildChange]
}

// New constructs object sources and their core observer. It starts no
// goroutines; Watch executes in the Extension-owned calling goroutine.
func New(storage *store.Store, stats conductorextension.StatsReader) (*Host, *Observer) {
	host, observer := newWithCapacity(storage, defaultQueueCapacity)
	host.stats = stats
	return host, observer
}

func newWithCapacity(storage *store.Store, capacity int) (*Host, *Observer) {
	sandboxHub := newHub[sandboxChange](capacity)
	buildHub := newHub[buildChange](capacity)
	return &Host{
		sandboxes: &sandboxSource{store: storage, hub: sandboxHub},
		builds:    &buildSource{store: storage, hub: buildHub},
	}, &Observer{sandboxes: sandboxHub, builds: buildHub}
}

func (h *Host) Sandboxes() conductorextension.SandboxSource { return h.sandboxes }
func (h *Host) Builds() conductorextension.BuildSource      { return h.builds }
func (h *Host) Stats() conductorextension.StatsReader       { return h.stats }

func (s *sandboxSource) Get(ctx context.Context, id string) (conductorextension.SandboxView, bool, error) {
	sandbox, err := s.store.Get(ctx, id)
	if err != nil || sandbox == nil {
		return conductorextension.SandboxView{}, false, err
	}
	return projectSandbox(sandbox), true, nil
}

func (s *buildSource) Get(ctx context.Context, id string) (conductorextension.BuildView, bool, error) {
	build, err := s.store.GetBuild(ctx, id)
	if err != nil || build == nil {
		return conductorextension.BuildView{}, false, err
	}
	return projectBuild(build), true, nil
}

func (s *sandboxSource) Watch(ctx context.Context, callback func(conductorextension.SandboxEvent) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if callback == nil {
		return errors.New("conductor extension: sandbox Watch callback is required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sub := s.hub.subscribe()
		generation := s.generation.Add(1)
		complete, err := s.snapshot(ctx, sub, generation, callback)
		if err != nil {
			s.hub.unsubscribe(sub)
			return err
		}
		if !complete {
			s.hub.unsubscribe(sub)
			continue
		}
		for {
			select {
			case <-sub.reset:
				s.hub.unsubscribe(sub)
				goto resync
			default:
			}
			select {
			case <-ctx.Done():
				s.hub.unsubscribe(sub)
				return ctx.Err()
			case <-sub.reset:
				s.hub.unsubscribe(sub)
				goto resync
			case change := <-sub.events:
				event := sandboxEvent(generation, change)
				if err := callback(event); err != nil {
					s.hub.unsubscribe(sub)
					return err
				}
			}
		}
	resync:
	}
}

func (s *sandboxSource) snapshot(ctx context.Context, sub *subscription[sandboxChange], generation uint64, callback func(conductorextension.SandboxEvent) error) (bool, error) {
	if err := callback(conductorextension.SandboxEvent{Generation: generation, Kind: conductorextension.SandboxSyncBegin}); err != nil {
		return false, err
	}
	err := s.store.RangeSandboxes(ctx, func(sandbox *types.Sandbox) error {
		select {
		case <-sub.reset:
			return errGenerationLost
		default:
		}
		view := projectSandbox(sandbox)
		return callback(conductorextension.SandboxEvent{
			Generation: generation, Kind: conductorextension.SandboxUpsert, SandboxID: view.ID, View: &view,
		})
	})
	if err == errGenerationLost {
		return false, nil
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, err
	}
	select {
	case <-sub.reset:
		return false, nil
	default:
	}
	if err := callback(conductorextension.SandboxEvent{Generation: generation, Kind: conductorextension.SandboxSyncEnd}); err != nil {
		return false, err
	}
	return true, nil
}

func (s *buildSource) Watch(ctx context.Context, callback func(conductorextension.BuildEvent) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if callback == nil {
		return errors.New("conductor extension: build Watch callback is required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		sub := s.hub.subscribe()
		generation := s.generation.Add(1)
		complete, err := s.snapshot(ctx, sub, generation, callback)
		if err != nil {
			s.hub.unsubscribe(sub)
			return err
		}
		if !complete {
			s.hub.unsubscribe(sub)
			continue
		}
		for {
			select {
			case <-sub.reset:
				s.hub.unsubscribe(sub)
				goto resync
			default:
			}
			select {
			case <-ctx.Done():
				s.hub.unsubscribe(sub)
				return ctx.Err()
			case <-sub.reset:
				s.hub.unsubscribe(sub)
				goto resync
			case change := <-sub.events:
				if err := callback(buildEvent(generation, change)); err != nil {
					s.hub.unsubscribe(sub)
					return err
				}
			}
		}
	resync:
	}
}

func (s *buildSource) snapshot(ctx context.Context, sub *subscription[buildChange], generation uint64, callback func(conductorextension.BuildEvent) error) (bool, error) {
	if err := callback(conductorextension.BuildEvent{Generation: generation, Kind: conductorextension.BuildSyncBegin}); err != nil {
		return false, err
	}
	err := s.store.RangeBuilds(ctx, func(build *types.Build) error {
		select {
		case <-sub.reset:
			return errGenerationLost
		default:
		}
		view := projectBuild(build)
		return callback(conductorextension.BuildEvent{
			Generation: generation, Kind: conductorextension.BuildUpsert, BuildID: view.BuildID, View: &view,
		})
	})
	if err == errGenerationLost {
		return false, nil
	}
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, err
	}
	select {
	case <-sub.reset:
		return false, nil
	default:
	}
	if err := callback(conductorextension.BuildEvent{Generation: generation, Kind: conductorextension.BuildSyncEnd}); err != nil {
		return false, err
	}
	return true, nil
}

type generationLostError struct{}

func (generationLostError) Error() string { return "extension watch generation lost" }

var errGenerationLost error = generationLostError{}

func sandboxEvent(generation uint64, change sandboxChange) conductorextension.SandboxEvent {
	view := cloneSandboxView(change.view)
	return conductorextension.SandboxEvent{
		Generation: generation, Kind: change.kind, SandboxID: change.sandboxID, View: &view,
	}
}

func buildEvent(generation uint64, change buildChange) conductorextension.BuildEvent {
	view := cloneBuildView(change.view)
	return conductorextension.BuildEvent{
		Generation: generation, Kind: change.kind, BuildID: change.buildID, View: &view, Reason: change.reason,
	}
}

func (o *Observer) SandboxUpsert(sandbox *types.Sandbox) {
	if o == nil || sandbox == nil {
		return
	}
	view := projectSandbox(sandbox)
	o.sandboxes.publish(sandboxChange{kind: conductorextension.SandboxUpsert, sandboxID: view.ID, view: view})
}

func (o *Observer) SandboxDelete(sandbox *types.Sandbox) {
	if o == nil || sandbox == nil {
		return
	}
	view := projectSandbox(sandbox)
	o.sandboxes.publish(sandboxChange{kind: conductorextension.SandboxDelete, sandboxID: view.ID, view: view})
}

func (o *Observer) BuildUpsert(build *types.Build) {
	if o == nil || build == nil {
		return
	}
	view := projectBuild(build)
	o.builds.publish(buildChange{kind: conductorextension.BuildUpsert, buildID: view.BuildID, view: view})
}

func (o *Observer) BuildRemove(build *types.Build) {
	if o == nil || build == nil {
		return
	}
	view := projectBuild(build)
	o.builds.publish(buildChange{kind: conductorextension.BuildRemove, buildID: view.BuildID, view: view, reason: view.Reason})
}

//go:build fdb && cgo

package fdbdriver

import (
	"context"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func (s *Store) routeScope(group string) fdb.Key { return s.key("routes", group) }
func (s *Store) routeKey(key metadata.RouteKey) fdb.Key {
	return mainKey(s.routeScope(key.Group), key.RouteKey)
}
func (s *Store) ReadRoute(ctx context.Context, key metadata.RouteKey) (out metadata.Versioned[metadata.SandboxRecord], err error) {
	if err = validateParts(key.Group, key.RouteKey); err != nil {
		return
	}
	err = s.read(ctx, func(tr fdb.Transaction) error {
		var e error
		out, e = readTyped[metadata.SandboxRecord](tr, s.routeKey(key))
		if e == nil && out.Exists {
			e = metadata.ValidateSandbox(out.Value)
			if out.Value.Group != key.Group || out.Value.RouteKey != key.RouteKey {
				return invalidRecord("route key mismatch")
			}
		}
		return e
	})
	return
}
func (s *Store) ReadResolution(ctx context.Context, key metadata.RouteKey) (out metadata.Resolution, err error) {
	if err = validateParts(key.Group, key.RouteKey); err != nil {
		return
	}
	err = s.read(ctx, func(tr fdb.Transaction) error {
		out = metadata.Resolution{}
		var err error
		out.Route, err = readTyped[metadata.SandboxRecord](tr, s.routeKey(key))
		if err != nil || !out.Route.Exists {
			return err
		}
		if err = metadata.ValidateSandbox(out.Route.Value); err != nil {
			return err
		}
		if out.Route.Value.Group != key.Group || out.Route.Value.RouteKey != key.RouteKey {
			return invalidRecord("route key mismatch")
		}
		if out.Route.Value.NodeID != "" {
			out.Node, err = readTyped[metadata.NodeProfileRecord](tr, s.profileKey(out.Route.Value.NodeID))
		}
		return err
	})
	return
}
func (s *Store) CompareAndPutRoute(ctx context.Context, key metadata.RouteKey, expected metadata.Version, value metadata.SandboxRecord) (metadata.Version, error) {
	if err := validateParts(key.Group, key.RouteKey); err != nil {
		return metadata.Version{}, err
	}
	if value.Group != key.Group || value.RouteKey != key.RouteKey {
		return metadata.Version{}, invalidRecord("route key mismatch")
	}
	if err := metadata.ValidateSandbox(value); err != nil {
		return metadata.Version{}, err
	}
	return s.mutate(ctx, func(m *mutation) (bool, error) {
		if _, err := m.expect(s.routeKey(key), expected); err != nil {
			return false, err
		}
		return true, putJSON(m.tr, s.routeKey(key), value)
	})
}
func (s *Store) DeleteRoute(ctx context.Context, key metadata.RouteKey, expected metadata.Version) error {
	if err := validateParts(key.Group, key.RouteKey); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		current, err := m.expect(s.routeKey(key), expected)
		if err != nil || !current.exists {
			return false, err
		}
		clearRecord(m.tr, s.routeKey(key))
		return true, nil
	})
	return err
}
func (s *Store) CommitIntentAndOwnership(ctx context.Context, expected metadata.Version, value metadata.SandboxRecord, ref metadata.NodeSandboxRef) (metadata.Version, error) {
	if err := metadata.ValidateSandbox(value); err != nil {
		return metadata.Version{}, err
	}
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return metadata.Version{}, err
	}
	if (value.State != metadata.StateReserved && value.State != metadata.StateReady && value.State != metadata.StatePausing) || value.NodeID == "" || ref != metadata.SandboxRef(value) {
		return metadata.Version{}, invalidRecord("intent ownership mismatch")
	}
	if err := validateParts(value.Group, value.RouteKey, value.NodeID, ref.NodeSandboxID); err != nil {
		return metadata.Version{}, err
	}
	return s.mutate(ctx, func(m *mutation) (bool, error) {
		if _, err := m.expect(s.routeKey(metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}), expected); err != nil {
			return false, err
		}
		if err := s.guardLive(m, value.NodeID); err != nil {
			return false, err
		}
		if err := s.ensureSandbox(m, value.NodeID, ref); err != nil {
			return false, err
		}
		return true, putJSON(m.tr, s.routeKey(metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}), value)
	})
}
func (s *Store) RollbackIntent(ctx context.Context, key metadata.RouteKey, expected metadata.Version, restore *metadata.SandboxRecord, dropCurrentOwner bool) (metadata.Version, error) {
	if err := validateParts(key.Group, key.RouteKey); err != nil {
		return metadata.Version{}, err
	}
	if restore != nil {
		copy := *restore
		restore = &copy
		if restore.Group != key.Group || restore.RouteKey != key.RouteKey {
			return metadata.Version{}, invalidRecord("rollback identity")
		}
		if err := metadata.ValidateSandbox(*restore); err != nil {
			return metadata.Version{}, err
		}
	}
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		raw, err := m.expect(s.routeKey(key), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.SandboxRecord](raw)
		if err != nil {
			return false, err
		}
		if !current.Exists || current.Value.State != metadata.StateReserved {
			return false, metadata.ErrConflict
		}
		if restore != nil {
			next := *restore
			if next.NextSandboxGeneration < current.Value.NextSandboxGeneration {
				next.NextSandboxGeneration = current.Value.NextSandboxGeneration
			}
			if next.NodeID != "" {
				if err = s.restoreSandboxOwner(m, next.NodeID, metadata.SandboxRef(next)); err != nil {
					return false, err
				}
			}
			if err = putJSON(m.tr, s.routeKey(key), next); err != nil {
				return false, err
			}
		} else {
			clearRecord(m.tr, s.routeKey(key))
		}
		if dropCurrentOwner && current.Value.NodeID != "" && (restore == nil || restore.NodeID != current.Value.NodeID || metadata.SandboxRef(*restore) != metadata.SandboxRef(current.Value)) {
			if _, err = s.clearMatchingSandbox(m, current.Value.NodeID, metadata.SandboxRef(current.Value)); err != nil {
				return false, err
			}
		}
		return true, nil
	})
	if restore == nil {
		return metadata.Version{}, err
	}
	return version, err
}
func (s *Store) DeleteRouteAndOwnership(ctx context.Context, key metadata.RouteKey, expected metadata.Version, nodeID string, ref metadata.NodeSandboxRef) error {
	if err := validateParts(key.Group, key.RouteKey, nodeID, ref.NodeSandboxID); err != nil {
		return err
	}
	if key.Group != ref.Group || key.RouteKey != ref.RouteKey {
		return invalidRecord("delete ownership scope")
	}
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return err
	}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		raw, err := m.expect(s.routeKey(key), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.SandboxRecord](raw)
		if err != nil {
			return false, err
		}
		deleted := current.Exists && current.Value.NodeID == nodeID && metadata.SandboxRef(current.Value) == ref
		if deleted {
			clearRecord(m.tr, s.routeKey(key))
		}
		cleared, err := s.clearMatchingSandbox(m, nodeID, ref)
		return deleted || cleared, err
	})
	return err
}
func (s *Store) ApplyRouteReport(ctx context.Context, registration metadata.Registration, ref metadata.NodeSandboxRef, event routesync.RouteEntry) (metadata.RouteReportResult, error) {
	nodeID := registration.NodeID
	if err := validateParts(nodeID, ref.Group, ref.RouteKey); err != nil {
		return metadata.RouteReportResult{}, err
	}
	now := time.Now().Unix()
	var out metadata.RouteReportResult
	version, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardRegistration(m, registration); err != nil {
			return false, err
		}
		current, err := getTyped[metadata.SandboxRecord](m, s.routeKey(metadata.RouteKey{Group: ref.Group, RouteKey: ref.RouteKey}))
		if err != nil {
			return false, err
		}
		out, err = metadata.MergeRouteReport(current, nodeID, ref, event, now)
		if err != nil || out.Disposition != metadata.ReportWritten {
			return false, err
		}
		return true, putJSON(m.tr, s.routeKey(metadata.RouteKey{Group: ref.Group, RouteKey: ref.RouteKey}), out.Record.Value)
	})
	if err != nil {
		return metadata.RouteReportResult{}, err
	}
	if out.Disposition == metadata.ReportWritten {
		out.Record.Version = version
	}
	return out, nil
}
func (s *Store) ListRoutes(ctx context.Context, group string, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.SandboxRecord]], error) {
	if err := validateParts(group); err != nil {
		return metadata.Page[metadata.Versioned[metadata.SandboxRecord]]{}, err
	}
	page, err := listTyped[metadata.SandboxRecord](ctx, s, s.routeScope(group), cursor, limit)
	if err != nil {
		return metadata.Page[metadata.Versioned[metadata.SandboxRecord]]{}, err
	}
	for _, item := range page.Items {
		if err = metadata.ValidateSandbox(item.Value); err != nil {
			return metadata.Page[metadata.Versioned[metadata.SandboxRecord]]{}, err
		}
		if item.Value.Group != group {
			return metadata.Page[metadata.Versioned[metadata.SandboxRecord]]{}, invalidRecord("route listing scope")
		}
	}
	return page, nil
}

func (s *Store) ReadAndObserveRoute(ctx context.Context, key metadata.RouteKey) (out metadata.Versioned[metadata.SandboxRecord], observation metadata.Invalidation, returnedErr error) {
	if err := validateParts(key.Group, key.RouteKey); err != nil {
		return out, nil, err
	}
	ctx, opRelease, err := s.operation(ctx)
	if err != nil {
		return out, nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			opRelease()
		}
		returnedErr = observationError(returnedErr)
	}()
	release, err := s.observationSlot(ctx)
	if err != nil {
		return out, nil, err
	}
	defer func() {
		if !transferred {
			release()
		}
	}()
	tr, cleanup, err := s.newTransaction(ctx)
	if err != nil {
		return out, nil, err
	}
	defer cleanup()
	physical := s.routeKey(key)
	for {
		if err = ctx.Err(); err != nil {
			return out, nil, err
		}
		current, err := readTyped[metadata.SandboxRecord](tr, physical)
		if err != nil {
			if retryNative(tr, err) {
				continue
			}
			return out, nil, unavailable(ctx, err)
		}
		if current.Exists {
			if err = metadata.ValidateSandbox(current.Value); err != nil {
				return out, nil, err
			}
		}
		if s.afterReadBeforeWatch != nil {
			s.afterReadBeforeWatch()
		}
		future := tr.Watch(physical)
		err = tr.Commit().Get()
		if err == nil {
			transferred = true
			return current, s.observeFutures(ctx, []fdb.FutureNil{future}, []func(){release}, opRelease), nil
		}
		cancelFutures([]fdb.FutureNil{future})
		if ctx.Err() != nil {
			return out, nil, ctx.Err()
		}
		if retryObservation(tr, err) {
			continue
		}
		return out, nil, err
	}
}

// ApplyRouteDelete is the stream-authorized delete/rollback boundary. The
// captured registration is checked in the same transaction as route and refs.
func (s *Store) ApplyRouteDelete(ctx context.Context, registration metadata.Registration, expected metadata.Version, ref metadata.NodeSandboxRef, restore *metadata.SandboxRecord) error {
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return err
	}
	if err := validateParts(registration.NodeID, ref.Group, ref.RouteKey, ref.NodeSandboxID); err != nil {
		return err
	}
	if restore != nil {
		copy := *restore
		restore = &copy
		if restore.Group != ref.Group || restore.RouteKey != ref.RouteKey {
			return invalidRecord("reported rollback scope")
		}
		if err := metadata.ValidateSandbox(*restore); err != nil {
			return err
		}
	}
	key := metadata.RouteKey{Group: ref.Group, RouteKey: ref.RouteKey}
	_, err := s.mutate(ctx, func(m *mutation) (bool, error) {
		if err := s.guardRegistration(m, registration); err != nil {
			return false, err
		}
		raw, err := m.expect(s.routeKey(key), expected)
		if err != nil {
			return false, err
		}
		current, err := typed[metadata.SandboxRecord](raw)
		if err != nil {
			return false, err
		}
		matched := current.Exists && current.Value.NodeID == registration.NodeID && metadata.SandboxRef(current.Value) == ref
		preserveOwner := false
		if matched {
			if restore == nil {
				clearRecord(m.tr, s.routeKey(key))
			} else {
				if current.Value.State != metadata.StateReserved {
					return false, metadata.ErrConflict
				}
				next := *restore
				if next.NextSandboxGeneration < current.Value.NextSandboxGeneration {
					next.NextSandboxGeneration = current.Value.NextSandboxGeneration
				}
				if next.NodeID != "" {
					if err = s.restoreSandboxOwner(m, next.NodeID, metadata.SandboxRef(next)); err != nil {
						return false, err
					}
					preserveOwner = next.NodeID == registration.NodeID && metadata.SandboxRef(next) == ref
				}
				if err = putJSON(m.tr, s.routeKey(key), next); err != nil {
					return false, err
				}
			}
		}
		cleared := false
		if !preserveOwner {
			cleared, err = s.clearMatchingSandbox(m, registration.NodeID, ref)
			if err != nil {
				return false, err
			}
		}
		return matched || cleared, nil
	})
	return err
}

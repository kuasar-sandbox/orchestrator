package metastore

import (
	"context"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
)

func (s *Store) routeScope(group string) records.Key { return s.key("routes", group) }
func (s *Store) routeKey(key metadata.RouteKey) records.Key {
	return mainKey(s.routeScope(key.Group), key.RouteKey)
}
func (s *Store) ReadRoute(ctx context.Context, key metadata.RouteKey) (out metadata.Versioned[metadata.SandboxRecord], err error) {
	if err = validateParts(key.Group, key.RouteKey); err != nil {
		return
	}
	err = s.read(ctx, func(tr *records.Tx) error {
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
	err = s.read(ctx, func(tr *records.Tx) error {
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
		return true, s.putRoute(m, value)
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
		if err := s.clearRoute(m, key); err != nil {
			return false, err
		}
		return true, nil
	})
	return err
}
func (s *Store) CommitIntentAndOwnership(ctx context.Context, expected metadata.Version, value metadata.SandboxRecord, ref metadata.NodeSandboxRef) (metadata.Version, error) {
	return s.commitSandboxIntent(ctx, expected, value, ref, nil)
}
func (s *Store) CommitGroupIntent(ctx context.Context, expected metadata.Version, value metadata.SandboxRecord, ref metadata.NodeSandboxRef, selected metadata.GroupAdmission) (metadata.Version, error) {
	return s.commitSandboxIntent(ctx, expected, value, ref, &selected)
}
func (s *Store) commitSandboxIntent(ctx context.Context, expected metadata.Version, value metadata.SandboxRecord, ref metadata.NodeSandboxRef, selected *metadata.GroupAdmission) (metadata.Version, error) {
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
		if selected != nil {
			if !value.ImportPending && value.TemplateID != value.CreateTemplateID {
				return false, invalidRecord("Group create template identity")
			}
			template, source := value.CreateTemplateID, value.TemplateSource
			// Registry has only an opaque KMT for import. The original template
			// is lineage history, never the actual restore runtime input.
			if value.ImportPending {
				template, source = "", ""
			}
			if err := s.guardGroupAdmission(m, *selected, value.Group, value.NodeID, value.APISecretFingerprint, value.GroupVersion, template, source); err != nil {
				return false, err
			}
		}
		if err := s.guardLive(m, value.NodeID); err != nil {
			return false, err
		}
		if err := s.ensureSandbox(m, value.NodeID, ref); err != nil {
			return false, err
		}
		return true, s.putRoute(m, value)
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
			if err = s.putRoute(m, next); err != nil {
				return false, err
			}
		} else {
			if err := s.clearRoute(m, key); err != nil {
				return false, err
			}
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

// RetireRejectedGroupIntent is only for a fresh, single delivery whose exact
// connection returned GroupVersionConflict before copying inputs. A new report,
// registration or intent invalidates the proof. Ambiguous older owners survive.
func (s *Store) RetireRejectedGroupIntent(ctx context.Context, registration metadata.Registration, expected metadata.Version, ref metadata.NodeSandboxRef, restore *metadata.SandboxRecord) error {
	if err := metadata.ValidateSandboxRef(ref); err != nil {
		return err
	}
	if ref.GroupVersion <= 0 || !expected.IsCommitted() {
		return invalidRecord("rejected Group intent")
	}
	key := metadata.RouteKey{Group: ref.Group, RouteKey: ref.RouteKey}
	if restore != nil {
		copy := *restore
		restore = &copy
		if restore.Group != ref.Group || restore.RouteKey != ref.RouteKey || restore.NodeSandboxID == ref.NodeSandboxID {
			return invalidRecord("rejected Group restore identity")
		}
		if err := metadata.ValidateSandbox(*restore); err != nil {
			return err
		}
	}
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
		if !current.Exists || current.Value.State != metadata.StateReserved || current.Value.InstanceID != "" || current.Value.StateVersion != 0 || current.Value.NodeID != registration.NodeID || metadata.SandboxRef(current.Value) != ref {
			return false, metadata.ErrConflict
		}
		owner, err := getTyped[metadata.NodeSandboxRef](m, s.sandboxOwnerKey(registration.NodeID, ref.NodeSandboxID))
		if err != nil {
			return false, err
		}
		if !owner.Exists || owner.Value != ref {
			return false, metadata.ErrConflict
		}
		if restore == nil {
			if err := s.clearRoute(m, key); err != nil {
				return false, err
			}
		} else {
			next := *restore
			if next.NextSandboxGeneration < current.Value.NextSandboxGeneration {
				next.NextSandboxGeneration = current.Value.NextSandboxGeneration
			}
			if next.NodeID != "" {
				if err := s.restoreSandboxOwner(m, next.NodeID, metadata.SandboxRef(next)); err != nil {
					return false, err
				}
			}
			if err := s.putRoute(m, next); err != nil {
				return false, err
			}
		}
		_, err = s.clearMatchingSandbox(m, registration.NodeID, ref)
		return true, err
	})
	return err
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
			if err := s.clearRoute(m, key); err != nil {
				return false, err
			}
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
		return true, s.putRoute(m, out.Record.Value)
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

func (s *Store) ReadAndObserveRoute(ctx context.Context, key metadata.RouteKey) (current metadata.Versioned[metadata.SandboxRecord], observation metadata.Invalidation, err error) {
	if err = validateParts(key.Group, key.RouteKey); err != nil {
		return
	}
	physical := s.routeKey(key)
	var expected []metadata.Version
	err = s.read(ctx, func(tr *records.Tx) error {
		current, err = readTyped[metadata.SandboxRecord](tr, physical)
		if err != nil {
			return err
		}
		if current.Exists {
			if err := metadata.ValidateSandbox(current.Value); err != nil {
				return err
			}
			if current.Value.Group != key.Group || current.Value.RouteKey != key.RouteKey {
				return invalidRecord("route key mismatch")
			}
			// The envelope and durable revision cell change atomically. Avoid
			// another lookup for an existing record; absent records still need
			// the retained revision cell to observe delete/recreate correctly.
			expected = []metadata.Version{current.Version}
			return nil
		}
		expected, err = tr.Revisions([]records.Key{physical})
		return err
	})
	if err != nil {
		return
	}
	observation, err = s.records.Observe(ctx, []records.Key{physical}, expected)
	return
}

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
				if err := s.clearRoute(m, key); err != nil {
					return false, err
				}
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
				if err = s.putRoute(m, next); err != nil {
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

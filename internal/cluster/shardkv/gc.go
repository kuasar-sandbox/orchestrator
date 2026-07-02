package shardkv

import (
	"context"
	"time"
)

type GCStats struct {
	Tombstones int
	Shards     int
}

func (s *Store) Compact(ctx context.Context, now time.Time) (GCStats, error) {
	if err := ctx.Err(); err != nil {
		return GCStats{}, err
	}
	if now.IsZero() {
		now = s.now()
	}
	s.mu.Lock()
	keys := make([]string, 0, len(s.shards))
	for key := range s.shards {
		keys = append(keys, key)
	}
	s.mu.Unlock()

	var stats GCStats
	for _, id := range keys {
		ls := s.localShardByID(id)
		if ls == nil {
			continue
		}
		view, err := s.resolver.ResolveShard(ls.namespace, ls.shard)
		if err != nil {
			continue
		}
		spec := namespaceSpec(view.Namespace, s.resolver)
		if spec.TombstoneRetention > 0 && s.allMembersReady(view) {
			sh := &Shard{store: s, namespace: ls.namespace, shard: ls.shard}
			for _, setName := range ls.recordSetNames() {
				rs, err := sh.RecordSet(setName)
				if err != nil {
					continue
				}
				localSet := ls.recordSetNoTouch(setName)
				if localSet == nil {
					continue
				}
				records, head, certificate, ok, err := rs.fetchCommittedSnapshot(ctx, view)
				if err != nil || !ok {
					continue
				}
				localSet.installNoTouch(records, head, certificate)
				rs.installSnapshotBestEffort(ctx, view, records, head, certificate, true)
				localSet.markReady(view.Label, head)
				stats.Tombstones += localSet.compactTombstones(now, spec.TombstoneRetention)
			}
		}
		if spec.Pinned || spec.IdleShardTTL <= 0 {
			continue
		}
		if ls.emptyAndIdle(now, spec.IdleShardTTL) {
			s.mu.Lock()
			if cur := s.shards[id]; cur == ls && ls.emptyAndIdle(now, spec.IdleShardTTL) {
				delete(s.shards, id)
				stats.Shards++
			}
			s.mu.Unlock()
		}
	}
	return stats, nil
}

func (s *Store) localShardByID(id string) *localShard {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shards[id]
}

func (s *Store) allMembersReady(view ShardView) bool {
	if s.ready == nil {
		return true
	}
	for _, member := range writeMembers(view) {
		if member == s.local {
			continue
		}
		if !s.ready.Ready(view.Label, member) {
			return false
		}
	}
	return true
}

type layoutResolver interface {
	NamespaceSpec(Namespace) (NamespaceSpec, bool)
}

func namespaceSpec(ns Namespace, resolver ShardResolver) NamespaceSpec {
	if r, ok := resolver.(layoutResolver); ok {
		if spec, found := r.NamespaceSpec(ns); found {
			return spec
		}
	}
	return NamespaceSpec{}
}

func (r *MaglevResolver) NamespaceSpec(ns Namespace) (NamespaceSpec, bool) {
	if r == nil {
		return NamespaceSpec{}, false
	}
	spec, ok := r.layout.Namespaces[ns]
	return spec, ok
}

func (s *localRecordSet) compactTombstones(now time.Time, retention time.Duration) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for key, rec := range s.records {
		if !rec.Deleted || rec.Meta.UpdatedAt.IsZero() || now.Sub(rec.Meta.UpdatedAt) < retention {
			continue
		}
		delete(s.records, key)
		delete(s.promised, key)
		n++
	}
	return n
}

func (s *localShard) emptyAndIdle(now time.Time, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastAccess.IsZero() || now.Sub(s.lastAccess) < ttl {
		return false
	}
	for _, rs := range s.recordSets {
		if !rs.emptyAndIdle(now, ttl) {
			return false
		}
	}
	return true
}

func (s *localRecordSet) emptyAndIdle(now time.Time, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records) == 0 && !s.lastAccess.IsZero() && now.Sub(s.lastAccess) >= ttl
}

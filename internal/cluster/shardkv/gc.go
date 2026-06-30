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
			records, ok, err := sh.fetchAllShardRecords(ctx, view)
			if err != nil || !ok {
				continue
			}
			for _, rec := range records {
				ls.repair(rec, now)
			}
			sh.repairRecordsBestEffort(ctx, view, records)
			ls.markReady(view.Label)
			stats.Tombstones += ls.compactTombstones(now, spec.TombstoneRetention)
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
	for _, member := range shardMembers(view) {
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

func (s *localShard) compactTombstones(now time.Time, retention time.Duration) int {
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
	return len(s.records) == 0 && !s.lastAccess.IsZero() && now.Sub(s.lastAccess) >= ttl
}

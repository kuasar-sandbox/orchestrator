package registry

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
)

func (s *Stores) putRouteSandboxShard(ctx context.Context, r *SandboxRecord) (uint64, error) {
	if r == nil || r.Group == "" || r.RouteKey == "" {
		return 0, nil
	}
	sh, err := s.routeLinkRecordSet(r.Group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return 0, err
	}
	value, err := clusterstate.EncodeShardValue(r)
	if err != nil {
		return 0, err
	}
	rec, err := shardUpsertReturn(ctx, sh, clusterstate.RouteSandboxRecordKey(r.RouteKey), value)
	if err != nil {
		return 0, err
	}
	return rec.Meta.Rev, nil
}

func (s *Stores) casRouteSandboxShard(ctx context.Context, r *SandboxRecord, expectRev uint64) (uint64, bool, error) {
	if r == nil || r.Group == "" || r.RouteKey == "" {
		return 0, false, nil
	}
	sh, err := s.routeLinkRecordSet(r.Group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return 0, false, err
	}
	value, err := clusterstate.EncodeShardValue(r)
	if err != nil {
		return 0, false, err
	}
	rec, ok, err := sh.CAS(ctx, clusterstate.RouteSandboxRecordKey(r.RouteKey), expectRev, value)
	if err != nil || !ok {
		return 0, ok, err
	}
	return rec.Meta.Rev, true, nil
}

func (s *Stores) getRouteSandboxShard(ctx context.Context, group, routeKey string) (*SandboxRecord, uint64, bool, error) {
	if group == "" || routeKey == "" {
		return nil, 0, false, nil
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return nil, 0, false, err
	}
	rec, found, err := sh.Get(ctx, clusterstate.RouteSandboxRecordKey(routeKey))
	if err != nil || !found {
		return nil, 0, found, err
	}
	out, err := clusterstate.DecodeShardValue[SandboxRecord](rec.Value)
	if err != nil {
		return nil, 0, false, err
	}
	if out.LastActive == 0 {
		out.LastActive = rec.Meta.UpdatedAt.Unix()
	}
	return &out, rec.Meta.Rev, true, nil
}

func (s *Stores) deleteRouteSandboxShard(ctx context.Context, group, routeKey string) error {
	if group == "" || routeKey == "" {
		return nil
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.RouteSandboxRecordKey(routeKey))
}

func (s *Stores) deleteRouteSandboxShardIfRevision(ctx context.Context, group, routeKey string, expectRev uint64) (bool, error) {
	if group == "" || routeKey == "" {
		return false, nil
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return false, err
	}
	_, ok, err := sh.Delete(ctx, clusterstate.RouteSandboxRecordKey(routeKey), expectRev)
	return ok, err
}

func (s *Stores) putRouteBuildShard(ctx context.Context, b *BuildRecord) (uint64, error) {
	if b == nil || b.Group == "" || b.BuildID == "" {
		return 0, nil
	}
	sh, err := s.routeLinkRecordSet(b.Group, clusterstate.RecordSetRouteBuild)
	if err != nil {
		return 0, err
	}
	value, err := clusterstate.EncodeShardValue(b)
	if err != nil {
		return 0, err
	}
	rec, err := shardUpsertReturn(ctx, sh, clusterstate.RouteBuildRecordKey(b.BuildID), value)
	if err != nil {
		return 0, err
	}
	return rec.Meta.Rev, nil
}

func (s *Stores) getRouteBuildShard(ctx context.Context, group, buildID string) (*BuildRecord, uint64, bool, error) {
	if group == "" || buildID == "" {
		return nil, 0, false, nil
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteBuild)
	if err != nil {
		return nil, 0, false, err
	}
	rec, found, err := sh.Get(ctx, clusterstate.RouteBuildRecordKey(buildID))
	if err != nil || !found {
		return nil, 0, found, err
	}
	out, err := clusterstate.DecodeShardValue[BuildRecord](rec.Value)
	if err != nil {
		return nil, 0, false, err
	}
	return &out, rec.Meta.Rev, true, nil
}

func (s *Stores) deleteRouteBuildShard(ctx context.Context, group, buildID string) error {
	if group == "" || buildID == "" {
		return nil
	}
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteBuild)
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.RouteBuildRecordKey(buildID))
}

func (s *Stores) rangeRouteSandboxesShard(ctx context.Context, group string, fn func(*SandboxRecord) error) error {
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteSandbox)
	if err != nil {
		return err
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		return err
	}
	var out []SandboxRecord
	for _, rec := range snap.Records {
		if _, ok := clusterstate.ParseRouteSandboxRecordKey(rec.Key); !ok {
			continue
		}
		route, err := clusterstate.DecodeShardValue[SandboxRecord](rec.Value)
		if err != nil {
			return err
		}
		out = append(out, route)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RouteKey < out[j].RouteKey })
	for i := range out {
		if err := fn(&out[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stores) rangeRouteBuildsShard(ctx context.Context, group string, fn func(*BuildRecord) error) error {
	sh, err := s.routeLinkRecordSet(group, clusterstate.RecordSetRouteBuild)
	if err != nil {
		return err
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		return err
	}
	var out []BuildRecord
	for _, rec := range snap.Records {
		if _, ok := clusterstate.ParseRouteBuildRecordKey(rec.Key); !ok {
			continue
		}
		build, err := clusterstate.DecodeShardValue[BuildRecord](rec.Value)
		if err != nil {
			return err
		}
		out = append(out, build)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BuildID < out[j].BuildID })
	for i := range out {
		if err := fn(&out[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stores) routeLinkShard(group string) (*shardkv.Shard, error) {
	store := s.ShardStore()
	if store == nil {
		return nil, errors.New("registry: shard store is not initialized")
	}
	return store.Shard(shardkv.Namespace(clusterstate.NamespaceRouteLink), clusterstate.RouteLinkShard(group))
}

func (s *Stores) routeLinkRecordSet(group string, recordSet shardkv.RecordSetName) (*shardkv.RecordSet, error) {
	sh, err := s.routeLinkShard(group)
	if err != nil {
		return nil, err
	}
	return sh.RecordSet(recordSet)
}

func shardUpsertReturn(ctx context.Context, sh *shardkv.RecordSet, key shardkv.RecordKey, value []byte) (shardkv.Record, error) {
	for attempt := 0; attempt < 5; attempt++ {
		cur, found, err := sh.Get(ctx, key)
		if err != nil {
			return shardkv.Record{}, err
		}
		expect := uint64(0)
		if found {
			expect = cur.Meta.Rev
		}
		rec, ok, err := sh.CAS(ctx, key, expect, value)
		if err != nil {
			return shardkv.Record{}, err
		}
		if ok {
			return rec, nil
		}
	}
	return shardkv.Record{}, shardkv.ErrConflict
}

func routeWatchEvent(ev shardkv.WatchEvent) (WatchEvent, bool, error) {
	routeKey, ok := clusterstate.ParseRouteSandboxRecordKey(ev.Key)
	if !ok {
		return WatchEvent{}, false, nil
	}
	out := WatchEvent{Key: routeKey, Rev: int64(ev.Rev)}
	switch ev.Type {
	case shardkv.EventPut:
		route, err := clusterstate.DecodeShardValue[SandboxRecord](ev.Record.Value)
		if err != nil {
			return WatchEvent{}, false, err
		}
		raw, err := json.Marshal(route)
		if err != nil {
			return WatchEvent{}, false, err
		}
		out.Type = WatchEventPut
		out.Value = raw
		return out, true, nil
	case shardkv.EventDelete:
		out.Type = WatchEventDelete
		return out, true, nil
	default:
		return WatchEvent{}, false, nil
	}
}

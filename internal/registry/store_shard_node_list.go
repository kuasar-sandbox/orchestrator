package registry

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/shardkv"
)

func (s *Stores) putNodeListEntryShard(ctx context.Context, entry clusterstate.NodeListEntry) error {
	if entry.NodeID == "" {
		return nil
	}
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return err
	}
	value, err := clusterstate.EncodeShardValue(entry)
	if err != nil {
		return err
	}
	return shardUpsert(ctx, sh, clusterstate.NodeListRecordKey(entry.NodeID), value)
}

func (s *Stores) deleteNodeListEntryShard(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return nil
	}
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return err
	}
	return shardDeleteIfFound(ctx, sh, clusterstate.NodeListRecordKey(nodeID))
}

func (s *Stores) rangeNodeListShard(ctx context.Context, fn func(clusterstate.NodeListEntry) error) error {
	sh, err := s.nodeListRecordSet()
	if err != nil {
		return err
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		return err
	}
	out := make([]clusterstate.NodeListEntry, 0, len(snap.Records))
	for _, rec := range snap.Records {
		entry, err := clusterstate.DecodeShardValue[clusterstate.NodeListEntry](rec.Value)
		if err != nil {
			return err
		}
		if entry.Deleted {
			continue
		}
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	for _, entry := range out {
		if err := fn(entry); err != nil {
			return err
		}
	}
	return nil
}

func (s *Stores) nodeListShard() (*shardkv.Shard, error) {
	store := s.ShardStore()
	if store == nil {
		return nil, errors.New("registry: shard store is not initialized")
	}
	return store.Shard(shardkv.Namespace(clusterstate.NamespaceNodeList), clusterstate.NodeListShard)
}

func (s *Stores) nodeListRecordSet() (*shardkv.RecordSet, error) {
	sh, err := s.nodeListShard()
	if err != nil {
		return nil, err
	}
	return sh.RecordSet(clusterstate.RecordSetNodeListNodes)
}

func nodeListWatchEvent(ev shardkv.WatchEvent) (WatchEvent, bool, error) {
	nodeID := string(ev.Key)
	out := WatchEvent{Key: nodeID, Rev: int64(ev.Rev), Token: ev.Token}
	switch ev.Type {
	case shardkv.EventReset:
		out.Type = WatchEventReset
		return out, true, nil
	case shardkv.EventBookmark:
		out.Type = WatchEventBookmark
		return out, true, nil
	case shardkv.EventPut:
		if nodeID == "" {
			return WatchEvent{}, false, nil
		}
		entry, err := clusterstate.DecodeShardValue[clusterstate.NodeListEntry](ev.Record.Value)
		if err != nil {
			return WatchEvent{}, false, err
		}
		if entry.Deleted {
			out.Type = WatchEventDelete
			return out, true, nil
		}
		raw, err := json.Marshal(entry)
		if err != nil {
			return WatchEvent{}, false, err
		}
		out.Type = WatchEventPut
		out.Value = raw
		return out, true, nil
	case shardkv.EventDelete:
		if nodeID == "" {
			return WatchEvent{}, false, nil
		}
		out.Type = WatchEventDelete
		return out, true, nil
	default:
		return WatchEvent{}, false, nil
	}
}

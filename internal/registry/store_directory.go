package registry

import (
	"context"
	"encoding/json"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"reflect"
	"sort"
)

type WatchEventType int

const (
	WatchEventPut WatchEventType = iota + 1
	WatchEventDelete
	WatchEventReset
	WatchEventBookmark
)

type WatchEvent struct {
	Type  WatchEventType
	Key   string
	Value []byte
	Token string
}

func (s *Stores) RangeNodeList(ctx context.Context, fn func(clusterstate.NodeListEntry) error) error {
	view, err := s.directory.Load(ctx)
	if err != nil {
		return err
	}
	for _, entry := range view.Nodes {
		if err := fn(clusterstate.NodeListEntry(entry)); err != nil {
			return err
		}
	}
	return nil
}

// WatchNodeListToken emits a fresh bounded snapshot, then diffs after one-shot
// bucket invalidations. Tokens are bookmarks, never a retained event history.
func (s *Stores) WatchNodeListToken(ctx context.Context, token string) (<-chan WatchEvent, error) {
	if _, err := metadata.ParseDirectoryToken(token); err != nil {
		return nil, err
	}
	view, err := s.directory.Load(ctx)
	if err != nil {
		return nil, err
	}
	observation, err := s.directory.Observe(ctx, view.Token)
	if err != nil {
		return nil, err
	}
	out := make(chan WatchEvent, 64)
	go func() {
		defer close(out)
		send := func(e WatchEvent) bool {
			select {
			case out <- e:
				return true
			case <-ctx.Done():
				return false
			}
		}
		old := make(map[string]metadata.NodeListEntry)
		if !send(WatchEvent{Type: WatchEventReset}) {
			observation.Cancel()
			return
		}
		for {
			next := make(map[string]metadata.NodeListEntry, len(view.Nodes))
			for _, entry := range view.Nodes {
				next[entry.NodeID] = entry
			}
			keys := make([]string, 0, len(next)+len(old))
			for k := range next {
				keys = append(keys, k)
			}
			for k := range old {
				if _, ok := next[k]; !ok {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			for _, k := range keys {
				entry, present := next[k]
				if !present {
					if !send(WatchEvent{Type: WatchEventDelete, Key: k}) {
						observation.Cancel()
						return
					}
					continue
				}
				if prev, ok := old[k]; ok && reflect.DeepEqual(prev, entry) {
					continue
				}
				raw, err := json.Marshal(entry)
				if err != nil {
					observation.Cancel()
					return
				}
				if !send(WatchEvent{Type: WatchEventPut, Key: k, Value: raw}) {
					observation.Cancel()
					return
				}
			}
			if !send(WatchEvent{Type: WatchEventBookmark, Token: view.Token.String()}) {
				observation.Cancel()
				return
			}
			old = next
			select {
			case <-ctx.Done():
				observation.Cancel()
				return
			case <-observation.Done():
			}
			err := observation.Err()
			observation.Cancel()
			if err != nil {
				return
			}
			view, err = s.directory.Load(ctx)
			if err != nil {
				return
			}
			observation, err = s.directory.Observe(ctx, view.Token)
			if err != nil {
				return
			}
		}
	}()
	return out, nil
}

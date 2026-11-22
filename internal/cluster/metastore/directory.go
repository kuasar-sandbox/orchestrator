package metastore

import (
	"context"
	"errors"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	records "github.com/kuasar-sandbox/orchestrator/internal/storage/record"
	"sort"
)

func (s *Store) directoryKeys() []records.Key {
	keys := make([]records.Key, metadata.DirectoryBuckets)
	for i := range keys {
		keys[i] = s.catalogTokenKey(byte(i))
	}
	return keys
}
func (s *Store) directoryTokens(ctx context.Context) (out metadata.DirectoryToken, err error) {
	err = s.read(ctx, func(tr *records.Tx) error {
		values, err := tr.Revisions(s.directoryKeys())
		if err != nil {
			return err
		}
		copy(out[:], values)
		return nil
	})
	return
}

// Load assembles bounded pages and checks each immutable bucket's change token.
// It publishes a complete checked view, not a long-lived global MVCC snapshot.
// Observe closes immediately if any bucket changed after its checked token.
func (s *Store) Load(ctx context.Context) (metadata.DirectoryView, error) {
	tokens, err := s.directoryTokens(ctx)
	if err != nil {
		return metadata.DirectoryView{}, err
	}
	var buckets [metadata.DirectoryBuckets][]metadata.NodeListEntry
	var loaded [metadata.DirectoryBuckets]bool
	var checked metadata.DirectoryToken
	for attempt := 0; attempt < 8; attempt++ {
		for b := 0; b < metadata.DirectoryBuckets; b++ {
			if loaded[b] && checked[b] == tokens[b] {
				continue
			}
			buckets[b] = nil
			if !tokens[b].IsZero() {
				var cursor metadata.Cursor
				for {
					page, err := listTyped[metadata.NodeListEntry](ctx, s, s.catalogScope(byte(b)), cursor, 128)
					if err != nil {
						return metadata.DirectoryView{}, err
					}
					for _, v := range page.Items {
						if bucket(v.Value.NodeID) != byte(b) || !v.Version.IsCommitted() {
							return metadata.DirectoryView{}, invalidRecord("catalog bucket identity")
						}
						node := v.Value
						node.Version = v.Version
						buckets[b] = append(buckets[b], node)
					}
					if page.Next == "" {
						break
					}
					cursor = page.Next
				}
			}
			checked[b] = tokens[b]
			loaded[b] = true
		}
		next, err := s.directoryTokens(ctx)
		if err != nil {
			return metadata.DirectoryView{}, err
		}
		if next == checked {
			out := metadata.DirectoryView{Token: checked}
			for _, nodes := range buckets {
				out.Nodes = append(out.Nodes, nodes...)
			}
			sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].NodeID < out.Nodes[j].NodeID })
			return out, nil
		}
		tokens = next
	}
	return metadata.DirectoryView{}, &metadata.Unavailable{Cause: errors.New("catalog changed during bounded pagination")}
}

func (s *Store) Observe(ctx context.Context, expected metadata.DirectoryToken) (metadata.Invalidation, error) {
	for _, v := range expected {
		if !v.IsZero() && !v.IsCommitted() {
			return nil, invalidRecord("catalog token")
		}
	}
	return s.records.Observe(ctx, s.directoryKeys(), expected[:])
}

//go:build fdb && cgo

package fdbdriver

import (
	"context"
	"errors"
	"sort"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func decodeDirectoryStamp(raw []byte) (metadata.Version, error) {
	var v metadata.Version
	if raw == nil {
		return v, nil
	}
	if len(raw) != len(v) {
		return v, invalidRecord("catalog token length")
	}
	copy(v[:], raw)
	if !v.IsCommitted() {
		return metadata.Version{}, invalidRecord("catalog token version")
	}
	return v, nil
}
func (s *Store) readDirectoryTokens(tr fdb.Transaction) (out metadata.DirectoryToken, err error) {
	var futures [metadata.DirectoryBuckets]fdb.FutureByteSlice
	for i := range futures {
		futures[i] = tr.Get(s.catalogTokenKey(byte(i)))
	}
	for i, future := range futures {
		raw, err := future.Get()
		if err != nil {
			return out, err
		}
		out[i], err = decodeDirectoryStamp(raw)
		if err != nil {
			return out, err
		}
	}
	return out, nil
}
func (s *Store) directoryTokens(ctx context.Context) (out metadata.DirectoryToken, err error) {
	err = s.read(ctx, func(tr fdb.Transaction) error { var err error; out, err = s.readDirectoryTokens(tr); return err })
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
func (s *Store) Observe(ctx context.Context, expected metadata.DirectoryToken) (out metadata.Invalidation, returnedErr error) {
	for _, v := range expected {
		if !v.IsZero() && !v.IsCommitted() {
			return nil, invalidRecord("catalog token")
		}
	}
	ctx, opRelease, err := s.operation(ctx)
	if err != nil {
		return nil, err
	}
	transferred := false
	releases := make([]func(), 0, metadata.DirectoryBuckets)
	defer func() {
		if !transferred {
			for _, release := range releases {
				release()
			}
			opRelease()
		}
		returnedErr = observationError(returnedErr)
	}()
	for i := 0; i < metadata.DirectoryBuckets; i++ {
		release, err := s.owner.budget.TryAcquireCatalog()
		if err != nil {
			return nil, err
		}
		releases = append(releases, release)
	}
	tr, cleanup, err := s.newTransaction(ctx)
	if err != nil {
		return nil, err
	}
	defer cleanup()
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		current, err := s.readDirectoryTokens(tr)
		if err != nil {
			if retryNative(tr, err) {
				continue
			}
			return nil, unavailable(ctx, err)
		}
		if current != expected {
			for _, release := range releases {
				release()
			}
			transferred = true
			return s.observeFutures(ctx, nil, nil, opRelease), nil
		}
		futures := make([]fdb.FutureNil, 0, metadata.DirectoryBuckets)
		for i := 0; i < metadata.DirectoryBuckets; i++ {
			futures = append(futures, tr.Watch(s.catalogTokenKey(byte(i))))
		}
		err = tr.Commit().Get()
		if err == nil {
			transferred = true
			return s.observeFutures(ctx, futures, releases, opRelease), nil
		}
		cancelFutures(futures)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if retryObservation(tr, err) {
			continue
		}
		return nil, err
	}
}

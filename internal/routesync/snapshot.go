package routesync

import (
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
)

// reconcileSnapshot releases routes deleted while disconnected. An unchanged
// live route is applied in place so its active admission leases survive.
// Only new entries that need an occupied slot wait for the snapshot boundary.
func (s *Subscriber) reconcileSnapshot(ctx context.Context, reader io.Reader, up chan<- *Msg, ids []string, capacity int) error {
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		seen[id] = false
	}
	pending := make(map[string]*Msg)
	for {
		msg, err := ReadMsg(reader)
		if err != nil {
			return err
		}
		switch msg.Type {
		case TypeUpsert:
			if r := msg.Route; r != nil {
				sid := r.SandboxID
				if r.State == StateDead || r.State == StateDeleting {
					delete(seen, sid)
					delete(pending, sid)
				} else {
					if _, present := seen[sid]; !present && len(seen) >= capacity {
						pending[sid] = msg
						if len(pending) > capacity {
							return fmt.Errorf("routesync: snapshot exceeds route capacity %d", capacity)
						}
						continue
					}
					delete(pending, sid)
					seen[sid] = true
				}
			}
		case TypeDelete:
			delete(seen, msg.SID)
			delete(pending, msg.SID)
		case TypeBookmark:
			if !msg.FullSync {
				return fmt.Errorf("routesync: expected full snapshot bookmark")
			}
			for sid, present := range seen {
				if !present {
					s.sink.ApplyDelete(sid)
					delete(seen, sid)
				}
			}
			if len(seen)+len(pending) > capacity {
				return fmt.Errorf("routesync: snapshot exceeds route capacity %d", capacity)
			}
			for _, sid := range slices.Sorted(maps.Keys(pending)) {
				if err := s.apply(ctx, pending[sid], up); err != nil {
					return err
				}
			}
			return s.apply(ctx, msg, up)
		}
		if err := s.apply(ctx, msg, up); err != nil {
			return err
		}
	}
}

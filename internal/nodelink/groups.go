package nodelink

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// streamGroups subscribes before taking S. Events at or below S are already in
// the consistent snapshot; later gaps or overflow terminate this connection so
// the next original registration starts a new complete report.
func streamGroups(ctx context.Context, source routesync.GroupSource, out chan<- *routesync.Msg) error {
	events, unsubscribe := source.SubscribeGroups()
	defer unsubscribe()
	sequence, groups, err := source.GroupSnapshot(ctx)
	if err != nil {
		return err
	}
	syncID := rand.Text()
	send := func(kind string, report *routesync.GroupReport) error {
		msg := &routesync.Msg{Type: kind, Group: report}
		if err := routesync.ValidateMessage(msg); err != nil {
			return err
		}
		select {
		case out <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := send(routesync.TypeGroupBegin, &routesync.GroupReport{SyncID: syncID, Sequence: sequence, Total: len(groups)}); err != nil {
		return err
	}
	if err := batchGroupSnapshot(groups, func(offset int, batch []nodegroup.Support) error {
		return send(routesync.TypeGroupBatch, &routesync.GroupReport{SyncID: syncID, Sequence: sequence, Offset: offset, Groups: batch})
	}); err != nil {
		return err
	}
	if err := send(routesync.TypeGroupEnd, &routesync.GroupReport{SyncID: syncID, Sequence: sequence}); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-events:
			if !ok {
				return errors.New("node-link: Group subscriber overflow or commit uncertainty")
			}
			if event.Sequence <= sequence {
				continue
			}
			if event.Sequence != sequence+1 {
				return errors.New("node-link: Group sequence gap")
			}
			report := &routesync.GroupReport{Sequence: event.Sequence, Deleted: event.Deleted}
			kind := routesync.TypeGroupDelete
			if event.Support != nil {
				kind = routesync.TypeGroupUpsert
				report.Groups = []nodegroup.Support{*event.Support}
			}
			if err := send(kind, report); err != nil {
				return err
			}
			sequence = event.Sequence
		}
	}
}

// batchGroupSnapshot counts each record's exact JSON size once. An array adds
// two brackets and one comma between records; repeated prefix encoding is not
// needed to retain the existing row, entry and byte budgets.
func batchGroupSnapshot(groups []nodegroup.Support, send func(int, []nodegroup.Support) error) error {
	offset, entries, size := 0, 0, 2
	for i := range groups {
		raw, err := json.Marshal(groups[i])
		if err != nil {
			return err
		}
		nextSize := size + len(raw)
		if i > offset {
			nextSize++ // comma before this record
		}
		if i > offset && (i-offset == 128 || entries+len(groups[i].Entries) > 128 || nextSize > 256<<10) {
			if err := send(offset, groups[offset:i]); err != nil {
				return err
			}
			offset, entries, nextSize = i, 0, len(raw)+2
		}
		if len(groups[i].Entries) > 128 || nextSize > 256<<10 {
			return nodegroup.ErrTooLarge
		}
		entries += len(groups[i].Entries)
		size = nextSize
	}
	if offset < len(groups) {
		return send(offset, groups[offset:])
	}
	return nil
}

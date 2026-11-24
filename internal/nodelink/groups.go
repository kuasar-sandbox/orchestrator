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
	for offset := 0; offset < len(groups); {
		batch := []nodegroup.Support{}
		entries := 0
		for i := offset; i < len(groups) && len(batch) < 128; i++ {
			candidate := append(batch, groups[i])
			raw, err := json.Marshal(candidate)
			if err != nil {
				return err
			}
			if len(raw) > 256<<10 || entries+len(groups[i].Entries) > 128 {
				break
			}
			batch, entries = candidate, entries+len(groups[i].Entries)
		}
		if len(batch) == 0 {
			return nodegroup.ErrTooLarge
		}
		if err := send(routesync.TypeGroupBatch, &routesync.GroupReport{SyncID: syncID, Sequence: sequence, Offset: offset, Groups: batch}); err != nil {
			return err
		}
		offset += len(batch)
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

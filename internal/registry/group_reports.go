package registry

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type groupReportStream struct {
	syncID   string
	sequence int64
	complete bool
}

func (s *groupReportStream) apply(ctx context.Context, repo metadata.GroupSupportRepository, reg metadata.Registration, msg *routesync.Msg) error {
	p := msg.Group
	if p == nil {
		return errors.New("missing Group report")
	}
	switch msg.Type {
	case routesync.TypeGroupBegin:
		if s.syncID != "" || p.SyncID == "" || len(p.Groups) != 0 || p.Deleted != "" {
			return metadata.ErrGroupResync
		}
		if err := repo.BeginGroupSupport(ctx, reg, p.SyncID, p.Sequence, p.Total); err != nil {
			return err
		}
		s.syncID, s.sequence = p.SyncID, p.Sequence
	case routesync.TypeGroupBatch:
		if s.complete || s.syncID == "" || s.syncID != p.SyncID || s.sequence != p.Sequence || p.Deleted != "" {
			return metadata.ErrGroupResync
		}
		return repo.WriteGroupSupportBatch(ctx, reg, p.SyncID, p.Offset, p.Groups)
	case routesync.TypeGroupEnd:
		if s.complete || s.syncID == "" || s.syncID != p.SyncID || s.sequence != p.Sequence || len(p.Groups) != 0 || p.Deleted != "" {
			return metadata.ErrGroupResync
		}
		for {
			complete, err := repo.FinishGroupSupport(ctx, reg, p.SyncID)
			if err != nil {
				return err
			}
			if complete {
				s.complete = true
				break
			}
		}
	case routesync.TypeGroupUpsert:
		if !s.complete || p.SyncID != "" || len(p.Groups) != 1 || p.Deleted != "" {
			return metadata.ErrGroupResync
		}
		return repo.ApplyGroupSupport(ctx, reg, p.Sequence, &p.Groups[0], "")
	case routesync.TypeGroupDelete:
		if !s.complete || p.SyncID != "" || len(p.Groups) != 0 || p.Deleted == "" {
			return metadata.ErrGroupResync
		}
		return repo.ApplyGroupSupport(ctx, reg, p.Sequence, nil, p.Deleted)
	default:
		return metadata.ErrGroupResync
	}
	return nil
}

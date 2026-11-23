package orch

import (
	"context"
	"errors"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func (o *Orchestrator) GroupGet(ctx context.Context, key, group string) (nodegroup.Detail, error) {
	return o.st.GroupGet(ctx, key, group)
}

func (o *Orchestrator) GroupList(ctx context.Context, key, cursor string, limit int) (nodegroup.Page, error) {
	return o.st.GroupList(ctx, key, cursor, limit)
}

func (o *Orchestrator) GroupPut(ctx context.Context, key, group string, req nodegroup.PutRequest) (nodegroup.Result, error) {
	return o.putGroup(ctx, key, group, req, nil)
}

func (o *Orchestrator) putGroup(ctx context.Context, key, group string, req nodegroup.PutRequest, pair *nodegroup.KeyPut) (nodegroup.Result, error) {
	o.groupMu.Lock()
	defer o.groupMu.Unlock()
	result, _, err := o.st.GroupPut(ctx, key, group, req.ExpectedVersion, req.Context, pair)
	if err != nil {
		// A database error can follow an unknown commit. Resnapshot rather than
		// fabricating a missing event or treating the error as rollback proof.
		if errors.Is(err, store.ErrKeyPairConflict) {
			return result, nodegroup.ErrConflict
		}
		if nodegroup.HTTPStatus(err) == http.StatusInternalServerError {
			o.resetGroupSubscribersLocked()
		}
		return result, err
	}
	if result.Version != req.ExpectedVersion {
		support, err := o.st.GroupSupport(ctx, group)
		if err != nil {
			o.resetGroupSubscribersLocked()
			return result, err
		}
		o.publishGroupLocked(routesync.GroupEvent{Sequence: result.Version, Support: &support})
	}
	return result, nil
}

func (o *Orchestrator) GroupDelete(ctx context.Context, key, group string, expected int64) (nodegroup.Result, error) {
	o.groupMu.Lock()
	defer o.groupMu.Unlock()
	result, err := o.st.GroupDelete(ctx, key, group, expected)
	if err != nil {
		if nodegroup.HTTPStatus(err) == http.StatusInternalServerError {
			o.resetGroupSubscribersLocked()
		}
		return result, err
	}
	if result.Version != expected {
		o.publishGroupLocked(routesync.GroupEvent{Sequence: result.Version, Deleted: group})
	}
	return result, nil
}

func (o *Orchestrator) GroupSnapshot(ctx context.Context) (int64, []nodegroup.Support, error) {
	o.groupMu.Lock()
	defer o.groupMu.Unlock()
	return o.st.GroupSnapshot(ctx)
}

func (o *Orchestrator) SubscribeGroups() (<-chan routesync.GroupEvent, func()) {
	o.groupMu.Lock()
	defer o.groupMu.Unlock()
	if o.groupSubs == nil {
		o.groupSubs = make(map[uint64]chan routesync.GroupEvent)
	}
	o.groupSubSeq++
	id := o.groupSubSeq
	ch := make(chan routesync.GroupEvent, 256)
	o.groupSubs[id] = ch
	return ch, func() {
		o.groupMu.Lock()
		defer o.groupMu.Unlock()
		if current, ok := o.groupSubs[id]; ok {
			delete(o.groupSubs, id)
			close(current)
		}
	}
}

func (o *Orchestrator) publishGroupLocked(event routesync.GroupEvent) {
	for id, ch := range o.groupSubs {
		select {
		case ch <- event:
		default:
			delete(o.groupSubs, id)
			close(ch) // reconnect replaces the catalog after a bounded-buffer overflow
		}
	}
}

func (o *Orchestrator) resetGroupSubscribersLocked() {
	for id, ch := range o.groupSubs {
		delete(o.groupSubs, id)
		close(ch)
	}
}

func (o *Orchestrator) handleGroupCommand(ctx context.Context, cmd *routesync.Command) *routesync.CmdAck {
	input := cmd.Group
	if input == nil {
		return groupCommandError(cmd, nodegroup.ErrInvalid)
	}
	ack := accept(cmd)
	var err error
	switch cmd.Kind {
	case routesync.CmdGroupGet:
		if input.Context != nil || input.KeyPut != nil || input.ExpectedVersion != nil {
			return groupCommandError(cmd, nodegroup.ErrInvalid)
		}
		result, e := o.GroupGet(ctx, input.APIKey, input.Group)
		ack.GroupDetail, err = &result, e
	case routesync.CmdGroupList:
		if input.Context != nil || input.KeyPut != nil || input.ExpectedVersion != nil || input.Group != "" {
			return groupCommandError(cmd, nodegroup.ErrInvalid)
		}
		result, e := o.GroupList(ctx, input.APIKey, input.Cursor, input.Limit)
		ack.GroupPage, err = &result, e
	case routesync.CmdGroupPut:
		if input.ExpectedVersion == nil || input.Context == nil {
			return groupCommandError(cmd, nodegroup.ErrInvalid)
		}
		result, e := o.putGroup(ctx, input.APIKey, input.Group, nodegroup.PutRequest{ExpectedVersion: *input.ExpectedVersion, Context: *input.Context}, input.KeyPut)
		ack.GroupResult, err = &result, e
	case routesync.CmdGroupDelete:
		if input.ExpectedVersion == nil || input.Context != nil || input.KeyPut != nil {
			return groupCommandError(cmd, nodegroup.ErrInvalid)
		}
		result, e := o.GroupDelete(ctx, input.APIKey, input.Group, *input.ExpectedVersion)
		ack.GroupResult, err = &result, e
	default:
		err = nodegroup.ErrInvalid
	}
	if err != nil {
		return groupCommandError(cmd, err)
	}
	return ack
}

func groupCommandError(cmd *routesync.Command, err error) *routesync.CmdAck {
	ack := &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckRejected, HTTPStatus: nodegroup.HTTPStatus(err), Reason: err.Error()}
	if ack.HTTPStatus == http.StatusInternalServerError {
		ack.Reason = "Group operation failed"
	}
	var conflict *nodegroup.VersionError
	if errors.As(err, &conflict) {
		version := conflict.Version
		ack.CurrentGroupVersion = &version
	}
	return ack
}

package session

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type HolderDispatchRPC interface {
	AdmitAndDispatchAt(context.Context, string, DispatchCommand) (DispatchReply, error)
}

type DirectoryDispatcher struct {
	directory *Directory
	client    HolderDispatchRPC
}

func NewDirectoryDispatcher(directory *Directory, client HolderDispatchRPC) (*DirectoryDispatcher, error) {
	if directory == nil || client == nil {
		return nil, errors.New("session: Directory and Holder dispatch client are required")
	}
	return &DirectoryDispatcher{directory: directory, client: client}, nil
}

func (d *DirectoryDispatcher) AdmitAndDispatch(ctx context.Context, command DispatchCommand) (DispatchReply, error) {
	record, found := d.directory.LookupRecord(command.NodeID)
	if !found || !record.Available || record.Conflict {
		return d.classifyUnavailable(ctx, command, "node has no current Session Holder")
	}
	entry := record.Entry
	if entry.NodeEpoch > command.NodeEpoch {
		return d.classifyUnavailable(ctx, command, "selected NodeEpoch is no longer current")
	}
	if entry.NodeEpoch < command.NodeEpoch {
		return DispatchReply{Outcome: cluster.DispatchSessionMoved, Reason: "Session Directory is behind selected NodeEpoch"}, nil
	}
	command.SessionSeq = entry.SessionSeq
	reply, err := d.client.AdmitAndDispatchAt(ctx, entry.HolderMemberID, command)
	if errors.Is(err, ErrStaleSession) || errors.Is(err, ErrSessionUnavailable) {
		return d.classifyUnavailable(ctx, command, err.Error())
	}
	return reply, err
}

func (d *DirectoryDispatcher) classifyUnavailable(
	ctx context.Context,
	command DispatchCommand,
	reason string,
) (DispatchReply, error) {
	fenced, err := d.directory.NodeEpochPermanentlyFenced(ctx, command.NodeID, command.NodeEpoch)
	if err == nil && fenced {
		return DispatchReply{Outcome: cluster.DispatchDefinitiveReject, Reason: "selected NodeEpoch is permanently fenced"}, nil
	}
	return DispatchReply{Outcome: cluster.DispatchSessionMoved, Reason: reason}, nil
}

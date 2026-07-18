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
	entry, found := d.directory.Lookup(command.NodeID)
	if !found {
		return DispatchReply{Outcome: cluster.DispatchSessionMoved, Reason: "node has no current Session Holder"}, nil
	}
	if entry.NodeEpoch > command.NodeEpoch {
		return DispatchReply{Outcome: cluster.DispatchDefinitiveReject, Reason: "selected NodeEpoch is permanently fenced"}, nil
	}
	if entry.NodeEpoch < command.NodeEpoch {
		return DispatchReply{Outcome: cluster.DispatchSessionMoved, Reason: "Session Directory is behind selected NodeEpoch"}, nil
	}
	command.SessionSeq = entry.SessionSeq
	reply, err := d.client.AdmitAndDispatchAt(ctx, entry.HolderMemberID, command)
	if errors.Is(err, ErrStaleSession) || errors.Is(err, ErrSessionUnavailable) {
		return DispatchReply{Outcome: cluster.DispatchSessionMoved, Reason: err.Error()}, nil
	}
	return reply, err
}

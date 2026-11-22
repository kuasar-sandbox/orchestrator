package registry

import (
	"context"
	"errors"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

func nodeProfile(n *NodeRecord) metadata.NodeProfileRecord {
	state := metadata.NodeLive
	if n.Draining {
		state = metadata.NodeDrained
	}
	return metadata.NodeProfileRecord{State: state,
		NodeID:                    n.NodeID,
		Labels:                    n.Labels,
		Capacity:                  n.Capacity,
		BuildRegistrationCapacity: n.BuildRegistrationCapacity,
		BuildExecutionCapacity:    n.BuildExecutionCapacity,
		APIEndpoint:               n.APIEndpoint,
		DataEndpoint:              n.DataEndpoint,
		RuntimeDigest:             n.RuntimeDigest,
		Zone:                      n.Zone,
		Allocated:                 n.Allocated,
		Pool:                      n.Pool,
		BuildRegistrationUsage:    n.BuildRegistrationUsage,
		BuildExecutionUsage:       n.BuildExecutionUsage,
		Counts:                    n.Counts,
		Draining:                  n.Draining,
		LastHeartbeatUnix:         n.LastHeartbeatUnix,
		ResumeToken:               n.ResumeToken,
		LinkOwner:                 n.LinkOwner,
	}
}
func nodeRecord(v metadata.NodeSnapshot) *NodeRecord {
	return &NodeRecord{Version: v.Version, Registration: v.Registration, Reaping: v.Reaping,
		NodeID:                    v.Value.NodeID,
		Labels:                    v.Value.Labels,
		Capacity:                  v.Value.Capacity,
		BuildRegistrationCapacity: v.Value.BuildRegistrationCapacity,
		BuildExecutionCapacity:    v.Value.BuildExecutionCapacity,
		APIEndpoint:               v.Value.APIEndpoint,
		DataEndpoint:              v.Value.DataEndpoint,
		RuntimeDigest:             v.Value.RuntimeDigest,
		Zone:                      v.Value.Zone,
		Allocated:                 v.Value.Allocated,
		Pool:                      v.Value.Pool,
		BuildRegistrationUsage:    v.Value.BuildRegistrationUsage,
		BuildExecutionUsage:       v.Value.BuildExecutionUsage,
		Counts:                    v.Value.Counts,
		Draining:                  v.Value.Draining,
		LastHeartbeatUnix:         v.Value.LastHeartbeatUnix,
		ResumeToken:               v.Value.ResumeToken,
		LinkOwner:                 v.Value.LinkOwner,
	}
}
func (s *Stores) GetNodeProfile(ctx context.Context, id string) (*NodeRecord, bool, error) {
	v, err := s.nodes.ReadNodeProfile(ctx, id)
	if err != nil || !v.Exists {
		return nil, v.Exists, err
	}
	return nodeRecord(v), true, nil
}
func (s *Stores) GetNode(ctx context.Context, id string) (*NodeRecord, bool, error) {
	n, found, err := s.GetNodeProfile(ctx, id)
	if err != nil || !found {
		return n, found, err
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := s.ownership.ListSandboxOwners(ctx, id, cursor, 256)
		if err != nil {
			return nil, false, err
		}
		for _, v := range page.Items {
			if err := metadata.ValidateSandboxRef(v.Value); err != nil {
				return nil, false, err
			}
			n.Sandboxes = append(n.Sandboxes, clusterstate.NodeSandboxRef(v.Value))
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := s.ownership.ListBuildOwners(ctx, id, cursor, 256)
		if err != nil {
			return nil, false, err
		}
		for _, v := range page.Items {
			n.Builds = append(n.Builds, clusterstate.NodeBuildRef(v.Value))
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	for cursor := metadata.Cursor(""); ; {
		page, err := s.keyPairs.ListKeyPairs(ctx, id, cursor, 256)
		if err != nil {
			return nil, false, err
		}
		for _, v := range page.Items {
			n.KeyPairs = append(n.KeyPairs, clusterstate.NodeKeyPair(v.Value))
		}
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	return n, true, nil
}

// PutNode registers a profile. Its fresh registration fence is returned on n;
// heartbeats must instead carry the original stream's registration explicitly.
func (s *Stores) PutNode(ctx context.Context, n *NodeRecord) error {
	if n == nil || n.NodeID == "" {
		return errors.New("registry: node ID is required")
	}
	expected := n.Version
	if expected.IsZero() {
		current, err := s.nodes.ReadNodeProfile(ctx, n.NodeID)
		if err != nil {
			return err
		}
		expected = current.Version
	}
	next := nodeProfile(n)
	if next.LinkOwner == "" {
		next.LinkOwner = s.WriterID()
	}
	registered, err := s.nodes.Register(ctx, expected, next)
	if err != nil {
		return err
	}
	n.Version = registered.Version
	n.Registration = registered.Registration
	n.Reaping = false
	n.LinkOwner = next.LinkOwner
	// Child ownership is never copied from a caller's aggregate snapshot.
	return nil
}

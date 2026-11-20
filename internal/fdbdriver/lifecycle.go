//go:build fdb && cgo

package fdbdriver

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
)

type lifecycle struct {
	NodeID    string           `json:"node_id"`
	LinkOwner string           `json:"link_owner"`
	Fence     metadata.Version `json:"fence"`
	Reaping   bool             `json:"reaping"`
}

func (s *Store) profileKey(nodeID string) fdb.Key   { return mainKey(s.key("node-profile"), nodeID) }
func (s *Store) lifecycleKey(nodeID string) fdb.Key { return mainKey(s.key("node-lifecycle"), nodeID) }
func (s *Store) reapingKey(nodeID string) fdb.Key   { return mainKey(s.key("node-reaping"), nodeID) }
func lifeFence(value metadata.Versioned[lifecycle]) metadata.Version {
	if value.Value.Fence.IsZero() {
		return value.Version
	}
	return value.Value.Fence
}
func (s *Store) guardLive(m *mutation, nodeID string) error {
	current, err := getTyped[lifecycle](m, s.lifecycleKey(nodeID))
	if err != nil {
		return err
	}
	if !current.Exists || current.Value.NodeID != nodeID || current.Value.Reaping || !lifeFence(current).IsCommitted() {
		return metadata.ErrConflict
	}
	return nil
}

func (s *Store) guardRegistration(m *mutation, registration metadata.Registration) error {
	if registration.NodeID == "" || registration.LinkOwner == "" || !registration.Fence.IsCommitted() {
		return metadata.ErrConflict
	}
	current, err := getTyped[lifecycle](m, s.lifecycleKey(registration.NodeID))
	if err != nil {
		return err
	}
	if !current.Exists || current.Value.Reaping || current.Value.NodeID != registration.NodeID || current.Value.LinkOwner != registration.LinkOwner || lifeFence(current) != registration.Fence {
		return metadata.ErrConflict
	}
	return nil
}

// Rollback restores the prior route even when its old node is gone. Ownership
// is repaired only while that node's stable lifecycle is live; cleanup must
// never gain a new reference after a reaping claim.
func (s *Store) restoreSandboxOwner(m *mutation, nodeID string, ref metadata.NodeSandboxRef) error {
	current, err := getTyped[lifecycle](m, s.lifecycleKey(nodeID))
	if err != nil {
		return err
	}
	if !current.Exists || current.Value.Reaping {
		return nil
	}
	if current.Value.NodeID != nodeID || !lifeFence(current).IsCommitted() {
		return invalidRecord("restored node lifecycle")
	}
	return s.ensureSandbox(m, nodeID, ref)
}

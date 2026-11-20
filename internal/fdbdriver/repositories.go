//go:build fdb && cgo

package fdbdriver

import "github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"

func (s *Store) Repositories() metadata.Repositories {
	return metadata.Repositories{Routes: s, Builds: s, Nodes: s, Ownership: s, KeyPairs: s, Directory: s, Imports: s, Observations: s}
}

var (
	_ metadata.RouteRepository        = (*Store)(nil)
	_ metadata.BuildRepository        = (*Store)(nil)
	_ metadata.NodeRepository         = (*Store)(nil)
	_ metadata.OwnershipRepository    = (*Store)(nil)
	_ metadata.KeyPairRepository      = (*Store)(nil)
	_ metadata.NodeDirectory          = (*Store)(nil)
	_ metadata.ImportSourceRepository = (*Store)(nil)
	_ metadata.ObservationAdmission   = (*Store)(nil)
)

package registry

import "time"

// Manifest-key leases are written to node_link by shuffle-sharding selector
// patches and refreshed to connected nodes by heartbeat maintenance.

const (
	keyLeaseTTL    = 3 * time.Hour // lease lifetime pushed to nodes
	keyRenewBefore = time.Hour     // refresh node leases before they reach expiry
	keyAckTimeout  = 5 * time.Second
)

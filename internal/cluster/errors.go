package cluster

import "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster/shardkv"

var (
	ErrConflict = shardkv.ErrConflict
	ErrQuorum   = shardkv.ErrQuorum
)

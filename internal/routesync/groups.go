package routesync

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

const GroupSupportVersion = 1

const (
	CmdGroupGet               = "group_get"
	CmdGroupList              = "group_list"
	CmdGroupPut               = "group_put"
	CmdGroupDelete            = "group_delete"
	TypeGroupBegin            = "group_begin"
	TypeGroupBatch            = "group_batch"
	TypeGroupEnd              = "group_end"
	TypeGroupUpsert           = "group_upsert"
	TypeGroupDelete           = "group_delete"
	ErrorGroupVersionConflict = "group_version_conflict"
)

// GroupCommand is a trusted maintenance request. The API key is still fully
// authenticated by the Node; only this transport may carry an atomic key_put.
type GroupCommand struct {
	Group           string             `json:"group,omitempty"`
	APIKey          string             `json:"api_key"`
	ExpectedVersion *int64             `json:"expected_version,omitempty"`
	Context         *nodegroup.Context `json:"context,omitempty"`
	KeyPut          *nodegroup.KeyPut  `json:"key_put,omitempty"`
	Cursor          string             `json:"cursor,omitempty"`
	Limit           int                `json:"limit,omitempty"`
}

type GroupReport struct {
	SyncID   string              `json:"sync_id,omitempty"`
	Sequence int64               `json:"sequence"`
	Total    int                 `json:"total,omitempty"`
	Offset   int                 `json:"offset,omitempty"`
	Groups   []nodegroup.Support `json:"groups,omitempty"`
	Deleted  string              `json:"deleted,omitempty"`
}

type GroupEvent struct {
	Sequence int64
	Support  *nodegroup.Support
	Deleted  string
}

type GroupSource interface {
	GroupSnapshot(context.Context) (int64, []nodegroup.Support, error)
	SubscribeGroups() (<-chan GroupEvent, func())
}

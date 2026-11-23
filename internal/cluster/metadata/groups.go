package metadata

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

var ErrGroupResync = errors.New("Group support stream requires a complete snapshot")

const (
	MaxGroupSupportBatchRows    = 128
	MaxGroupSupportBatchBytes   = 256 << 10
	MaxGroupSupportBatchEntries = 128
)

// GroupPublication changes only at full-snapshot boundaries. Selection pins
// this stable record rather than the cursor updated by each report.
type GroupPublication struct {
	Registration     Registration `json:"registration"`
	SyncID           string       `json:"sync_id"`
	SnapshotSequence int64        `json:"snapshot_sequence"`
	Published        bool         `json:"published"`
}

type GroupSupportEntry struct {
	AuthRef string            `json:"auth_ref"`
	Routing nodegroup.Routing `json:"routing"`
}

// GroupSupportRecord omits secret material and full Node context. Authentication
// roots have independent references so deleting support cannot orphan an object.
type GroupSupportRecord struct {
	Registration   Registration              `json:"registration"`
	SyncID         string                    `json:"sync_id"`
	Group          string                    `json:"group"`
	GroupVersion   int64                     `json:"group_version"`
	DefaultContext *nodegroup.DefaultContext `json:"default_context,omitempty"`
	Entries        []GroupSupportEntry       `json:"entries"`
}

type GroupSupportSelection struct {
	Support     Versioned[GroupSupportRecord]
	Publication Versioned[GroupPublication]
	Node        NodeSnapshot
}

// GroupAdmission is the selected support observation to compare atomically with
// the intent write. Heartbeat profile and report cursor versions are excluded.
type GroupAdmission struct {
	Group               string
	GroupVersion        int64
	AuthRef             string
	Registration        Registration
	SupportVersion      Version
	PublicationVersion  Version
	Placement           placement.Request
	ZoneAdmitMax        string
	TargetRuntimeDigest string
	BuildResources      *routesync.BuildResources
}

type GroupAuth struct {
	Group     string `json:"group"`
	AuthRef   string `json:"auth_ref"`
	APISecret string `json:"api_secret"`
}

// GroupAuthReference identifies one material-retaining business fact. Support
// uses a NodeID; sandbox/build use their durable object identity. It is not a
// bearer capability and cannot replace full APIKey verification.
type GroupAuthReference struct {
	Group   string `json:"group"`
	AuthRef string `json:"auth_ref"`
	Kind    string `json:"kind"`
	ID      string `json:"id"`
}

type GroupSupportRepository interface {
	BeginGroupSupport(context.Context, Registration, string, int64, int) error
	WriteGroupSupportBatch(context.Context, Registration, string, int, []nodegroup.Support) error
	FinishGroupSupport(context.Context, Registration, string) (bool, error)
	ApplyGroupSupport(context.Context, Registration, int64, *nodegroup.Support, string) error
	ReadGroupSupport(context.Context, string, string) (GroupSupportSelection, error)
	ListGroupSupport(context.Context, string, Cursor, int) (Page[GroupSupportSelection], error)
	ReadGroupAuth(context.Context, string, string) (Versioned[GroupAuth], error)
	ListGroupAuth(context.Context, string, Cursor, int) (Page[Versioned[GroupAuth]], error)
	CleanupGroupSupport(context.Context, ReapClaim) (bool, error)
}

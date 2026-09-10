package configsock

import (
	"errors"
	"slices"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ArtifactPrepareSchemaVersion 4 binds the Build-only image Bundle publication
// preflight into task-local source preparation. Version 3 added portable
// allocatable/deflate resource defaults and an explicit Build-only source-image
// config read. Version 2
// replaced the v1 Snapshot-only wire with typed E/S sources, durable launch
// mode, a selected prepared source, and bounded network/disk topology summaries.
// Old runners fail closed before the secret-bearing provider call.
const ArtifactPrepareSchemaVersion = 4

// SandboxTaskRequest identifies one exact assigned sandbox-runner incarnation.
type SandboxTaskRequest struct {
	SandboxID string `json:"sandbox_id"`
	RunID     string `json:"run_id"`
	Version   int    `json:"version,omitempty"`
}

// ArtifactPrepareSpec is the complete non-secret resolution request for one
// tenant-bound runner. RootSourceKind and LaunchMode are durable lifecycle
// values. Artifact bytes, MANIFEST_KEY, and the selected prepared reference
// never cross back into the conductor process.
type ArtifactPrepareSpec struct {
	RootSourceKind        string `json:"root_source_kind"`
	RootRef               string `json:"root_ref"`
	LaunchMode            string `json:"launch_mode"`
	ManifestConfig        string `json:"manifest_config,omitempty"`
	RefLocationParent     string `json:"ref_location_parent,omitempty"`
	RelativeDir           string `json:"relative_dir,omitempty"`
	MaxRefs               int    `json:"max_refs"`
	ReadSourceImageConfig bool   `json:"read_source_image_config,omitempty"`
	// PreflightImageBundle is Build-only. It verifies the named-location image
	// Bundle configuration, task customer key, and write admission before the
	// source E/S carrier or its image is scanned.
	PreflightImageBundle     bool  `json:"preflight_image_bundle,omitempty"`
	AbsoluteDeadlineUnixNano int64 `json:"absolute_deadline_unix_nano"`
}

// SandboxTaskSpec is the authenticated bootstrap response. Exactly one of
// Final and Prepare is non-nil. Cold image launches receive Final immediately;
// restore launches complete Prepare and then receive a final LaunchSpec.
type SandboxTaskSpec struct {
	SandboxID string               `json:"sandbox_id"`
	RunID     string               `json:"run_id"`
	Workdir   string               `json:"workdir"`
	Env       map[string]string    `json:"env,omitempty"`
	Final     *LaunchSpec          `json:"final,omitempty"`
	Prepare   *ArtifactPrepareSpec `json:"prepare,omitempty"`
	Error     string               `json:"error,omitempty"`
}

type ArtifactCapacity struct {
	CPU               int     `json:"cpu"`
	Memory            string  `json:"memory"`
	AllocatableCPU    float64 `json:"allocatable_cpu,omitempty"`
	AllocatableMemory string  `json:"allocatable_memory,omitempty"`
	DeflateOnOOM      *bool   `json:"deflate_on_oom,omitempty"`
}

// ArtifactNetwork is the bounded, non-secret network projection produced by
// the tenant task after it has parsed and validated artifact metadata. The
// conductor never receives or parses the original metadata document.
type ArtifactNetwork struct {
	Hostname         string   `json:"hostname,omitempty"`
	DNS              []string `json:"dns,omitempty"`
	InnerIP          string   `json:"inner_ip,omitempty"`
	Nexthop          string   `json:"nexthop,omitempty"`
	TransitGatewayIP string   `json:"transit_gateway_ip,omitempty"`
	TransitGeneveVNI uint32   `json:"transit_geneve_vni,omitempty"`
	TransitMAC       string   `json:"transit_mac,omitempty"`
}

// ArtifactPrepareSummary is the non-secret immutable handoff from a runner to
// the one launch worker for its exact run. ResolutionDigest is an idempotency
// fingerprint, not an authentication credential.
type ArtifactPrepareSummary struct {
	SchemaVersion      int    `json:"schema_version"`
	PreparedSourceKind string `json:"prepared_source_kind"`
	// HasBuildCommands is Build-only and gated by BuildTaskSchemaVersion 6.
	// Source command text stays task-local; ordinary Sandbox summaries omit it.
	HasBuildCommands bool                       `json:"has_build_commands,omitempty"`
	Capacity         ArtifactCapacity           `json:"capacity"`
	Network          ArtifactNetwork            `json:"network,omitempty"`
	DiskTopology     types.ArtifactDiskTopology `json:"disk_topology"`
	ResolutionDigest string                     `json:"resolution_digest"`
	RequiredRefCount int                        `json:"required_ref_count"`
}

// CloneArtifactPrepareSummary isolates every slice-bearing summary field so an
// HTTP caller cannot mutate an accepted replay identity after admission.
func CloneArtifactPrepareSummary(summary ArtifactPrepareSummary) ArtifactPrepareSummary {
	if summary.Capacity.DeflateOnOOM != nil {
		value := *summary.Capacity.DeflateOnOOM
		summary.Capacity.DeflateOnOOM = &value
	}
	summary.Network.DNS = append([]string(nil), summary.Network.DNS...)
	summary.DiskTopology.Disks = append([]types.ArtifactDiskShape(nil), summary.DiskTopology.Disks...)
	return summary
}

// EqualArtifactPrepareSummary compares the complete immutable task handoff.
func EqualArtifactPrepareSummary(a, b ArtifactPrepareSummary) bool {
	return a.SchemaVersion == b.SchemaVersion &&
		a.PreparedSourceKind == b.PreparedSourceKind &&
		a.HasBuildCommands == b.HasBuildCommands &&
		equalArtifactCapacity(a.Capacity, b.Capacity) &&
		a.Network.Hostname == b.Network.Hostname &&
		slices.Equal(a.Network.DNS, b.Network.DNS) &&
		a.Network.InnerIP == b.Network.InnerIP &&
		a.Network.Nexthop == b.Network.Nexthop &&
		a.Network.TransitGatewayIP == b.Network.TransitGatewayIP &&
		a.Network.TransitGeneveVNI == b.Network.TransitGeneveVNI &&
		a.Network.TransitMAC == b.Network.TransitMAC &&
		a.DiskTopology.Root == b.DiskTopology.Root &&
		slices.Equal(a.DiskTopology.Disks, b.DiskTopology.Disks) &&
		a.ResolutionDigest == b.ResolutionDigest &&
		a.RequiredRefCount == b.RequiredRefCount
}

func equalArtifactCapacity(a, b ArtifactCapacity) bool {
	if a.CPU != b.CPU || a.Memory != b.Memory || a.AllocatableCPU != b.AllocatableCPU ||
		a.AllocatableMemory != b.AllocatableMemory {
		return false
	}
	return (a.DeflateOnOOM == nil && b.DeflateOnOOM == nil) ||
		(a.DeflateOnOOM != nil && b.DeflateOnOOM != nil && *a.DeflateOnOOM == *b.DeflateOnOOM)
}

type ArtifactPrepareRequest struct {
	SandboxID string                 `json:"sandbox_id"`
	RunID     string                 `json:"run_id"`
	Summary   ArtifactPrepareSummary `json:"summary"`
}

type ArtifactPrepareResponse struct {
	Final *LaunchSpec `json:"final,omitempty"`
	Error string      `json:"error,omitempty"`
}

// SandboxTaskAuth is the non-secret identity needed to authenticate an exact
// assigned runner before the secret-bearing SandboxTaskSpec provider is called.
type SandboxTaskAuth struct {
	PidFile string
}

// ArtifactPrepareRejection marks a definitive stale/conflicting completion.
// The task must not retry it as a transient conductor failure.
type ArtifactPrepareRejection struct{ Err error }

func (e *ArtifactPrepareRejection) Error() string { return e.Err.Error() }
func (e *ArtifactPrepareRejection) Unwrap() error { return e.Err }

func RejectArtifactPrepare(err error) error {
	if err == nil {
		return nil
	}
	return &ArtifactPrepareRejection{Err: err}
}

func IsArtifactPrepareRejection(err error) bool {
	var rejection *ArtifactPrepareRejection
	return errors.As(err, &rejection)
}

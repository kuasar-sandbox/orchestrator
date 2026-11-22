// Package metadata defines the cluster control plane's durable domain records and
// storage contracts. It has no native database or Registry dependency.
package metadata

import (
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type SandboxState string

const (
	StateNone     SandboxState = "none"
	StateReserved SandboxState = "reserved"
	StateReady    SandboxState = "ready"
	StatePaused   SandboxState = "paused"
	StatePausing  SandboxState = "pausing"
	StateDead     SandboxState = "dead"
	StateDeleting SandboxState = "deleting"
)

type BuildState string

const (
	BuildStarting   BuildState = "starting"
	BuildRegistered BuildState = "registered"
	BuildWaiting    BuildState = "waiting"
	BuildBuilding   BuildState = "building"
	BuildReady      BuildState = "ready"
	BuildError      BuildState = "error"
)

type SandboxRecord struct {
	WorkspacesDigest       string                    `json:"workspaces_digest,omitempty"`
	InstanceID             string                    `json:"instance_id,omitempty"`
	StateVersion           uint64                    `json:"state_version,omitempty"`
	Group                  string                    `json:"group"`
	RouteKey               string                    `json:"route_key"`
	SandboxID              string                    `json:"sandbox_id,omitempty"`
	NodeSandboxID          string                    `json:"node_sandbox_id,omitempty"`
	SandboxGeneration      uint64                    `json:"sandbox_generation"`
	NextSandboxGeneration  uint64                    `json:"next_sandbox_generation"`
	State                  SandboxState              `json:"state"`
	NodeID                 string                    `json:"node_id,omitempty"`
	ArtifactLocation       string                    `json:"artifact_location,omitempty"`
	TemplateID             string                    `json:"template_id,omitempty"`
	Profile                string                    `json:"profile,omitempty"`
	APISecretFingerprint   string                    `json:"api_secret_fingerprint,omitempty"`
	ManifestKeyFingerprint string                    `json:"manifest_key_fingerprint,omitempty"`
	CreateCredentials      *sandboxcfg.Credentials   `json:"create_credentials,omitempty"`
	StableID               string                    `json:"stable_id,omitempty"`
	APISecret              string                    `json:"api_secret,omitempty"`
	ServiceSecret          string                    `json:"service_secret,omitempty"`
	EnvdAccessToken        string                    `json:"envd_access_token,omitempty"`
	TrafficAccessToken     string                    `json:"traffic_access_token,omitempty"`
	ForwardAccessToken     string                    `json:"forward_access_token,omitempty"`
	TargetPort             int                       `json:"target_port,omitempty"`
	LastActive             int64                     `json:"last_active,omitempty"`
	BuildID                string                    `json:"build_id,omitempty"`
	BuildState             BuildState                `json:"build_state,omitempty"`
	BuildResources         *routesync.BuildResources `json:"build_resources,omitempty"`
	BuildReason            string                    `json:"build_reason,omitempty"`
	CreatedU               int64                     `json:"created_unix,omitempty"`
	IdleTimeout            *int64                    `json:"idle_timeout,omitempty"`
	MaxCheckpoints         int                       `json:"max_checkpoints,omitempty"`
	AutoPauseMemory        bool                      `json:"auto_pause_memory"`
	AutoReset              bool                      `json:"auto_reset"`
}

type BuildRecord struct {
	Group                         string                    `json:"group"`
	BuildID                       string                    `json:"build_id"`
	NodeID                        string                    `json:"node_id"`
	Profile                       types.Profile             `json:"profile"`
	APISecretFingerprint          string                    `json:"api_secret_fingerprint"`
	Resources                     *routesync.BuildResources `json:"resources,omitempty"`
	RegistrationConfig            map[string]string         `json:"registration_config,omitempty"`
	RegistrationEnv               map[string]string         `json:"registration_env,omitempty"`
	RegistrationSecure            bool                      `json:"registration_secure,omitempty"`
	RegistrationCredentialsDigest string                    `json:"registration_credentials_digest,omitempty"`
	RegistrationMMDSValuesDigest  string                    `json:"registration_mmds_values_digest,omitempty"`
	RegistrationImageRepo         string                    `json:"registration_image_repo,omitempty"`
	RegistrationRegistryAuth      string                    `json:"registration_registry_auth,omitempty"`
	RegistrationTarget            *types.BuildTarget        `json:"registration_target,omitempty"`
	RegistrationTargetSet         bool                      `json:"registration_target_set,omitempty"`
	State                         BuildState                `json:"state"`
	TemplateID                    string                    `json:"template_id,omitempty"` // immutable registration transient ID
	PersistID                     string                    `json:"persist_id,omitempty"`
	Reason                        string                    `json:"reason,omitempty"`
	CreatedU                      int64                     `json:"created_unix,omitempty"`

	RegistrationWorkspacesDigest string `json:"registration_workspaces_digest,omitempty"`
}

type NodeState string

const (
	NodeUnknown NodeState = "unknown"
	NodeLive    NodeState = "live"
	NodeDrained NodeState = "drained"
	NodeDead    NodeState = "dead"
)

type NodeSandboxRef struct {
	Group                string `json:"group"`
	RouteKey             string `json:"route_key"`
	SandboxID            string `json:"sandbox_id"`
	SandboxGeneration    uint64 `json:"sandbox_generation"`
	NodeSandboxID        string `json:"node_sandbox_id"`
	Profile              string `json:"profile"`
	APISecretFingerprint string `json:"api_secret_fingerprint"`
}

type NodeBuildRef struct {
	Group      string `json:"group"`
	BuildID    string `json:"build_id"`
	TemplateID string `json:"template_id"`
}

type NodeKeyPair struct {
	APISecretFingerprint   string `json:"api_secret_fingerprint"`
	APISecretType          string `json:"api_secret_type,omitempty"`
	APISecret              string `json:"api_secret,omitempty"`
	APISecretRef           string `json:"api_secret_ref,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	ManifestKeyType        string `json:"manifest_key_type,omitempty"`
	ManifestKey            string `json:"manifest_key,omitempty"`
	ManifestKeyRef         string `json:"manifest_key_ref,omitempty"`
	ExpiresUnix            int64  `json:"expires_unix,omitempty"`
	AckedExpiresUnix       int64  `json:"acked_expires_unix,omitempty"`
}

type NodeProfileRecord struct {
	NodeID                    string                         `json:"node_id"`
	State                     NodeState                      `json:"state"`
	Labels                    map[string]string              `json:"labels,omitempty"`
	Capacity                  int                            `json:"capacity,omitempty"`
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit `json:"build_registration_capacity,omitempty"`
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit `json:"build_execution_capacity,omitempty"`
	APIEndpoint               string                         `json:"api_endpoint,omitempty"`
	DataEndpoint              string                         `json:"data_endpoint,omitempty"`
	RuntimeDigest             string                         `json:"runtime_digest,omitempty"`
	Zone                      string                         `json:"zone,omitempty"`
	Allocated                 int64                          `json:"allocated,omitempty"`
	Pool                      int64                          `json:"pool,omitempty"`
	BuildRegistrationUsage    *routesync.BuildAdmissionUsage `json:"build_registration_usage,omitempty"`
	BuildExecutionUsage       *routesync.BuildAdmissionUsage `json:"build_execution_usage,omitempty"`
	Counts                    int                            `json:"counts,omitempty"`
	Draining                  bool                           `json:"draining,omitempty"`
	LastHeartbeatUnix         int64                          `json:"last_heartbeat_unix,omitempty"`
	ResumeToken               string                         `json:"resume_token,omitempty"`
	LinkOwner                 string                         `json:"link_owner,omitempty"`
}

type PlacerImportSourceState struct {
	SourceID      string `json:"source_id"`
	OwnerID       string `json:"owner_id,omitempty"`
	RunID         string `json:"run_id,omitempty"`
	Term          uint64 `json:"term,omitempty"`
	Cursor        string `json:"cursor,omitempty"`
	Round         uint64 `json:"round,omitempty"`
	NextRunUnixMs int64  `json:"next_run_unix_ms,omitempty"`
	LastError     string `json:"last_error,omitempty"`
	ExpiresUnixMs int64  `json:"expires_unix_ms,omitempty"`
}

type NodeListEntry struct {
	// Version is the catalog record's committed version, populated by Load.
	Version                   Version                        `json:"version"`
	NodeID                    string                         `json:"node_id"`
	Labels                    map[string]string              `json:"labels,omitempty"`
	Capacity                  int                            `json:"capacity,omitempty"`
	BuildRegistrationCapacity *routesync.BuildAdmissionLimit `json:"build_registration_capacity,omitempty"`
	BuildExecutionCapacity    *routesync.BuildAdmissionLimit `json:"build_execution_capacity,omitempty"`
	APIEndpoint               string                         `json:"api_endpoint,omitempty"`
	DataEndpoint              string                         `json:"data_endpoint,omitempty"`
	RuntimeDigest             string                         `json:"runtime_digest,omitempty"`
	Draining                  bool                           `json:"draining,omitempty"`
}

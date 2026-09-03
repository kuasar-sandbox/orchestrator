package registry

import (
	"context"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// BuildStore is the registry-facing routing view of build execution state. Builds
// are stored as route_link build records and tracked through registered →
// waiting → building → ready/error. The node's SQLite state and heartbeat are
// the authoritative source for admission usage.

// BuildState mirrors the node's build lifecycle for placement accounting.
type BuildState string

const (
	BuildStarting   BuildState = "starting"
	BuildRegistered BuildState = "registered"
	BuildWaiting    BuildState = "waiting"
	BuildBuilding   BuildState = "building"
	BuildReady      BuildState = "ready"
	BuildError      BuildState = "error"
)

// BuildRecord is the Registry's retention-bounded projection of a node Build.
// It is query/routing state, never the authority for a canonical TemplateID.
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
	TemplateID                    string                    `json:"template_id,omitempty"` // assigned template id, refreshed from terminal node events
	Reason                        string                    `json:"reason,omitempty"`
	CreatedU                      int64                     `json:"created_unix,omitempty"`

	// registrationMMDSSecrets exists only on the current ReserveBuild call. It
	// is intentionally unexported so shard serialization can never persist
	// tenant MMDS values in the replicated Registry record.
	registrationMMDSSecrets map[string]string
	// registrationCredentials follows the same request-scoped replay boundary:
	// only its digest is replicated, while the selected node persists the values
	// encrypted in its Build record.
	registrationCredentials *sandboxcfg.Credentials
}

// occupies reports whether the registry still considers the build live for node
// ownership and disconnect reconciliation. It does not calculate admission usage.
func (b *BuildRecord) occupies() bool {
	return b.State == BuildStarting || b.State == BuildRegistered || b.State == BuildWaiting || b.State == BuildBuilding
}

func (s *Stores) PutBuild(ctx context.Context, b *BuildRecord) error {
	_, err := s.putRouteBuildShard(ctx, b)
	return err
}

func (s *Stores) GetBuildInGroup(ctx context.Context, group, buildID string) (*BuildRecord, bool, error) {
	rec, _, found, err := s.getRouteBuildShard(ctx, group, buildID)
	return rec, found, err
}

func (s *Stores) DeleteBuild(ctx context.Context, group, buildID string) error {
	return s.deleteRouteBuildShard(ctx, group, buildID)
}

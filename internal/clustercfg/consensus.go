package clustercfg

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type RegistryLayoutArtifacts struct {
	Chain string `yaml:"chain"`
	Keys  string `yaml:"keys"`
	Guard string `yaml:"guard"`
}

func (a RegistryLayoutArtifacts) Validate() error {
	for name, path := range map[string]string{"chain": a.Chain, "keys": a.Keys, "guard": a.Guard} {
		if path == "" || !filepath.IsAbs(path) {
			return fmt.Errorf("clustercfg: registryLayout.%s must be an absolute path", name)
		}
	}
	return nil
}

type NamedEndpoint struct {
	Name     string `yaml:"name"`
	Endpoint string `yaml:"endpoint"`
}

type EndpointSet struct {
	Endpoints []NamedEndpoint `yaml:"endpoints"`
	TLS       TLS             `yaml:"tls"`
}

func (s EndpointSet) Validate(name string) error {
	if len(s.Endpoints) == 0 {
		return fmt.Errorf("clustercfg: %s.endpoints must not be empty", name)
	}
	if !s.TLS.Enabled() || s.TLS.CA == "" {
		return fmt.Errorf("clustercfg: %s.tls requires cert, key, and CA", name)
	}
	seen := make(map[string]struct{}, len(s.Endpoints))
	for _, endpoint := range s.Endpoints {
		if endpoint.Name == "" || clusterstate.ValidateCanonicalHTTPSBaseEndpoint(endpoint.Endpoint) != nil {
			return fmt.Errorf("clustercfg: %s endpoint must have a name and canonical HTTPS base URL", name)
		}
		if _, duplicate := seen[endpoint.Name]; duplicate {
			return fmt.Errorf("clustercfg: duplicate %s endpoint %q", name, endpoint.Name)
		}
		seen[endpoint.Name] = struct{}{}
	}
	return nil
}

type ConsensusStorage struct {
	NodeHostDir       string `yaml:"nodehost_dir"`
	WALDir            string `yaml:"wal_dir,omitempty"`
	StateEngineDir    string `yaml:"state_engine_dir"`
	EnrollmentPath    string `yaml:"enrollment_path"`
	RaftListen        string `yaml:"raft_listen,omitempty"`
	OpenMode          string `yaml:"open_mode"`
	BootstrapSecret   string `yaml:"bootstrap_secret_file,omitempty"`
	StorageProtection string `yaml:"storage_protection"`
	InitializeWorkers int    `yaml:"initialize_workers"`
	TransitionWorkers int    `yaml:"transition_workers"`
	SnapshotWorkers   int    `yaml:"snapshot_workers"`
	OperationTimeout  string `yaml:"operation_timeout"`
	FenceRetention    string `yaml:"fence_retention"`
	TLS               TLS    `yaml:"tls"`
}

func (s ConsensusStorage) Validate() error {
	for name, path := range map[string]string{
		"nodehost_dir": s.NodeHostDir, "state_engine_dir": s.StateEngineDir,
		"enrollment_path": s.EnrollmentPath,
	} {
		if path == "" || !filepath.IsAbs(path) {
			return fmt.Errorf("clustercfg: storage.%s must be an absolute path", name)
		}
	}
	if s.WALDir != "" && !filepath.IsAbs(s.WALDir) {
		return errors.New("clustercfg: storage.wal_dir must be an absolute path")
	}
	switch s.OpenMode {
	case "bootstrap":
		if s.BootstrapSecret == "" || !filepath.IsAbs(s.BootstrapSecret) {
			return errors.New("clustercfg: bootstrap requires an absolute bootstrap_secret_file")
		}
	case "join", "restart":
		if s.BootstrapSecret != "" {
			return errors.New("clustercfg: bootstrap_secret_file is valid only in bootstrap mode")
		}
	default:
		return errors.New("clustercfg: storage.open_mode must be bootstrap, join, or restart")
	}
	if s.StorageProtection != "dm-crypt" && s.StorageProtection != "ephemeral-tmpfs" {
		return errors.New("clustercfg: storage.storage_protection must be dm-crypt or ephemeral-tmpfs")
	}
	if s.InitializeWorkers <= 0 || s.InitializeWorkers > 256 {
		return errors.New("clustercfg: storage.initialize_workers must be in [1,256]")
	}
	if s.TransitionWorkers <= 0 || s.TransitionWorkers > 256 {
		return errors.New("clustercfg: storage.transition_workers must be in [1,256]")
	}
	if s.SnapshotWorkers <= 0 || s.SnapshotWorkers > 64 {
		return errors.New("clustercfg: storage.snapshot_workers must be in [1,64]")
	}
	operationTimeout, err := time.ParseDuration(s.OperationTimeout)
	if err != nil || operationTimeout < 100*time.Millisecond || operationTimeout > time.Minute {
		return errors.New("clustercfg: storage.operation_timeout must be in [100ms,60s]")
	}
	retention, err := time.ParseDuration(s.FenceRetention)
	if err != nil || retention <= 0 || retention > 30*24*time.Hour {
		return errors.New("clustercfg: storage.fence_retention must be in (0,30d]")
	}
	if !s.TLS.Enabled() || s.TLS.CA == "" {
		return errors.New("clustercfg: storage.tls requires cert, key, and CA")
	}
	return nil
}

func (s ConsensusStorage) OperationTimeoutDuration() time.Duration {
	value, _ := time.ParseDuration(s.OperationTimeout)
	return value
}

func (s ConsensusStorage) FenceRetentionDuration() time.Duration {
	value, _ := time.ParseDuration(s.FenceRetention)
	return value
}

type RegistrySessionConfig struct {
	MaxNodes           int    `yaml:"max_nodes"`
	AntiEntropy        string `yaml:"anti_entropy"`
	EventWorkers       int    `yaml:"event_workers"`
	ReconnectPerSecond int    `yaml:"reconnect_per_second"`
}

type RegistryWorkflowConfig struct {
	ParkTimeout             string `yaml:"park_timeout"`
	PollInterval            string `yaml:"poll_interval"`
	PermitRefresh           string `yaml:"permit_refresh"`
	RecoveryScanInterval    string `yaml:"recovery_scan_interval"`
	RecoveryShardsPerScan   uint32 `yaml:"recovery_shards_per_scan"`
	RecoveryWorkers         int    `yaml:"recovery_workers"`
	RecoveryPerNodeWorkers  int    `yaml:"recovery_per_node_workers"`
	CompactionWorkers       int    `yaml:"compaction_workers"`
	PendingWorkflowsPerPage uint32 `yaml:"pending_workflows_per_page"`
	RecoveryPageObjects     uint32 `yaml:"recovery_page_objects"`
	RecoveryPageBytes       uint32 `yaml:"recovery_page_bytes"`
	RecoveryMaxReportBytes  uint64 `yaml:"recovery_max_report_bytes"`
	RecoveryBytesPerSecond  uint64 `yaml:"recovery_bytes_per_second"`
	RecoveryLookupPage      uint32 `yaml:"recovery_lookup_page"`
	SandboxLaunchPerSecond  int    `yaml:"sandbox_launch_per_second"`
	SandboxLaunchBurst      int    `yaml:"sandbox_launch_burst"`
	BuildLaunchPerSecond    int    `yaml:"build_launch_per_second"`
	BuildLaunchBurst        int    `yaml:"build_launch_burst"`
}

type ConsensusRegistryConfig struct {
	Member         MemberConfig            `yaml:"member"`
	RegistryLayout RegistryLayoutArtifacts `yaml:"registry_layout"`
	Storage        ConsensusStorage        `yaml:"storage"`
	Placers        EndpointSet             `yaml:"placers"`
	Session        RegistrySessionConfig   `yaml:"session"`
	Workflow       RegistryWorkflowConfig  `yaml:"workflow"`
}

func LoadConsensusRegistry(path string) (*ConsensusRegistryConfig, error) {
	config := ConsensusRegistryConfig{
		Member: MemberConfig{Listen: ":7700"},
		Storage: ConsensusStorage{
			OpenMode: "restart", StorageProtection: "dm-crypt", InitializeWorkers: 16,
			TransitionWorkers: 16, SnapshotWorkers: 4, OperationTimeout: "5s", FenceRetention: "1h",
		},
		Session: RegistrySessionConfig{MaxNodes: 5000, AntiEntropy: "2s", EventWorkers: 32, ReconnectPerSecond: 200},
		Workflow: RegistryWorkflowConfig{
			ParkTimeout: "30s", PollInterval: "20ms", PermitRefresh: "1s",
			RecoveryScanInterval: "250ms", RecoveryShardsPerScan: 64,
			RecoveryWorkers: 8, RecoveryPerNodeWorkers: 1,
			CompactionWorkers: 4, PendingWorkflowsPerPage: 256,
			RecoveryPageObjects: 64, RecoveryPageBytes: 512 << 10,
			RecoveryMaxReportBytes: 64 << 20, RecoveryBytesPerSecond: 16 << 20, RecoveryLookupPage: 256,
			SandboxLaunchPerSecond: 500, SandboxLaunchBurst: 500,
			BuildLaunchPerSecond: 100, BuildLaunchBurst: 100,
		},
	}
	if err := loadStrictYAML(path, &config); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (c ConsensusRegistryConfig) Validate() error {
	if c.Member.ID == "" || c.Member.Listen == "" || !c.Member.TLS.Enabled() || c.Member.TLS.CA == "" {
		return errors.New("clustercfg: Registry member ID, listen address, and mTLS are required")
	}
	if filepath.IsAbs(c.Member.Listen) {
		return errors.New("clustercfg: certificate-authenticated Registry listener must use TCP")
	}
	if err := c.RegistryLayout.Validate(); err != nil {
		return err
	}
	if err := c.Storage.Validate(); err != nil {
		return err
	}
	if err := c.Placers.Validate("placers"); err != nil {
		return err
	}
	if c.Session.MaxNodes <= 0 || c.Session.EventWorkers <= 0 || c.Session.ReconnectPerSecond <= 0 {
		return errors.New("clustercfg: Registry Session limits must be positive")
	}
	if c.Workflow.RecoveryShardsPerScan == 0 || c.Workflow.RecoveryWorkers <= 0 ||
		c.Workflow.RecoveryPerNodeWorkers <= 0 || c.Workflow.RecoveryPerNodeWorkers > c.Workflow.RecoveryWorkers ||
		c.Workflow.CompactionWorkers <= 0 || c.Workflow.CompactionWorkers > 256 ||
		c.Workflow.PendingWorkflowsPerPage == 0 || c.Workflow.PendingWorkflowsPerPage > 4096 {
		return errors.New("clustercfg: Registry workflow bounds are invalid")
	}
	if c.Workflow.RecoveryPageObjects == 0 || c.Workflow.RecoveryPageObjects > 256 ||
		c.Workflow.RecoveryPageBytes == 0 || c.Workflow.RecoveryPageBytes > 768<<10 ||
		c.Workflow.RecoveryMaxReportBytes < uint64(c.Workflow.RecoveryPageBytes) ||
		c.Workflow.RecoveryBytesPerSecond < uint64(c.Workflow.RecoveryPageBytes) ||
		c.Workflow.RecoveryLookupPage == 0 || c.Workflow.RecoveryLookupPage > 4096 {
		return errors.New("clustercfg: Registry recovery report bounds are invalid")
	}
	if c.Workflow.SandboxLaunchPerSecond <= 0 || c.Workflow.SandboxLaunchBurst <= 0 ||
		c.Workflow.BuildLaunchPerSecond <= 0 || c.Workflow.BuildLaunchBurst <= 0 {
		return errors.New("clustercfg: Registry aggregate launch rates and bursts must be positive")
	}
	return validateDurations(map[string]string{
		"session.anti_entropy":            c.Session.AntiEntropy,
		"workflow.park_timeout":           c.Workflow.ParkTimeout,
		"workflow.poll_interval":          c.Workflow.PollInterval,
		"workflow.permit_refresh":         c.Workflow.PermitRefresh,
		"workflow.recovery_scan_interval": c.Workflow.RecoveryScanInterval,
	})
}

func (c ConsensusRegistryConfig) AntiEntropyDuration() time.Duration {
	value, _ := time.ParseDuration(c.Session.AntiEntropy)
	return value
}

func (c ConsensusRegistryConfig) WorkflowDurations() (park, poll, permit, recovery time.Duration) {
	park, _ = time.ParseDuration(c.Workflow.ParkTimeout)
	poll, _ = time.ParseDuration(c.Workflow.PollInterval)
	permit, _ = time.ParseDuration(c.Workflow.PermitRefresh)
	recovery, _ = time.ParseDuration(c.Workflow.RecoveryScanInterval)
	return
}

type ConsensusRouterConfig struct {
	Domain                  string                  `yaml:"domain"`
	RegistryLayout          RegistryLayoutArtifacts `yaml:"registry_layout"`
	RegistryTLS             TLS                     `yaml:"registry_tls"`
	RegistryResponseTimeout string                  `yaml:"registry_response_timeout"`
	NodeTLS                 TLS                     `yaml:"node_tls"`
	Providers               EndpointSet             `yaml:"providers"`
	Ingress                 IngressConfig           `yaml:"ingress"`
	Auth                    RouterAuth              `yaml:"auth"`
	Cache                   RouterCache             `yaml:"cache"`
	MetricsListen           string                  `yaml:"metrics_listen,omitempty"`
}

func LoadConsensusRouter(path string) (*ConsensusRouterConfig, error) {
	config := ConsensusRouterConfig{
		RegistryResponseTimeout: "35s",
		Ingress:                 IngressConfig{Listen: ":443"},
		Auth:                    RouterAuth{APIKey: "enforce", DataPlane: "enforce", CacheTTL: "60s"},
		Cache:                   RouterCache{RouteTTL: "5m", IdleTimeout: "2m"},
	}
	if err := loadStrictYAML(path, &config); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (c ConsensusRouterConfig) Validate() error {
	if c.Domain == "" || c.Ingress.Listen == "" {
		return errors.New("clustercfg: Router domain and ingress.listen are required")
	}
	if (c.Ingress.TLS.Cert != "" || c.Ingress.TLS.Key != "" || c.Ingress.TLS.CA != "") && !c.Ingress.TLS.Enabled() {
		return errors.New("clustercfg: Router ingress TLS requires both cert and key when configured")
	}
	if err := c.RegistryLayout.Validate(); err != nil {
		return err
	}
	if !c.RegistryTLS.Enabled() || c.RegistryTLS.CA == "" {
		return errors.New("clustercfg: Router registry_tls requires cert, key, and CA")
	}
	if !c.NodeTLS.Enabled() || c.NodeTLS.CA == "" {
		return errors.New("clustercfg: Router node_tls requires cert, key, and CA")
	}
	if err := c.Providers.Validate("providers"); err != nil {
		return err
	}
	switch c.Auth.APIKey {
	case "off", "log", "enforce":
	default:
		return errors.New("clustercfg: auth.api_key must be off, log, or enforce")
	}
	switch c.Auth.DataPlane {
	case "off", "log", "enforce":
	default:
		return errors.New("clustercfg: auth.data_plane must be off, log, or enforce")
	}
	if err := validateDurations(map[string]string{
		"registry_response_timeout": c.RegistryResponseTimeout,
		"auth.cache_ttl":            c.Auth.CacheTTL,
		"cache.route_ttl":           c.Cache.RouteTTL,
		"cache.idle_timeout":        c.Cache.IdleTimeout,
	}); err != nil {
		return err
	}
	responseTimeout, _ := time.ParseDuration(c.RegistryResponseTimeout)
	if responseTimeout > 5*time.Minute {
		return errors.New("clustercfg: registry_response_timeout must not exceed 5m")
	}
	return nil
}

func (c ConsensusRouterConfig) RegistryResponseTimeoutDuration() time.Duration {
	value, _ := time.ParseDuration(c.RegistryResponseTimeout)
	return value
}

func (c ConsensusRouterConfig) AuthCacheDuration() time.Duration {
	value, _ := time.ParseDuration(c.Auth.CacheTTL)
	return value
}

func (c ConsensusRouterConfig) CacheDurations() (time.Duration, time.Duration) {
	route, _ := time.ParseDuration(c.Cache.RouteTTL)
	idle, _ := time.ParseDuration(c.Cache.IdleTimeout)
	return route, idle
}

type FinalPlacerProcessConfig struct {
	ID     string `yaml:"id"`
	Listen string `yaml:"listen"`
	TLS    TLS    `yaml:"tls"`
}

type FinalPlacerConfig struct {
	Placer       FinalPlacerProcessConfig `yaml:"placer"`
	GroupSources []GroupSourceConfig      `yaml:"group_sources"`
	Placement    PlacementConfig          `yaml:"placement"`
}

func LoadFinalPlacer(path string) (*FinalPlacerConfig, error) {
	config := FinalPlacerConfig{
		Placer:    FinalPlacerProcessConfig{ID: "placer", Listen: ":7800"},
		Placement: PlacementConfig{Candidates: 4},
	}
	if err := loadStrictYAML(path, &config); err != nil {
		return nil, err
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return &config, nil
}

func (c FinalPlacerConfig) Validate() error {
	if c.Placer.ID == "" || c.Placer.Listen == "" || !c.Placer.TLS.Enabled() || c.Placer.TLS.CA == "" {
		return errors.New("clustercfg: Placer ID, listen address, and mTLS are required")
	}
	if filepath.IsAbs(c.Placer.Listen) {
		return errors.New("clustercfg: certificate-authenticated Placer listener must use TCP")
	}
	if len(c.GroupSources) == 0 {
		return errors.New("clustercfg: Placer requires at least one group source")
	}
	seen := make(map[string]struct{}, len(c.GroupSources))
	for _, source := range c.GroupSources {
		if source.SourceID == "" || source.SourceType != "file" || source.Path == "" || !filepath.IsAbs(source.Path) {
			return errors.New("clustercfg: every final Placer source requires a unique ID, type=file, and absolute path")
		}
		if _, duplicate := seen[source.SourceID]; duplicate {
			return errors.New("clustercfg: duplicate Placer source ID")
		}
		seen[source.SourceID] = struct{}{}
	}
	if c.Placement.Candidates != 4 {
		return errors.New("clustercfg: final Placer candidate count must be 4")
	}
	for _, rule := range c.Placement.ShuffleSharding {
		if len(rule.Selector) == 0 || rule.ShardBy == "" || rule.N <= 0 {
			return errors.New("clustercfg: every shuffle_sharding rule requires selector, shard_by, and positive n")
		}
		for key, value := range rule.Selector {
			if key == "" || value == "" {
				return errors.New("clustercfg: shuffle_sharding selector keys and values must not be empty")
			}
		}
	}
	return nil
}

func loadStrictYAML(path string, target any) error {
	if path == "" {
		return errors.New("clustercfg: config path is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("clustercfg: read %s: %w", path, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("clustercfg: parse %s: %w", path, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return errors.New("clustercfg: config contains multiple YAML documents")
	} else if !errors.Is(err, io.EOF) {
		return fmt.Errorf("clustercfg: parse trailing YAML: %w", err)
	}
	return nil
}

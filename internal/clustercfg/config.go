// Package clustercfg loads cluster-ctl's configuration. Each role runs as its own
// process with its OWN config file and schema (cluster.md §3) — there is no shared
// file: registry.yaml / router.yaml / scaler.yaml each carry only what that role
// needs, grouped by the cluster link they operate: node_link, route_link,
// scale_link, and node_list.
package clustercfg

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// SandboxGroup provider selection. Each fine-grained interface is independently
// "store" (registry self-stores) or "external:<addr>".
const (
	ProviderStore         = "store"
	ProviderExternalPfx   = "external:" // external:<addr>
	envGroupEncryptionKey = "SANDBOX_GROUP_ENCRYPTION_KEY"
)

// Provider interface names (keys of SandboxGroupConfig.Providers).
const (
	ProviderKey           = "key"            // GroupKeyProvider (project_id + manifest_key + auth_key)
	ProviderSandboxConfig = "sandbox_config" // GroupSandboxConfigProvider
	ProviderPlacement     = "placement"      // GroupPlacementProvider (nodeSelectors + shuffle labels)
	ProviderImagePull     = "image_pull"     // GroupImagePullProvider (image_repo + registry_auth, §7.5)
)

const (
	defaultRouteLinkEndpoint = "/run/cluster/route-link.sock"
	defaultScaleLinkEndpoint = "/run/cluster/scale-link.sock"
)

// ===========================================================================
// Shared sub-types (reused across the three role schemas).
// ===========================================================================

// SandboxGroupConfig selects, per fine-grained provider, store vs external, and
// the at-rest key for self-stored sandbox-group secrets.
type SandboxGroupConfig struct {
	// Providers maps each interface (key / sandbox_config / placement / image_pull)
	// to "store" or "external:<addr>". Empty entries default to store.
	Providers     map[string]string `yaml:"providers"`
	EncryptionKey string            `yaml:"encryption_key"` // AES-256 at-rest for self-stored group secrets; or SANDBOX_GROUP_ENCRYPTION_KEY env
	TLS           TLS               `yaml:"tls"`            // client mTLS for external:<addr> providers (§6.2)
}

// NodeLinkConfig is the listener nodes dial to register, stream routes, and receive
// commands (the node-link protocol).
type NodeLinkConfig struct {
	Listen            string `yaml:"listen"`             // node dial target; default :7700
	TLS               TLS    `yaml:"tls"`                // server mTLS (production)
	HeartbeatInterval string `yaml:"heartbeat_interval"` // default 10s
	NodeDeadAfter     string `yaml:"node_dead_after"`    // default 30s (§11 failure)
	RevisionRetention int    `yaml:"revision_retention"` // change-log depth for resume_from; default 10000
}

// LinkListenConfig is a registry-owned link listener. route_link is dialed by
// routers/admin tools; scale_link is dialed by scalers for node_list/group views
// and reverse placement.
type LinkListenConfig struct {
	Listen string `yaml:"listen"` // UDS path / host:port
	TLS    TLS    `yaml:"tls"`    // server mTLS when split across hosts
}

// ReserveConfig bounds how long Reserve waits for a node's running event.
type ReserveConfig struct {
	ParkTimeout string `yaml:"park_timeout"` // default 30s; on timeout router → 503
}

// LinkDialConfig is how a role (node / router / scaler) reaches the registry: the
// endpoint it dials plus the client mTLS to present (when the endpoint is remote).
type LinkDialConfig struct {
	Endpoint string `yaml:"endpoint"` // registry listener to dial (UDS path / host:port)
	TLS      TLS    `yaml:"tls"`      // client mTLS for a remote endpoint (cluster.md §5.4)
}

// IngressConfig is the router's client-facing e2b ingress listener.
type IngressConfig struct {
	Listen string `yaml:"listen"` // e2b control+data ingress; default :443
	TLS    TLS    `yaml:"tls"`    // wildcard *.<domain> + api.<domain>
}

// RouterAuth groups the router's auth policy.
type RouterAuth struct {
	APIKey    string `yaml:"api_key"`    // caller api_key auth: off | log | enforce (default); §8
	DataPlane string `yaml:"data_plane"` // data-plane access-token check: off | log | enforce (default)
	CacheTTL  string `yaml:"cache_ttl"`  // api_key↔group verification cache; default 60s
}

// PlacementConfig groups the scaler's placement policy.
type PlacementConfig struct {
	Candidates      int           `yaml:"candidates"`       // P2C sample size; default 2
	ZoneAdmitMax    string        `yaml:"zone_admit_max"`   // exclude nodes hotter than this; default yellow
	NodeDeadAfter   string        `yaml:"node_dead_after"`  // exclude nodes silent longer than this; default 30s
	ShuffleSharding []ShuffleRule `yaml:"shuffle_sharding"` // empty = static nodeSelectors only
}

// ShuffleRule pins each matching group to n deterministic shards of the node set
// bucketed by a label (cluster-scaler.md §4.4).
type ShuffleRule struct {
	Selector map[string]string `yaml:"selector"` // label(s) a group's nodeSelectors and a node's labels must carry
	ShardBy  string            `yaml:"shard_by"` // node label whose distinct values are the shards
	N        int               `yaml:"n"`        // shards assigned per matching group
}

// TLS is mTLS material (cert/key + CA for peer verification).
type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
	CA   string `yaml:"ca"`
}

// Enabled reports whether a TLS server/client should be configured (cert + key).
func (t TLS) Enabled() bool { return t.Cert != "" && t.Key != "" }

// ServerConfig builds a server tls.Config; a CA enables mTLS (require + verify
// client certs). h2 is advertised so registry links and ingress negotiate HTTP/2.
func (t TLS) ServerConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(t.Cert, t.Key)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2", "http/1.1"}}
	if t.CA != "" {
		pool, err := caPool(t.CA)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = pool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg, nil
}

// ClientConfig builds a client tls.Config (client cert for mTLS + CA to verify the
// server); serverName sets the verification/SNI name when non-empty.
func (t TLS) ClientConfig(serverName string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}, ServerName: serverName}
	if t.Cert != "" && t.Key != "" {
		cert, err := tls.LoadX509KeyPair(t.Cert, t.Key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if t.CA != "" {
		pool, err := caPool(t.CA)
		if err != nil {
			return nil, err
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

func caPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("clustercfg: no certificates in %s", path)
	}
	return pool, nil
}

// Shared duration accessors (validated by the owning role's Validate).
func (c *NodeLinkConfig) HeartbeatDur() time.Duration {
	d, _ := time.ParseDuration(c.HeartbeatInterval)
	return d
}
func (c *NodeLinkConfig) NodeDeadDur() time.Duration {
	d, _ := time.ParseDuration(c.NodeDeadAfter)
	return d
}
func (c *ReserveConfig) ParkDur() time.Duration { d, _ := time.ParseDuration(c.ParkTimeout); return d }

// ProviderFor returns the configured provider spec for a fine-grained interface
// (defaulting to store), and whether it is external (with the address).
func (g *SandboxGroupConfig) ProviderFor(iface string) (spec string, external bool, addr string) {
	spec = g.Providers[iface]
	if spec == "" {
		spec = ProviderStore
	}
	if strings.HasPrefix(spec, ProviderExternalPfx) {
		return spec, true, strings.TrimPrefix(spec, ProviderExternalPfx)
	}
	return spec, false, ""
}

// validateDurations checks a name→value map parses as Go durations ("" skipped).
func validateDurations(m map[string]string) error {
	for name, d := range m {
		if d == "" {
			continue
		}
		if _, err := time.ParseDuration(d); err != nil {
			return fmt.Errorf("clustercfg: %s %q: %w", name, d, err)
		}
	}
	return nil
}

// validateProviders checks each sandbox_group provider is store|external:<addr>.
func validateProviders(p map[string]string) error {
	for iface, spec := range p {
		switch {
		case spec == ProviderStore:
		case strings.HasPrefix(spec, ProviderExternalPfx):
			if strings.TrimPrefix(spec, ProviderExternalPfx) == "" {
				return fmt.Errorf("clustercfg: sandbox_group.providers[%s]=%q has an empty external address", iface, spec)
			}
		default:
			return fmt.Errorf("clustercfg: sandbox_group.providers[%s]=%q invalid (store|external:<addr>)", iface, spec)
		}
	}
	return nil
}

// ===========================================================================
// registry.yaml — registry-owned state authority + node_link / route_link /
// scale_link hub (cluster.md §4.1).
// ===========================================================================

// RegistryConfig is the registry role's config: sandbox-group config providers,
// the node_link listener it holds, the route_link and scale_link listeners it
// binds, and the Reserve park budget.
type RegistryConfig struct {
	SandboxGroup SandboxGroupConfig `yaml:"sandbox_group"` // sandbox-group config providers
	NodeLink     NodeLinkConfig     `yaml:"node_link"`     // nodes dial this (registry holds it)
	RouteLink    LinkListenConfig   `yaml:"route_link"`    // routers/admin tools dial this
	ScaleLink    LinkListenConfig   `yaml:"scale_link"`    // scalers dial this
	Reserve      ReserveConfig      `yaml:"reserve"`       // Reserve park budget
}

// DefaultRegistry returns the registry config with all non-required fields set.
func DefaultRegistry() RegistryConfig {
	return RegistryConfig{
		SandboxGroup: SandboxGroupConfig{Providers: map[string]string{
			ProviderKey:           ProviderStore,
			ProviderSandboxConfig: ProviderStore,
			ProviderPlacement:     ProviderStore,
			ProviderImagePull:     ProviderStore,
		}},
		NodeLink:  NodeLinkConfig{Listen: ":7700", HeartbeatInterval: "10s", NodeDeadAfter: "30s", RevisionRetention: 10000},
		RouteLink: LinkListenConfig{Listen: defaultRouteLinkEndpoint},
		ScaleLink: LinkListenConfig{Listen: defaultScaleLinkEndpoint},
		Reserve:   ReserveConfig{ParkTimeout: "30s"},
	}
}

// LoadRegistry reads, defaults, and validates registry.yaml. The sandbox-group
// encryption_key falls back to the SANDBOX_GROUP_ENCRYPTION_KEY env when unset.
func LoadRegistry(path string) (*RegistryConfig, error) {
	c := DefaultRegistry()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("clustercfg: read %s: %w", path, err)
		}
		if err := yaml.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("clustercfg: parse %s: %w", path, err)
		}
		c.applyDefaults()
	}
	if c.SandboxGroup.EncryptionKey == "" {
		c.SandboxGroup.EncryptionKey = os.Getenv(envGroupEncryptionKey)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// applyDefaults re-fills fields a partial YAML left zero (yaml.Unmarshal into a
// defaulted struct overwrites whole sub-structs that appear, zeroing siblings).
func (c *RegistryConfig) applyDefaults() {
	d := DefaultRegistry()
	if c.SandboxGroup.Providers == nil {
		c.SandboxGroup.Providers = d.SandboxGroup.Providers
	} else {
		for _, k := range []string{ProviderKey, ProviderSandboxConfig, ProviderPlacement, ProviderImagePull} {
			if c.SandboxGroup.Providers[k] == "" {
				c.SandboxGroup.Providers[k] = ProviderStore
			}
		}
	}
	if c.NodeLink.Listen == "" {
		c.NodeLink.Listen = d.NodeLink.Listen
	}
	if c.NodeLink.HeartbeatInterval == "" {
		c.NodeLink.HeartbeatInterval = d.NodeLink.HeartbeatInterval
	}
	if c.NodeLink.NodeDeadAfter == "" {
		c.NodeLink.NodeDeadAfter = d.NodeLink.NodeDeadAfter
	}
	if c.NodeLink.RevisionRetention == 0 {
		c.NodeLink.RevisionRetention = d.NodeLink.RevisionRetention
	}
	if c.RouteLink.Listen == "" {
		c.RouteLink.Listen = d.RouteLink.Listen
	}
	if c.ScaleLink.Listen == "" {
		c.ScaleLink.Listen = d.ScaleLink.Listen
	}
	if c.Reserve.ParkTimeout == "" {
		c.Reserve.ParkTimeout = d.Reserve.ParkTimeout
	}
}

// Validate checks required fields and that durations / enums parse.
func (c *RegistryConfig) Validate() error {
	if err := validateProviders(c.SandboxGroup.Providers); err != nil {
		return err
	}
	return validateDurations(map[string]string{
		"node_link.heartbeat_interval": c.NodeLink.HeartbeatInterval,
		"node_link.node_dead_after":    c.NodeLink.NodeDeadAfter,
		"reserve.park_timeout":         c.Reserve.ParkTimeout,
	})
}

// ===========================================================================
// router.yaml — e2b-compatible unified ingress (cluster-router.md).
// ===========================================================================

// RouterConfig is the router role's config, grouped as upstream (registry) /
// downstream (ingress) / policy (auth) plus the service domain.
type RouterConfig struct {
	Domain        string         `yaml:"domain"`         // service domain; splits control/data (required)
	RouteLink     LinkDialConfig `yaml:"route_link"`     // upstream: registry route_link to dial
	Ingress       IngressConfig  `yaml:"ingress"`        // downstream: e2b client ingress
	Auth          RouterAuth     `yaml:"auth"`           // auth policy
	MetricsListen string         `yaml:"metrics_listen"` // optional Prometheus text endpoint
}

// DefaultRouter returns the router config with all non-required fields set.
func DefaultRouter() RouterConfig {
	return RouterConfig{
		RouteLink: LinkDialConfig{Endpoint: defaultRouteLinkEndpoint},
		Ingress:   IngressConfig{Listen: ":443"},
		Auth:      RouterAuth{APIKey: "enforce", DataPlane: "enforce", CacheTTL: "60s"},
	}
}

// LoadRouter reads, defaults, and validates router.yaml.
func LoadRouter(path string) (*RouterConfig, error) {
	c := DefaultRouter()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("clustercfg: read %s: %w", path, err)
		}
		if err := yaml.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("clustercfg: parse %s: %w", path, err)
		}
		c.applyDefaults()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *RouterConfig) applyDefaults() {
	d := DefaultRouter()
	if c.RouteLink.Endpoint == "" {
		c.RouteLink.Endpoint = d.RouteLink.Endpoint
	}
	if c.Ingress.Listen == "" {
		c.Ingress.Listen = d.Ingress.Listen
	}
	if c.Auth.APIKey == "" {
		c.Auth.APIKey = d.Auth.APIKey
	}
	if c.Auth.DataPlane == "" {
		c.Auth.DataPlane = d.Auth.DataPlane
	}
	if c.Auth.CacheTTL == "" {
		c.Auth.CacheTTL = d.Auth.CacheTTL
	}
}

func (c *RouterConfig) Validate() error {
	if c.Domain == "" {
		return fmt.Errorf("clustercfg: domain is required")
	}
	switch c.Auth.APIKey {
	case "", "off", "log", "enforce":
	default:
		return fmt.Errorf("clustercfg: auth.api_key %q invalid (off|log|enforce)", c.Auth.APIKey)
	}
	switch c.Auth.DataPlane {
	case "off", "log", "enforce":
	default:
		return fmt.Errorf("clustercfg: auth.data_plane %q invalid (off|log|enforce)", c.Auth.DataPlane)
	}
	return validateDurations(map[string]string{"auth.cache_ttl": c.Auth.CacheTTL})
}

func (c *RouterConfig) AuthCacheDur() time.Duration {
	d, _ := time.ParseDuration(c.Auth.CacheTTL)
	return d
}

// ===========================================================================
// scaler.yaml — placement scheduler (cluster-scaler.md).
// ===========================================================================

// ScalerConfig is the standalone scaler's config (cluster.md §1.2/§4.2 — always a
// separate process that dials registry scale_link and answers placement over that
// link): the upstream scale_link plus the placement policy.
type ScalerConfig struct {
	ScaleLink LinkDialConfig  `yaml:"scale_link"` // upstream: registry scale_link to dial
	Placement PlacementConfig `yaml:"placement"`  // placement policy
}

// DefaultScaler returns the scaler config with all non-required fields set.
func DefaultScaler() ScalerConfig {
	return ScalerConfig{
		ScaleLink: LinkDialConfig{Endpoint: defaultScaleLinkEndpoint},
		Placement: PlacementConfig{Candidates: 2, ZoneAdmitMax: "yellow", NodeDeadAfter: "30s"},
	}
}

// LoadScaler reads, defaults, and validates scaler.yaml.
func LoadScaler(path string) (*ScalerConfig, error) {
	c := DefaultScaler()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("clustercfg: read %s: %w", path, err)
		}
		if err := yaml.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("clustercfg: parse %s: %w", path, err)
		}
		c.applyDefaults()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *ScalerConfig) applyDefaults() {
	d := DefaultScaler()
	if c.ScaleLink.Endpoint == "" {
		c.ScaleLink.Endpoint = d.ScaleLink.Endpoint
	}
	if c.Placement.Candidates == 0 {
		c.Placement.Candidates = d.Placement.Candidates
	}
	if c.Placement.ZoneAdmitMax == "" {
		c.Placement.ZoneAdmitMax = d.Placement.ZoneAdmitMax
	}
	if c.Placement.NodeDeadAfter == "" {
		c.Placement.NodeDeadAfter = d.Placement.NodeDeadAfter
	}
}

func (c *ScalerConfig) Validate() error {
	switch c.Placement.ZoneAdmitMax {
	case "", "green", "yellow", "red":
	default:
		return fmt.Errorf("clustercfg: placement.zone_admit_max %q invalid (green|yellow|red)", c.Placement.ZoneAdmitMax)
	}
	if c.Placement.Candidates < 0 {
		return fmt.Errorf("clustercfg: placement.candidates %d invalid (must be >= 0)", c.Placement.Candidates)
	}
	return validateDurations(map[string]string{"placement.node_dead_after": c.Placement.NodeDeadAfter})
}

// NodeDeadDur is how long a node may be silent before the scaler excludes it.
func (c *ScalerConfig) NodeDeadDur() time.Duration {
	d, _ := time.ParseDuration(c.Placement.NodeDeadAfter)
	return d
}

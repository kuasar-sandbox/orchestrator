// Package clustercfg loads cluster-ctl's configuration (registry / router /
// scaler), grouped by concern per cluster.md §3. Only `domain` and `store` are
// required; everything else has a default. The three roles share one schema and
// one file — each role reads the groups it needs (registry: store/group_config/
// channel/op/reserve; router: router; scaler: scaler).
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

// Store backends. No memory mode: every backend is durable so a registry
// restart recovers from storage + node reconnect (cluster.md §1.2).
const (
	StoreSQLite = "sqlite" // single-host durable (Phase 0b; pure-Go)
	StoreEtcd   = "etcd"   // multi-replica (Phase 7)
	StoreRaft   = "raft"   // group-sharded multi-raft (Phase 7)
)

// GroupConfigProvider selection (cluster.md §6.2). Each fine-grained interface
// is independently "store" (registry self-stores) or "external:<addr>" (a
// cloud-provider service; manifest_key only passes through, never persisted).
const (
	ProviderStore        = "store"
	ProviderExternalPfx  = "external:" // external:<addr>
	envGroupEncryptionKey = "CLUSTER_GROUP_ENCRYPTION_KEY"
)

// Config is the grouped cluster-ctl configuration.
type Config struct {
	Domain      string            `yaml:"domain"`       // service domain; router splits control/data by it (required)
	Store       StoreConfig       `yaml:"store"`        // registry backend (required)
	GroupConfig GroupConfigConfig `yaml:"group_config"` // sandbox-group config providers
	Channel     ChannelConfig     `yaml:"channel"`      // node-link枢纽 (registry holds)
	Op          OpConfig          `yaml:"op"`           // async op / watch interface (router/scaler dial)
	Reserve     ReserveConfig     `yaml:"reserve"`      // Reserve park budget
	Router      RouterConfig      `yaml:"router"`       // router role (cluster-router.md §3)
	Scaler      ScalerConfig      `yaml:"scaler"`       // scaler role (cluster-scaler.md §3)
}

// StoreConfig selects the durable registry backend.
type StoreConfig struct {
	Kind string `yaml:"kind"` // sqlite (default) | etcd | raft
	DSN  string `yaml:"dsn"`  // sqlite path / etcd endpoints / raft config
}

// GroupConfigConfig selects, per fine-grained provider, store vs external, and
// the at-rest key for self-stored manifest_keys.
type GroupConfigConfig struct {
	// Providers maps each interface (key / sandbox_config / placement) to
	// "store" or "external:<addr>". Empty entries default to store.
	Providers     map[string]string `yaml:"providers"`
	EncryptionKey string            `yaml:"encryption_key"` // AES-256 at-rest for self-stored manifest_key; or CLUSTER_GROUP_ENCRYPTION_KEY env
}

// Provider interface names (keys of GroupConfigConfig.Providers).
const (
	ProviderKey           = "key"            // GroupKeyProvider (project_id + manifest_key)
	ProviderSandboxConfig = "sandbox_config" // GroupSandboxConfigProvider
	ProviderPlacement     = "placement"      // GroupPlacementProvider (nodeSelectors + shuffle labels)
)

// ChannelConfig is the per-node node-link hub the registry holds.
type ChannelConfig struct {
	Listen            string `yaml:"listen"`             // node dial target; default :7700
	TLS               TLS    `yaml:"tls"`                // mTLS (production)
	HeartbeatInterval string `yaml:"heartbeat_interval"` // default 10s
	NodeDeadAfter     string `yaml:"node_dead_after"`    // default 30s (§11 failure)
	RevisionRetention int    `yaml:"revision_retention"` // change-log depth for resume_from; default 10000
}

// OpConfig is the async op / watch interface router and scaler dial.
type OpConfig struct {
	Listen string `yaml:"listen"` // default /run/cluster/registry.sock (UDS); remote via TLS
	TLS    TLS    `yaml:"tls"`    // mTLS when split across hosts
}

// ReserveConfig bounds how long Reserve waits for a node's running event.
type ReserveConfig struct {
	ParkTimeout string `yaml:"park_timeout"` // default 30s; on timeout router → 503
}

// RouterConfig is the unified e2b ingress role.
type RouterConfig struct {
	Registry      string `yaml:"registry"`        // registry op endpoint (UDS / mTLS addr)
	Listen        string `yaml:"listen"`          // e2b control+data ingress; default :443
	TLS           TLS    `yaml:"tls"`             // wildcard *.<domain> + api.<domain>
	AuthCacheTTL  string `yaml:"auth_cache_ttl"`  // api_key↔group verification cache; default 60s
	DataPlaneAuth string `yaml:"data_plane_auth"` // off | log | enforce (default)
	MetricsListen string `yaml:"metrics_listen"`  // optional Prometheus text endpoint
}

// ScalerConfig is the placement scheduler role.
type ScalerConfig struct {
	Registry        string        `yaml:"registry"`         // registry op endpoint (the scaler dials this)
	Mode            string        `yaml:"mode"`             // inprocess (default) | remote
	Endpoint        string        `yaml:"endpoint"`         // registry -> scaler placement addr (mode=remote)
	Listen          string        `yaml:"listen"`           // scaler /scaler/place listener (mode=remote)
	TLS             TLS           `yaml:"tls"`              // mTLS for the registry <-> scaler placement hop
	PlaceCandidates int           `yaml:"place_candidates"` // P2C sample size; default 2
	ZoneAdmitMax    string        `yaml:"zone_admit_max"`   // exclude nodes hotter than this; default yellow
	ShuffleSharding []ShuffleRule `yaml:"shuffle_sharding"` // empty = static nodeSelectors only
}

// Scaler deployment modes (scaler.mode).
const (
	ScalerInprocess = "inprocess" // placement runs inside the registry process (default)
	ScalerRemote    = "remote"    // a standalone scaler process serves placement over op
)

// ShuffleRule pins each matching group to n deterministic shards of the node
// set bucketed by a label (cluster-scaler.md §4.4).
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
// client certs). h2 is advertised so node-link / op / ingress negotiate HTTP/2.
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

// ClientConfig builds a client tls.Config (client cert for mTLS + CA to verify
// the server); serverName sets the verification/SNI name when non-empty.
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

// Default returns the configuration with all non-required fields populated.
func Default() Config {
	return Config{
		Store: StoreConfig{Kind: StoreSQLite, DSN: "/var/lib/cluster/registry.db"},
		GroupConfig: GroupConfigConfig{Providers: map[string]string{
			ProviderKey:           ProviderStore,
			ProviderSandboxConfig: ProviderStore,
			ProviderPlacement:     ProviderStore,
		}},
		Channel: ChannelConfig{
			Listen:            ":7700",
			HeartbeatInterval: "10s",
			NodeDeadAfter:     "30s",
			RevisionRetention: 10000,
		},
		Op:      OpConfig{Listen: "/run/cluster/registry.sock"},
		Reserve: ReserveConfig{ParkTimeout: "30s"},
		Router: RouterConfig{
			Listen:        ":443",
			AuthCacheTTL:  "60s",
			DataPlaneAuth: "enforce",
		},
		Scaler: ScalerConfig{Mode: ScalerInprocess, PlaceCandidates: 2, ZoneAdmitMax: "yellow"},
	}
}

// Load reads, defaults, and validates the config at path. The group
// encryption_key falls back to the CLUSTER_GROUP_ENCRYPTION_KEY env when unset.
func Load(path string) (*Config, error) {
	c := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("clustercfg: read %s: %w", path, err)
		}
		if err := yaml.Unmarshal(raw, &c); err != nil {
			return nil, fmt.Errorf("clustercfg: parse %s: %w", path, err)
		}
		c.applyDefaults() // re-fill any group the YAML zeroed
	}
	if c.GroupConfig.EncryptionKey == "" {
		c.GroupConfig.EncryptionKey = os.Getenv(envGroupEncryptionKey)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// applyDefaults fills fields a partial YAML left zero (yaml.Unmarshal into a
// defaulted struct overwrites whole sub-structs that appear, zeroing siblings).
func (c *Config) applyDefaults() {
	d := Default()
	if c.Store.Kind == "" {
		c.Store.Kind = d.Store.Kind
	}
	if c.Store.DSN == "" {
		c.Store.DSN = d.Store.DSN
	}
	if c.GroupConfig.Providers == nil {
		c.GroupConfig.Providers = d.GroupConfig.Providers
	} else {
		for _, k := range []string{ProviderKey, ProviderSandboxConfig, ProviderPlacement} {
			if c.GroupConfig.Providers[k] == "" {
				c.GroupConfig.Providers[k] = ProviderStore
			}
		}
	}
	if c.Channel.Listen == "" {
		c.Channel.Listen = d.Channel.Listen
	}
	if c.Channel.HeartbeatInterval == "" {
		c.Channel.HeartbeatInterval = d.Channel.HeartbeatInterval
	}
	if c.Channel.NodeDeadAfter == "" {
		c.Channel.NodeDeadAfter = d.Channel.NodeDeadAfter
	}
	if c.Channel.RevisionRetention == 0 {
		c.Channel.RevisionRetention = d.Channel.RevisionRetention
	}
	if c.Op.Listen == "" {
		c.Op.Listen = d.Op.Listen
	}
	if c.Reserve.ParkTimeout == "" {
		c.Reserve.ParkTimeout = d.Reserve.ParkTimeout
	}
	if c.Router.Listen == "" {
		c.Router.Listen = d.Router.Listen
	}
	if c.Router.AuthCacheTTL == "" {
		c.Router.AuthCacheTTL = d.Router.AuthCacheTTL
	}
	if c.Router.DataPlaneAuth == "" {
		c.Router.DataPlaneAuth = d.Router.DataPlaneAuth
	}
	if c.Scaler.PlaceCandidates == 0 {
		c.Scaler.PlaceCandidates = d.Scaler.PlaceCandidates
	}
	if c.Scaler.Mode == "" {
		c.Scaler.Mode = d.Scaler.Mode
	}
	if c.Scaler.ZoneAdmitMax == "" {
		c.Scaler.ZoneAdmitMax = d.Scaler.ZoneAdmitMax
	}
}

// Validate checks required fields and that durations / enums parse.
func (c *Config) Validate() error {
	if c.Domain == "" {
		return fmt.Errorf("clustercfg: domain is required")
	}
	switch c.Store.Kind {
	case StoreSQLite, StoreEtcd, StoreRaft:
	default:
		return fmt.Errorf("clustercfg: store.kind %q invalid (sqlite|etcd|raft)", c.Store.Kind)
	}
	if c.Store.DSN == "" {
		return fmt.Errorf("clustercfg: store.dsn is required")
	}
	for iface, p := range c.GroupConfig.Providers {
		switch {
		case p == ProviderStore:
		case strings.HasPrefix(p, ProviderExternalPfx):
			if strings.TrimPrefix(p, ProviderExternalPfx) == "" {
				return fmt.Errorf("clustercfg: group_config.providers[%s]=%q has an empty external address", iface, p)
			}
		default:
			return fmt.Errorf("clustercfg: group_config.providers[%s]=%q invalid (store|external:<addr>)", iface, p)
		}
	}
	for name, d := range map[string]string{
		"channel.heartbeat_interval": c.Channel.HeartbeatInterval,
		"channel.node_dead_after":    c.Channel.NodeDeadAfter,
		"reserve.park_timeout":       c.Reserve.ParkTimeout,
		"router.auth_cache_ttl":      c.Router.AuthCacheTTL,
	} {
		if _, err := time.ParseDuration(d); err != nil {
			return fmt.Errorf("clustercfg: %s %q: %w", name, d, err)
		}
	}
	switch c.Router.DataPlaneAuth {
	case "off", "log", "enforce":
	default:
		return fmt.Errorf("clustercfg: router.data_plane_auth %q invalid (off|log|enforce)", c.Router.DataPlaneAuth)
	}
	switch c.Scaler.ZoneAdmitMax {
	case "", "green", "yellow", "red":
	default:
		return fmt.Errorf("clustercfg: scaler.zone_admit_max %q invalid (green|yellow|red)", c.Scaler.ZoneAdmitMax)
	}
	if c.Scaler.PlaceCandidates < 0 {
		return fmt.Errorf("clustercfg: scaler.place_candidates %d invalid (must be >= 0)", c.Scaler.PlaceCandidates)
	}
	switch c.Scaler.Mode {
	case "", ScalerInprocess, ScalerRemote:
	default:
		return fmt.Errorf("clustercfg: scaler.mode %q invalid (inprocess|remote)", c.Scaler.Mode)
	}
	if c.Scaler.Mode == ScalerRemote && c.Scaler.Endpoint == "" {
		return fmt.Errorf("clustercfg: scaler.endpoint required when scaler.mode=remote")
	}
	return nil
}

// Duration accessors (validated by Validate, so the parse can't fail here).

func (c *ChannelConfig) HeartbeatDur() time.Duration { d, _ := time.ParseDuration(c.HeartbeatInterval); return d }
func (c *ChannelConfig) NodeDeadDur() time.Duration  { d, _ := time.ParseDuration(c.NodeDeadAfter); return d }
func (c *ReserveConfig) ParkDur() time.Duration      { d, _ := time.ParseDuration(c.ParkTimeout); return d }
func (c *RouterConfig) AuthCacheDur() time.Duration  { d, _ := time.ParseDuration(c.AuthCacheTTL); return d }

// ProviderFor returns the configured provider spec for a fine-grained interface
// (defaulting to store), and whether it is external (with the address).
func (g *GroupConfigConfig) ProviderFor(iface string) (spec string, external bool, addr string) {
	spec = g.Providers[iface]
	if spec == "" {
		spec = ProviderStore
	}
	if strings.HasPrefix(spec, ProviderExternalPfx) {
		return spec, true, strings.TrimPrefix(spec, ProviderExternalPfx)
	}
	return spec, false, ""
}

// Package clustercfg loads cluster-ctl's configuration. Each role runs as its own
// process with its OWN config file and schema (cluster.md §3) — there is no shared
// file: registry.yaml / router.yaml / scaler.yaml each carry only what that role
// needs, grouped by the cluster link they operate: node_link, route_link,
// scale_link, and node_list.
package clustercfg

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultRegistryBootstrap = "127.0.0.1:7700"
)

// ===========================================================================
// Shared sub-types (reused across the three role schemas).
// ===========================================================================

// MemberConfig is a registry member's unified control-plane endpoint. node_link,
// route_link, scale_link, and member RPC are path namespaces on this listener
// unless node_link.listen is explicitly split out.
type MemberConfig struct {
	ID        string `yaml:"id"`
	Listen    string `yaml:"listen"`    // unified control-plane listener
	Advertise string `yaml:"advertise"` // address peers/clients use to reach this member
	TLS       TLS    `yaml:"tls"`       // server mTLS for the unified listener
}

// MembershipConfig is the versioned registry member view. The active version is
// the client routing input. When next is set, registry writes use joint owner
// sets: active quorum and next quorum must both commit before the write returns.
// old_grace keeps a previous version reachable for peer/node-owner RPC without
// adding it to owner quorums.
type MembershipConfig struct {
	Active   int64                 `yaml:"active" json:"active"`
	Next     int64                 `yaml:"next,omitempty" json:"next,omitempty"`
	OldGrace int64                 `yaml:"old_grace,omitempty" json:"old_grace,omitempty"`
	Versions []MembershipVersion   `yaml:"versions" json:"versions"`
	Owners   MembershipOwnerConfig `yaml:"owners" json:"owners"`
}

type MembershipVersion struct {
	Version int64              `yaml:"version" json:"version"`
	Label   string             `yaml:"label,omitempty" json:"label,omitempty"`
	Members []MembershipMember `yaml:"members" json:"members"`
}

type MembershipMember struct {
	ID        string `yaml:"id" json:"id"`
	Advertise string `yaml:"advertise" json:"advertise"`
}

type MembershipOwnerConfig struct {
	RouteLink int `yaml:"route_link" json:"route_link"`
	NodeLink  int `yaml:"node_link" json:"node_link"`
	NodeList  int `yaml:"node_list" json:"node_list"`
}

// NodeLinkConfig configures node_link behavior. listen/advertise are optional
// production split points for node long-lived streams; empty means reuse member.
type NodeLinkConfig struct {
	Listen            string `yaml:"listen"`
	Advertise         string `yaml:"advertise"`
	TLS               TLS    `yaml:"tls"`
	HeartbeatInterval string `yaml:"heartbeat_interval"` // default 10s
	NodeDeadAfter     string `yaml:"node_dead_after"`    // default 30s
	RevisionRetention int    `yaml:"revision_retention"` // change-log depth for resume_from; default 10000
}

// RouteLinkConfig configures route_link behavior. The HTTP listener is member.listen.
type RouteLinkConfig struct {
	ParkTimeout string `yaml:"park_timeout"` // default 30s; on timeout router -> 503
}

type NodeListConfig struct {
	WatchRetention int `yaml:"watch_retention"`
}

// ScaleLinkConfig configures scaler discovery/placement over the unified member listener.
type ScaleLinkConfig struct {
	ScalerReplicaCount int    `yaml:"scaler_replica_count"`
	MinReadyScalers    int    `yaml:"min_ready_scalers"`
	ScalerLabel        string `yaml:"scaler_label"`
	PlaceTimeout       string `yaml:"place_timeout"`
}

type ScalerMemberlistConfig struct {
	Label string `yaml:"label"`
}

// RegistryDialConfig is how a consumer reaches the registry bootstrap endpoint.
// Consumers fetch /cluster/membership first, then route group/node operations to
// the owner set from that versioned membership.
type RegistryDialConfig struct {
	Bootstrap string `yaml:"bootstrap"`
	TLS       TLS    `yaml:"tls"`
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

// RouterCache configures local route resolution and active-route cache retention.
type RouterCache struct {
	RouteTTL    string `yaml:"route_ttl"`    // route resolution max age; default 5m
	IdleTimeout string `yaml:"idle_timeout"` // route resolution idle age; default 2m
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
func (c *RouteLinkConfig) ParkDur() time.Duration {
	d, _ := time.ParseDuration(c.ParkTimeout)
	return d
}
func (c *ScaleLinkConfig) PlaceDur() time.Duration {
	d, _ := time.ParseDuration(c.PlaceTimeout)
	return d
}

// ActiveVersion returns the configured active membership version.
func (m MembershipConfig) ActiveVersion() (MembershipVersion, bool) {
	for _, v := range m.Versions {
		if v.Version == m.Active {
			return v.WithComputedLabel(), true
		}
	}
	return MembershipVersion{}, false
}

func (m MembershipConfig) NextVersion() (MembershipVersion, bool) {
	if m.Next == 0 || m.Next == m.Active {
		return MembershipVersion{}, false
	}
	for _, v := range m.Versions {
		if v.Version == m.Next {
			return v.WithComputedLabel(), true
		}
	}
	return MembershipVersion{}, false
}

func (m MembershipConfig) OldGraceVersion() (MembershipVersion, bool) {
	if m.OldGrace == 0 || m.OldGrace == m.Active || m.OldGrace == m.Next {
		return MembershipVersion{}, false
	}
	for _, v := range m.Versions {
		if v.Version == m.OldGrace {
			return v.WithComputedLabel(), true
		}
	}
	return MembershipVersion{}, false
}

func (m MembershipConfig) OwnerVersions() []MembershipVersion {
	out := make([]MembershipVersion, 0, 2)
	if active, ok := m.ActiveVersion(); ok {
		out = append(out, active)
	}
	if next, ok := m.NextVersion(); ok {
		out = append(out, next)
	}
	return out
}

func (m MembershipConfig) MemberVersions() []MembershipVersion {
	seen := map[int64]bool{}
	out := make([]MembershipVersion, 0, len(m.Versions))
	for _, v := range m.OwnerVersions() {
		if seen[v.Version] {
			continue
		}
		seen[v.Version] = true
		out = append(out, v)
	}
	if oldGrace, ok := m.OldGraceVersion(); ok && !seen[oldGrace.Version] {
		out = append(out, oldGrace)
	}
	return out
}

// WithComputedLabels returns a copy whose version labels are stable and explicit.
func (m MembershipConfig) WithComputedLabels() MembershipConfig {
	out := m
	out.Versions = append([]MembershipVersion(nil), m.Versions...)
	for i := range out.Versions {
		out.Versions[i] = out.Versions[i].WithComputedLabel()
	}
	return out
}

// WithComputedLabel returns a copy with label set to
// registry.<version>.<sha256(sort(member ids))>. The label is used to isolate
// scaler ready state and watch tokens from different membership versions.
func (v MembershipVersion) WithComputedLabel() MembershipVersion {
	if v.Label != "" {
		return v
	}
	ids := make([]string, 0, len(v.Members))
	for _, m := range v.Members {
		if m.ID != "" {
			ids = append(ids, m.ID)
		}
	}
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\n")))
	v.Label = fmt.Sprintf("registry.%d.%s", v.Version, hex.EncodeToString(sum[:])[:16])
	return v
}

// MemberIDs returns the sorted member ids in this membership version.
func (v MembershipVersion) MemberIDs() []string {
	out := make([]string, 0, len(v.Members))
	for _, m := range v.Members {
		if m.ID != "" {
			out = append(out, m.ID)
		}
	}
	sort.Strings(out)
	return out
}

// validateDurations checks a name→value map parses as positive Go durations
// ("" skipped).
func validateDurations(m map[string]string) error {
	for name, d := range m {
		if d == "" {
			continue
		}
		parsed, err := time.ParseDuration(d)
		if err != nil {
			return fmt.Errorf("clustercfg: %s %q: %w", name, d, err)
		}
		if parsed <= 0 {
			return fmt.Errorf("clustercfg: %s must be positive", name)
		}
	}
	return nil
}

// ===========================================================================
// registry.yaml — registry member unified control plane (cluster.md §2).
// ===========================================================================

// RegistryConfig is the registry role's config. member.listen is the default
// listener for node_link, route_link, scale_link, and member RPC. node_link.listen
// may split long-lived node streams onto another listener.
type RegistryConfig struct {
	Member     MemberConfig     `yaml:"member"`
	Membership MembershipConfig `yaml:"membership"`
	NodeLink   NodeLinkConfig   `yaml:"node_link"`
	RouteLink  RouteLinkConfig  `yaml:"route_link"`
	NodeList   NodeListConfig   `yaml:"node_list"`
	ScaleLink  ScaleLinkConfig  `yaml:"scale_link"`
}

// DefaultRegistry returns the registry config with all non-required fields set.
func DefaultRegistry() RegistryConfig {
	return RegistryConfig{
		Member: MemberConfig{ID: "registry", Listen: ":7700"},
		Membership: MembershipConfig{
			Active: 1,
			Versions: []MembershipVersion{{
				Version: 1,
				Members: []MembershipMember{{ID: "registry"}},
			}},
			Owners: MembershipOwnerConfig{RouteLink: 1, NodeLink: 1, NodeList: 1},
		},
		NodeLink:  NodeLinkConfig{HeartbeatInterval: "10s", NodeDeadAfter: "30s", RevisionRetention: 10000},
		RouteLink: RouteLinkConfig{ParkTimeout: "30s"},
		NodeList:  NodeListConfig{WatchRetention: 10000},
		ScaleLink: ScaleLinkConfig{ScalerReplicaCount: 3, MinReadyScalers: 1, ScalerLabel: "scaler.default", PlaceTimeout: "2s"},
	}
}

// LoadRegistry reads, defaults, and validates registry.yaml.
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
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// applyDefaults re-fills fields a partial YAML left zero (yaml.Unmarshal into a
// defaulted struct overwrites whole sub-structs that appear, zeroing siblings).
func (c *RegistryConfig) applyDefaults() {
	d := DefaultRegistry()
	if c.Member.ID == "" {
		c.Member.ID = d.Member.ID
	}
	if c.Member.Listen == "" {
		c.Member.Listen = d.Member.Listen
	}
	if c.Member.Advertise == "" {
		c.Member.Advertise = c.Member.Listen
	}
	if c.Membership.Active == 0 {
		c.Membership.Active = d.Membership.Active
	}
	if len(c.Membership.Versions) == 0 {
		c.Membership.Versions = d.Membership.Versions
	}
	for vi := range c.Membership.Versions {
		for mi := range c.Membership.Versions[vi].Members {
			if c.Membership.Versions[vi].Members[mi].ID == c.Member.ID &&
				c.Membership.Versions[vi].Members[mi].Advertise == "" {
				c.Membership.Versions[vi].Members[mi].Advertise = c.ControlAdvertise()
			}
		}
		c.Membership.Versions[vi] = c.Membership.Versions[vi].WithComputedLabel()
	}
	if c.Membership.Owners.RouteLink == 0 {
		c.Membership.Owners.RouteLink = d.Membership.Owners.RouteLink
	}
	if c.Membership.Owners.NodeLink == 0 {
		c.Membership.Owners.NodeLink = d.Membership.Owners.NodeLink
	}
	if c.Membership.Owners.NodeList == 0 {
		c.Membership.Owners.NodeList = d.Membership.Owners.NodeList
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
	if c.RouteLink.ParkTimeout == "" {
		c.RouteLink.ParkTimeout = d.RouteLink.ParkTimeout
	}
	if c.NodeList.WatchRetention == 0 {
		c.NodeList.WatchRetention = d.NodeList.WatchRetention
	}
	if c.ScaleLink.ScalerReplicaCount == 0 {
		c.ScaleLink.ScalerReplicaCount = d.ScaleLink.ScalerReplicaCount
	}
	if c.ScaleLink.MinReadyScalers == 0 {
		c.ScaleLink.MinReadyScalers = d.ScaleLink.MinReadyScalers
	}
	if c.ScaleLink.ScalerLabel == "" {
		c.ScaleLink.ScalerLabel = d.ScaleLink.ScalerLabel
	}
	if c.ScaleLink.PlaceTimeout == "" {
		c.ScaleLink.PlaceTimeout = d.ScaleLink.PlaceTimeout
	}
}

// Validate checks required fields and that durations / enums parse.
func (c *RegistryConfig) Validate() error {
	if c.Member.ID == "" {
		return fmt.Errorf("clustercfg: member.id is required")
	}
	if c.Member.Listen == "" {
		return fmt.Errorf("clustercfg: member.listen is required")
	}
	if c.Membership.Active <= 0 {
		return fmt.Errorf("clustercfg: membership.active must be positive")
	}
	if c.Membership.Next < 0 {
		return fmt.Errorf("clustercfg: membership.next must not be negative")
	}
	if c.Membership.OldGrace < 0 {
		return fmt.Errorf("clustercfg: membership.old_grace must not be negative")
	}
	if len(c.Membership.Versions) == 0 {
		return fmt.Errorf("clustercfg: membership.versions must not be empty")
	}
	activeFound := false
	nextFound := c.Membership.Next == 0 || c.Membership.Next == c.Membership.Active
	oldGraceFound := c.Membership.OldGrace == 0 || c.Membership.OldGrace == c.Membership.Active || c.Membership.OldGrace == c.Membership.Next
	selfInServingVersion := false
	for _, v := range c.Membership.Versions {
		if v.Version == c.Membership.Active {
			activeFound = true
		}
		if v.Version == c.Membership.Next {
			nextFound = true
		}
		if v.Version == c.Membership.OldGrace {
			oldGraceFound = true
		}
		if v.Version <= 0 {
			return fmt.Errorf("clustercfg: membership version must be positive")
		}
		if len(v.Members) == 0 {
			return fmt.Errorf("clustercfg: membership version %d has no members", v.Version)
		}
		if v.Version == c.Membership.Active ||
			(c.Membership.Next != 0 && v.Version == c.Membership.Next) ||
			(c.Membership.OldGrace != 0 && v.Version == c.Membership.OldGrace) {
			for _, member := range v.Members {
				if member.ID == c.Member.ID {
					selfInServingVersion = true
				}
			}
		}
	}
	if !activeFound {
		return fmt.Errorf("clustercfg: membership.active %d is not in membership.versions", c.Membership.Active)
	}
	if !nextFound {
		return fmt.Errorf("clustercfg: membership.next %d is not in membership.versions", c.Membership.Next)
	}
	if !oldGraceFound {
		return fmt.Errorf("clustercfg: membership.old_grace %d is not in membership.versions", c.Membership.OldGrace)
	}
	if !selfInServingVersion {
		return fmt.Errorf("clustercfg: member.id %q is not in active, next, or old_grace membership", c.Member.ID)
	}
	if c.Membership.Owners.RouteLink <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.route_link must be positive")
	}
	if c.Membership.Owners.NodeLink <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.node_link must be positive")
	}
	if c.Membership.Owners.NodeList <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.node_list must be positive")
	}
	if c.NodeLink.RevisionRetention <= 0 {
		return fmt.Errorf("clustercfg: node_link.revision_retention must be positive")
	}
	if c.NodeList.WatchRetention <= 0 {
		return fmt.Errorf("clustercfg: node_list.watch_retention must be positive")
	}
	if c.ScaleLink.ScalerReplicaCount <= 0 {
		return fmt.Errorf("clustercfg: scale_link.scaler_replica_count must be positive")
	}
	if c.ScaleLink.MinReadyScalers <= 0 {
		return fmt.Errorf("clustercfg: scale_link.min_ready_scalers must be positive")
	}
	if c.ScaleLink.MinReadyScalers > c.ScaleLink.ScalerReplicaCount {
		return fmt.Errorf("clustercfg: scale_link.min_ready_scalers must not exceed scaler_replica_count")
	}
	if c.ScaleLink.ScalerLabel == "" {
		return fmt.Errorf("clustercfg: scale_link.scaler_label is required")
	}
	return validateDurations(map[string]string{
		"node_link.heartbeat_interval": c.NodeLink.HeartbeatInterval,
		"node_link.node_dead_after":    c.NodeLink.NodeDeadAfter,
		"route_link.park_timeout":      c.RouteLink.ParkTimeout,
		"scale_link.place_timeout":     c.ScaleLink.PlaceTimeout,
	})
}

// ControlListen is the unified registry listener.
func (c *RegistryConfig) ControlListen() string { return c.Member.Listen }

// ControlAdvertise is the address other components should use for this member.
func (c *RegistryConfig) ControlAdvertise() string {
	if c.Member.Advertise != "" {
		return c.Member.Advertise
	}
	return c.Member.Listen
}

// NodeListen is the node_link listener; empty node_link.listen means reuse the
// unified control listener.
func (c *RegistryConfig) NodeListen() string {
	if c.NodeLink.Listen != "" {
		return c.NodeLink.Listen
	}
	return c.Member.Listen
}

// NodeLinkSplit reports whether node_link should bind a separate listener.
func (c *RegistryConfig) NodeLinkSplit() bool {
	return c.NodeLink.Listen != "" && c.NodeLink.Listen != c.Member.Listen
}

// ===========================================================================
// router.yaml — e2b-compatible unified ingress (cluster-router.md).
// ===========================================================================

// RouterConfig is the router role's config, grouped as upstream (registry) /
// downstream (ingress) / policy (auth) plus the service domain.
type RouterConfig struct {
	Domain        string             `yaml:"domain"`         // service domain; splits control/data (required)
	Registry      RegistryDialConfig `yaml:"registry"`       // upstream: registry bootstrap/membership
	Ingress       IngressConfig      `yaml:"ingress"`        // downstream: e2b client ingress
	Auth          RouterAuth         `yaml:"auth"`           // auth policy
	Cache         RouterCache        `yaml:"cache"`          // local route cache policy
	MetricsListen string             `yaml:"metrics_listen"` // optional Prometheus text endpoint
}

// DefaultRouter returns the router config with all non-required fields set.
func DefaultRouter() RouterConfig {
	return RouterConfig{
		Registry: RegistryDialConfig{Bootstrap: defaultRegistryBootstrap},
		Ingress:  IngressConfig{Listen: ":443"},
		Auth:     RouterAuth{APIKey: "enforce", DataPlane: "enforce", CacheTTL: "60s"},
		Cache:    RouterCache{RouteTTL: "5m", IdleTimeout: "2m"},
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
	if c.Registry.Bootstrap == "" {
		c.Registry.Bootstrap = d.Registry.Bootstrap
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
	if c.Cache.RouteTTL == "" {
		c.Cache.RouteTTL = d.Cache.RouteTTL
	}
	if c.Cache.IdleTimeout == "" {
		c.Cache.IdleTimeout = d.Cache.IdleTimeout
	}
}

func (c *RouterConfig) Validate() error {
	if c.Domain == "" {
		return fmt.Errorf("clustercfg: domain is required")
	}
	if c.Registry.Bootstrap == "" {
		return fmt.Errorf("clustercfg: registry.bootstrap is required")
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
	return validateDurations(map[string]string{
		"auth.cache_ttl":     c.Auth.CacheTTL,
		"cache.route_ttl":    c.Cache.RouteTTL,
		"cache.idle_timeout": c.Cache.IdleTimeout,
	})
}

func (c *RouterConfig) AuthCacheDur() time.Duration {
	d, _ := time.ParseDuration(c.Auth.CacheTTL)
	return d
}

func (c *RouterConfig) RouteCacheDur() time.Duration {
	d, _ := time.ParseDuration(c.Cache.RouteTTL)
	return d
}

func (c *RouterConfig) RouteIdleDur() time.Duration {
	d, _ := time.ParseDuration(c.Cache.IdleTimeout)
	return d
}

// ===========================================================================
// scaler.yaml — placement scheduler (cluster-scaler.md).
// ===========================================================================

// ScalerConfig is the standalone scaler's config. It discovers registry
// membership through the bootstrap endpoint, pushes itself to scale_link, and
// answers placement calls from registry route owners.
type ScalerConfig struct {
	Member     MemberConfig           `yaml:"member"`
	Memberlist ScalerMemberlistConfig `yaml:"memberlist"`
	Registry   RegistryDialConfig     `yaml:"registry"`  // upstream: registry bootstrap/membership
	Placement  PlacementConfig        `yaml:"placement"` // placement policy
}

// DefaultScaler returns the scaler config with all non-required fields set.
func DefaultScaler() ScalerConfig {
	return ScalerConfig{
		Member:     MemberConfig{ID: "scaler", Listen: ":7800"},
		Memberlist: ScalerMemberlistConfig{Label: "scaler.default"},
		Registry:   RegistryDialConfig{Bootstrap: defaultRegistryBootstrap},
		Placement:  PlacementConfig{Candidates: 2, ZoneAdmitMax: "yellow", NodeDeadAfter: "30s"},
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
	if c.Member.ID == "" {
		c.Member.ID = d.Member.ID
	}
	if c.Member.Listen == "" {
		c.Member.Listen = d.Member.Listen
	}
	if c.Member.Advertise == "" {
		c.Member.Advertise = c.Member.Listen
	}
	if c.Memberlist.Label == "" {
		c.Memberlist.Label = d.Memberlist.Label
	}
	if c.Registry.Bootstrap == "" {
		c.Registry.Bootstrap = d.Registry.Bootstrap
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
	if c.Member.ID == "" {
		return fmt.Errorf("clustercfg: member.id is required")
	}
	if c.Member.Listen == "" {
		return fmt.Errorf("clustercfg: member.listen is required")
	}
	if c.Registry.Bootstrap == "" {
		return fmt.Errorf("clustercfg: registry.bootstrap is required")
	}
	if c.Memberlist.Label == "" {
		return fmt.Errorf("clustercfg: memberlist.label is required")
	}
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

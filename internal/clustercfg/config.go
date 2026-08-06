// Package clustercfg loads cluster-ctl's configuration. Each role runs as its own
// process with its OWN config file and schema; there is no shared
// file: registry.yaml / router.yaml / placer.yaml each carry only what that role
// needs, grouped by the cluster link they operate: node_link, route_link,
// placer_link, and node_list.
package clustercfg

import (
	"bytes"
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

// strictUnmarshalYAML decodes raw into out, rejecting any field not present in
// out's struct tags. Plain yaml.Unmarshal silently ignores unknown fields, so
// a typo'd or stale key (e.g. in placer_link.placer_label) would otherwise
// leave the intended setting at its zero-value default without any
// indication the operator's value was never applied.
func strictUnmarshalYAML(raw []byte, out any) error {
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	return dec.Decode(out)
}

const (
	defaultRegistryBootstrap = "127.0.0.1:7700"
)

// ===========================================================================
// Shared sub-types (reused across role schemas).
// ===========================================================================

// MemberConfig identifies a registry process and its local unified control-plane
// listener. The externally reachable registry address is versioned in
// membership.versions[].members[].advertise, not duplicated here.
type MemberConfig struct {
	ID     string `yaml:"id"`
	Listen string `yaml:"listen"` // unified control-plane listener
	TLS    TLS    `yaml:"tls"`    // server mTLS for the unified listener
}

// MembershipConfig is the versioned registry member view. The active version is
// the client routing input. When next is set, registry writes use joint owner
// sets: active quorum and next quorum must both commit before the write returns.
// old_grace keeps a previous version reachable for peer/node-owner RPC without
// adding it to owner quorums.
type MembershipConfig struct {
	Active             int64                 `yaml:"active" json:"active"`
	Next               int64                 `yaml:"next,omitempty" json:"next,omitempty"`
	OldGrace           int64                 `yaml:"old_grace,omitempty" json:"old_grace,omitempty"`
	ReloadReadyTimeout string                `yaml:"reload_ready_timeout,omitempty" json:"reload_ready_timeout,omitempty"`
	Versions           []MembershipVersion   `yaml:"versions" json:"versions"`
	Owners             MembershipOwnerConfig `yaml:"owners" json:"owners"`
}

type MembershipVersion struct {
	Version int64              `yaml:"version" json:"version"`
	Label   string             `yaml:"label,omitempty" json:"label,omitempty"`
	Members []MembershipMember `yaml:"members" json:"members"`
}

type MembershipMember struct {
	ID            string `yaml:"id" json:"id"`
	Advertise     string `yaml:"advertise" json:"advertise"`
	NodeAdvertise string `yaml:"node_advertise,omitempty" json:"node_advertise,omitempty"`
}

type MembershipOwnerConfig struct {
	RouteLink  int `yaml:"route_link" json:"route_link"`
	NodeLink   int `yaml:"node_link" json:"node_link"`
	PlacerLink int `yaml:"placer_link" json:"placer_link"`
	NodeList   int `yaml:"node_list" json:"node_list"`
}

// NodeLinkConfig configures node_link behavior. listen is the optional production
// split point for node long-lived streams; empty means reuse member.listen.
type NodeLinkConfig struct {
	Listen            string `yaml:"listen"`
	TLS               TLS    `yaml:"tls"`
	HeartbeatInterval string `yaml:"heartbeat_interval"` // default 10s
	NodeDeadAfter     string `yaml:"node_dead_after"`    // default 30s
}

// RouteLinkConfig configures route_link behavior. The HTTP listener is member.listen.
type RouteLinkConfig struct {
	ParkTimeout string `yaml:"park_timeout"` // default 30s; on timeout router -> 503
}

type NodeListConfig struct {
	WatchRetention int `yaml:"watch_retention"`
}

// PlacerLinkConfig configures placer discovery/placement over the unified member listener.
type PlacerLinkConfig struct {
	PlacerReplicaCount int    `yaml:"placer_replica_count"`
	MinReadyPlacers    int    `yaml:"min_ready_placers"`
	PlacerLabel        string `yaml:"placer_label"`
	PlaceTimeout       string `yaml:"place_timeout"`
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
	DataPlane string `yaml:"data_plane"` // data-plane access-token check: off | log | enforce (default)
	CacheTTL  string `yaml:"cache_ttl"`  // api_key↔group verification cache; default 60s
}

// RouterCache configures local route resolution and active-route cache retention.
type RouterCache struct {
	RouteTTL    string `yaml:"route_ttl"`    // route resolution max age; default 5m
	IdleTimeout string `yaml:"idle_timeout"` // route resolution idle age; default 2m
}

// PlacementConfig groups the placer's placement policy.
type PlacementConfig struct {
	Candidates             int           `yaml:"candidates"`                // P2C sample size; default 2
	ZoneAdmitMax           string        `yaml:"zone_admit_max"`            // exclude nodes hotter than this; default yellow
	ImportSourceOwnerCount int           `yaml:"import_source_owner_count"` // placer candidates that may race for one source lease
	ImportSourceLeaseTTL   string        `yaml:"import_source_lease_ttl"`   // registry-side source lease TTL
	SelectorPatchRefresh   string        `yaml:"selector_patch_refresh_interval"`
	ShuffleSharding        []ShuffleRule `yaml:"shuffle_sharding"` // empty = static nodeSelectors only
}

type GroupSourceConfig struct {
	SourceID   string `yaml:"source_id"`
	SourceType string `yaml:"source_type"`
	Path       string `yaml:"path,omitempty"`
}

// PlacerProcessConfig is the standalone placer's own control plane. The same
// advertise address is used for placer API calls and placer memberlist HTTP
// transport.
type PlacerProcessConfig struct {
	ID              string `yaml:"id"`
	Listen          string `yaml:"listen"`
	Advertise       string `yaml:"advertise"`
	MemberlistLabel string `yaml:"memberlist_label"`
	TLS             TLS    `yaml:"tls"`
}

// ShuffleRule pins each matching group to n deterministic shards of the node set
// bucketed by a node label.
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
func (c *PlacerLinkConfig) PlaceDur() time.Duration {
	d, _ := time.ParseDuration(c.PlaceTimeout)
	return d
}
func (c *PlacementConfig) ImportSourceLeaseTTLDur() time.Duration {
	d, _ := time.ParseDuration(c.ImportSourceLeaseTTL)
	return d
}
func (c *PlacementConfig) SelectorPatchRefreshDur() time.Duration {
	d, _ := time.ParseDuration(c.SelectorPatchRefresh)
	return d
}
func (m MembershipConfig) ReloadReadyTimeoutDur() time.Duration {
	d, _ := time.ParseDuration(m.ReloadReadyTimeout)
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

func (m MembershipConfig) ShardVersions() []MembershipVersion {
	seen := map[int64]bool{}
	out := make([]MembershipVersion, 0, 3)
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
// placer ready state and watch tokens from different membership versions.
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
// registry.yaml — registry member unified control plane.
// ===========================================================================

// RegistryConfig is the registry role's config. member.listen is the default
// listener for node_link, route_link, placer_link, and member RPC. node_link.listen
// may split long-lived node streams onto another listener.
type RegistryConfig struct {
	Member     MemberConfig     `yaml:"member"`
	Membership MembershipConfig `yaml:"membership"`
	NodeLink   NodeLinkConfig   `yaml:"node_link"`
	RouteLink  RouteLinkConfig  `yaml:"route_link"`
	NodeList   NodeListConfig   `yaml:"node_list"`
	PlacerLink PlacerLinkConfig `yaml:"placer_link"`
}

// DefaultRegistry returns the registry config with all non-required fields set.
func DefaultRegistry() RegistryConfig {
	return RegistryConfig{
		Member: MemberConfig{ID: "registry", Listen: ":7700"},
		Membership: MembershipConfig{
			Active: 1, ReloadReadyTimeout: "10s",
			Versions: []MembershipVersion{{
				Version: 1,
				Members: []MembershipMember{{ID: "registry", Advertise: defaultRegistryBootstrap, NodeAdvertise: defaultRegistryBootstrap}},
			}},
			Owners: MembershipOwnerConfig{RouteLink: 1, NodeLink: 1, PlacerLink: 1, NodeList: 1},
		},
		NodeLink:   NodeLinkConfig{HeartbeatInterval: "10s", NodeDeadAfter: "30s"},
		RouteLink:  RouteLinkConfig{ParkTimeout: "30s"},
		NodeList:   NodeListConfig{WatchRetention: 10000},
		PlacerLink: PlacerLinkConfig{PlacerReplicaCount: 3, MinReadyPlacers: 1, PlacerLabel: "placer.default", PlaceTimeout: "2s"},
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
		if err := strictUnmarshalYAML(raw, &c); err != nil {
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
	if c.Membership.Active == 0 {
		c.Membership.Active = d.Membership.Active
	}
	if c.Membership.ReloadReadyTimeout == "" {
		c.Membership.ReloadReadyTimeout = d.Membership.ReloadReadyTimeout
	}
	if len(c.Membership.Versions) == 0 {
		c.Membership.Versions = d.Membership.Versions
	}
	for vi := range c.Membership.Versions {
		c.Membership.Versions[vi] = c.Membership.Versions[vi].WithComputedLabel()
	}
	if c.Membership.Owners.RouteLink == 0 {
		c.Membership.Owners.RouteLink = d.Membership.Owners.RouteLink
	}
	if c.Membership.Owners.NodeLink == 0 {
		c.Membership.Owners.NodeLink = d.Membership.Owners.NodeLink
	}
	if c.Membership.Owners.PlacerLink == 0 {
		c.Membership.Owners.PlacerLink = d.Membership.Owners.PlacerLink
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
	if c.RouteLink.ParkTimeout == "" {
		c.RouteLink.ParkTimeout = d.RouteLink.ParkTimeout
	}
	if c.NodeList.WatchRetention == 0 {
		c.NodeList.WatchRetention = d.NodeList.WatchRetention
	}
	if c.PlacerLink.PlacerReplicaCount == 0 {
		c.PlacerLink.PlacerReplicaCount = d.PlacerLink.PlacerReplicaCount
	}
	if c.PlacerLink.MinReadyPlacers == 0 {
		c.PlacerLink.MinReadyPlacers = d.PlacerLink.MinReadyPlacers
	}
	if c.PlacerLink.PlacerLabel == "" {
		c.PlacerLink.PlacerLabel = d.PlacerLink.PlacerLabel
	}
	if c.PlacerLink.PlaceTimeout == "" {
		c.PlacerLink.PlaceTimeout = d.PlacerLink.PlaceTimeout
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
		seenMembers := map[string]bool{}
		for _, member := range v.Members {
			if member.ID == "" {
				return fmt.Errorf("clustercfg: membership version %d has member with empty id", v.Version)
			}
			if seenMembers[member.ID] {
				return fmt.Errorf("clustercfg: membership version %d has duplicate member id %q", v.Version, member.ID)
			}
			seenMembers[member.ID] = true
			if member.Advertise == "" {
				return fmt.Errorf("clustercfg: membership version %d member %q advertise is required", v.Version, member.ID)
			}
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
	if err := c.validateSelfMemberConsistency(); err != nil {
		return err
	}
	if err := c.validatePlacerLabelDoesNotConflict(); err != nil {
		return err
	}
	if c.Membership.Owners.RouteLink <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.route_link must be positive")
	}
	if c.Membership.Owners.NodeLink <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.node_link must be positive")
	}
	if c.Membership.Owners.PlacerLink <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.placer_link must be positive")
	}
	if c.Membership.Owners.NodeList <= 0 {
		return fmt.Errorf("clustercfg: membership.owners.node_list must be positive")
	}
	if c.NodeList.WatchRetention <= 0 {
		return fmt.Errorf("clustercfg: node_list.watch_retention must be positive")
	}
	if c.PlacerLink.PlacerReplicaCount <= 0 {
		return fmt.Errorf("clustercfg: placer_link.placer_replica_count must be positive")
	}
	if c.PlacerLink.MinReadyPlacers <= 0 {
		return fmt.Errorf("clustercfg: placer_link.min_ready_placers must be positive")
	}
	if c.PlacerLink.MinReadyPlacers > c.PlacerLink.PlacerReplicaCount {
		return fmt.Errorf("clustercfg: placer_link.min_ready_placers must not exceed placer_replica_count")
	}
	if c.PlacerLink.PlacerLabel == "" {
		return fmt.Errorf("clustercfg: placer_link.placer_label is required")
	}
	return validateDurations(map[string]string{
		"membership.reload_ready_timeout": c.Membership.ReloadReadyTimeout,
		"node_link.heartbeat_interval":    c.NodeLink.HeartbeatInterval,
		"node_link.node_dead_after":       c.NodeLink.NodeDeadAfter,
		"route_link.park_timeout":         c.RouteLink.ParkTimeout,
		"placer_link.place_timeout":       c.PlacerLink.PlaceTimeout,
	})
}

// ControlListen is the unified registry listener.
func (c *RegistryConfig) ControlListen() string { return c.Member.Listen }

// SelfMember returns this registry process' member entry from the serving
// membership versions. Validate guarantees that repeated entries are consistent.
func (c *RegistryConfig) SelfMember() (MembershipMember, bool) {
	for _, version := range c.Membership.MemberVersions() {
		for _, member := range version.Members {
			if member.ID == c.Member.ID {
				return member, true
			}
		}
	}
	return MembershipMember{}, false
}

func (c *RegistryConfig) SelfAdvertise() string {
	member, ok := c.SelfMember()
	if !ok {
		return ""
	}
	return member.Advertise
}

func (c *RegistryConfig) SelfNodeAdvertise() string {
	member, ok := c.SelfMember()
	if !ok {
		return ""
	}
	return member.NodeAdvertise
}

func (c *RegistryConfig) validateSelfMemberConsistency() error {
	var seen bool
	var advertise, nodeAdvertise string
	for _, version := range c.Membership.MemberVersions() {
		for _, member := range version.Members {
			if member.ID != c.Member.ID {
				continue
			}
			if !seen {
				seen = true
				advertise = member.Advertise
				nodeAdvertise = member.NodeAdvertise
				continue
			}
			if member.Advertise != advertise {
				return fmt.Errorf("clustercfg: member.id %q has inconsistent advertise across membership versions", c.Member.ID)
			}
			if member.NodeAdvertise != nodeAdvertise {
				return fmt.Errorf("clustercfg: member.id %q has inconsistent node_advertise across membership versions", c.Member.ID)
			}
		}
	}
	return nil
}

func (c *RegistryConfig) validatePlacerLabelDoesNotConflict() error {
	if c.PlacerLink.PlacerLabel == "" {
		return nil
	}
	for _, version := range c.Membership.Versions {
		label := version.WithComputedLabel().Label
		if label == c.PlacerLink.PlacerLabel {
			return fmt.Errorf("clustercfg: placer_link.placer_label %q conflicts with registry membership label", c.PlacerLink.PlacerLabel)
		}
	}
	return nil
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
		Auth:     RouterAuth{DataPlane: "enforce", CacheTTL: "60s"},
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
		if err := strictUnmarshalYAML(raw, &c); err != nil {
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
// placer.yaml — placement scheduler (cluster-placer.md).
// ===========================================================================

// PlacerConfig is the standalone placer's config. It discovers registry
// membership through the bootstrap endpoint, pushes itself to placer_link, and
// answers placement calls from registry route owners.
type PlacerConfig struct {
	Placer       PlacerProcessConfig `yaml:"placer"`
	Registry     RegistryDialConfig  `yaml:"registry"` // upstream: registry bootstrap/membership
	ImportGroups []GroupSourceConfig `yaml:"import_groups,omitempty"`
	Placement    PlacementConfig     `yaml:"placement"` // placement policy
}

// DefaultPlacer returns the placer config with all non-required fields set.
func DefaultPlacer() PlacerConfig {
	return PlacerConfig{
		Placer:       PlacerProcessConfig{ID: "placer", Listen: ":7800", Advertise: "127.0.0.1:7800", MemberlistLabel: "placer.default"},
		Registry:     RegistryDialConfig{Bootstrap: defaultRegistryBootstrap},
		ImportGroups: nil,
		Placement: PlacementConfig{
			Candidates: 2, ZoneAdmitMax: "yellow",
			ImportSourceOwnerCount: 3, ImportSourceLeaseTTL: "15s", SelectorPatchRefresh: "1m",
		},
	}
}

// LoadPlacer reads, defaults, and validates placer.yaml.
func LoadPlacer(path string) (*PlacerConfig, error) {
	c := DefaultPlacer()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("clustercfg: read %s: %w", path, err)
		}
		if err := strictUnmarshalYAML(raw, &c); err != nil {
			return nil, fmt.Errorf("clustercfg: parse %s: %w", path, err)
		}
		c.applyDefaults()
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *PlacerConfig) applyDefaults() {
	d := DefaultPlacer()
	if c.Placer.ID == "" {
		c.Placer.ID = d.Placer.ID
	}
	if c.Placer.Listen == "" {
		c.Placer.Listen = d.Placer.Listen
	}
	if c.Placer.Advertise == "" {
		c.Placer.Advertise = d.Placer.Advertise
	}
	if c.Placer.MemberlistLabel == "" {
		c.Placer.MemberlistLabel = d.Placer.MemberlistLabel
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
	if c.Placement.ImportSourceOwnerCount == 0 {
		c.Placement.ImportSourceOwnerCount = d.Placement.ImportSourceOwnerCount
	}
	if c.Placement.ImportSourceLeaseTTL == "" {
		c.Placement.ImportSourceLeaseTTL = d.Placement.ImportSourceLeaseTTL
	}
	if c.Placement.SelectorPatchRefresh == "" {
		c.Placement.SelectorPatchRefresh = d.Placement.SelectorPatchRefresh
	}
}

func (c *PlacerConfig) Validate() error {
	if c.Placer.ID == "" {
		return fmt.Errorf("clustercfg: placer.id is required")
	}
	if c.Placer.Listen == "" {
		return fmt.Errorf("clustercfg: placer.listen is required")
	}
	if c.Placer.Advertise == "" {
		return fmt.Errorf("clustercfg: placer.advertise is required")
	}
	if c.Registry.Bootstrap == "" {
		return fmt.Errorf("clustercfg: registry.bootstrap is required")
	}
	if c.Placer.MemberlistLabel == "" {
		return fmt.Errorf("clustercfg: placer.memberlist_label is required")
	}
	switch c.Placement.ZoneAdmitMax {
	case "", "green", "yellow", "red":
	default:
		return fmt.Errorf("clustercfg: placement.zone_admit_max %q invalid (green|yellow|red)", c.Placement.ZoneAdmitMax)
	}
	if c.Placement.Candidates < 0 {
		return fmt.Errorf("clustercfg: placement.candidates %d invalid (must be >= 0)", c.Placement.Candidates)
	}
	if c.Placement.ImportSourceOwnerCount <= 0 {
		return fmt.Errorf("clustercfg: placement.import_source_owner_count must be positive")
	}
	seenSources := map[string]bool{}
	for _, source := range c.ImportGroups {
		if source.SourceID == "" {
			return fmt.Errorf("clustercfg: import_groups.source_id is required")
		}
		if seenSources[source.SourceID] {
			return fmt.Errorf("clustercfg: duplicate import_groups.source_id %q", source.SourceID)
		}
		seenSources[source.SourceID] = true
		switch source.SourceType {
		case "file":
			if source.Path == "" {
				return fmt.Errorf("clustercfg: import_groups[%s].path is required for file source", source.SourceID)
			}
		default:
			return fmt.Errorf("clustercfg: import_groups[%s].source_type %q invalid (file)", source.SourceID, source.SourceType)
		}
	}
	return validateDurations(map[string]string{
		"placement.import_source_lease_ttl":         c.Placement.ImportSourceLeaseTTL,
		"placement.selector_patch_refresh_interval": c.Placement.SelectorPatchRefresh,
	})
}

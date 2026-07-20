// Package types holds the core domain types shared across node-ctl.
package types

import (
	"fmt"
	"regexp"
	"strings"
)

// Profile selects which guest runtime image (and whether envd is present).
type Profile string

const (
	ProfileE2B  Profile = "e2b"  // envd in guest; full e2b data plane
	ProfileBare Profile = "bare" // no envd; only floatingip network
)

func (p Profile) Valid() bool { return p == ProfileE2B || p == ProfileBare }

func ParseProfile(s string) (Profile, error) {
	p := Profile(s)
	if !p.Valid() {
		return "", fmt.Errorf("unknown profile %q", s)
	}
	return p, nil
}

// Kind selects the boot path.
type Kind string

const (
	KindImg Kind = "img" // cold boot from an image manifest
	KindSnp Kind = "snp" // restore from a snapshot manifest
)

// State is the lifecycle state persisted in the store.
type State string

const (
	StateRunning State = "running"
	StatePaused  State = "paused"
	StateDead    State = "dead"
)

var hexKeyRe = regexp.MustCompile(`^[0-9a-f]{64}$`)

// TemplateID is the e2b templateID, self-describing as <profile>-<kind>-<key>.
// key is the 64-hex manifest content key. There is no separate template registry.
type TemplateID struct {
	Profile Profile
	Kind    Kind
	Key     string // 64-hex manifest content key
}

// ParseTemplateID parses "<profile>-<kind>-<key>", e.g. "e2b-snp-<64hex>".
func ParseTemplateID(s string) (TemplateID, error) {
	var t TemplateID
	parts := strings.SplitN(s, "-", 3)
	if len(parts) != 3 {
		return t, fmt.Errorf("templateID %q: want <profile>-<kind>-<key>", s)
	}
	profile, err := ParseProfile(parts[0])
	if err != nil {
		return t, fmt.Errorf("templateID %q: unknown profile %q", s, parts[0])
	}
	t.Profile, t.Kind, t.Key = profile, Kind(parts[1]), parts[2]
	switch t.Kind {
	case KindImg, KindSnp:
	default:
		return t, fmt.Errorf("templateID %q: unknown kind %q", s, t.Kind)
	}
	if !hexKeyRe.MatchString(t.Key) {
		return t, fmt.Errorf("templateID %q: key must be 64 hex chars", s)
	}
	return t, nil
}

func (t TemplateID) String() string { return fmt.Sprintf("%s-%s-%s", t.Profile, t.Kind, t.Key) }

// ManifestRef returns the manifest:// reference sandbox-ctl consumes.
func (t TemplateID) ManifestRef() string { return "manifest://" + t.Key }

// Sandbox is one managed sandbox instance.
type Sandbox struct {
	ID                 string
	TemplateID         string
	State              State
	DeadlineUnix       int64 // 0 = no deadline
	RunDir             string
	BaseDir            string
	RunID              string // current systemd runner instance id
	EnvdUDS            string // empty for bare
	CiUDS              string // empty for bare
	FloatingIP         string
	VswitchPort        string // vswitch port handle (1-based; slot is vswitch-internal)
	InnerIP            string // guest inner IP (CIDR), passed to vswitch attach + Network.IP
	PortMAC            string // per-port MAC from attach -> Network.MAC
	AuthKey            string // caller API authentication root; encrypted at rest
	ManifestKey        string // content cryptography root; never accepted for API authentication
	SnapshotRef        string // latest snapshot manifest key (for resume); empty if never paused
	EnvdAccessToken    string
	TrafficAccessToken string
	Metadata           map[string]string
	Env                map[string]string
	CreatedUnix        int64
}

func (s *Sandbox) Profile() Profile {
	t, err := ParseTemplateID(s.TemplateID)
	if err != nil {
		return ""
	}
	return t.Profile
}

// PidFile is where sandbox-ctl writes its pid (config-socket auth reads it).
// The unit-instance name lives in orch (configurable template names); cgroup paths
// are no longer pre-created (sandbox-ctl --cgroup-adopt uses the unit's own cgroup).
func (s *Sandbox) PidFile() string { return s.RunDir + "/" + s.ID + ".pid" }

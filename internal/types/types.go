// Package types holds the core domain types shared across node-ctl.
package types

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
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
	StateStarting State = "starting"
	StateRunning  State = "running"
	StatePaused   State = "paused"
	StateDead     State = "dead"
)

// MaxLocalSandboxIDBytes keeps <port>-<sandbox-id> within one 63-byte DNS label.
const MaxLocalSandboxIDBytes = 57

var localSandboxIDRe = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$`)

// ValidLocalSandboxID reports whether id is an opaque node-local sandbox ID.
// The contract is the 1..57-byte lowercase DNS-label subset
// ^[a-z0-9](?:[a-z0-9-]{0,55}[a-z0-9])?$.
func ValidLocalSandboxID(id string) bool {
	return localSandboxIDRe.MatchString(id)
}

const (
	MaxPortableRefBytes = 256
	MaxTemplateIDBytes  = len("bare-snp-") + (MaxPortableRefBytes*4+2)/3
)

// TemplateID is self-describing as <profile>-<kind>-<base64url(ref)>.
// Ref is one canonical portable manifest or located-file reference. There is no
// separate template registry or backend-specific template kind.
type TemplateID struct {
	Profile Profile
	Kind    Kind
	Ref     string
}

// ParseTemplateID parses <profile>-<kind>-<base64url(canonical-portable-ref)>.
func ParseTemplateID(s string) (TemplateID, error) {
	var t TemplateID
	if len(s) > MaxTemplateIDBytes {
		return t, fmt.Errorf("templateID: exceeds %d bytes", MaxTemplateIDBytes)
	}
	parts := strings.SplitN(s, "-", 3)
	if len(parts) != 3 {
		return t, fmt.Errorf("templateID %q: want <profile>-<kind>-<base64url-ref>", s)
	}
	profile, err := ParseProfile(parts[0])
	if err != nil {
		return t, fmt.Errorf("templateID %q: unknown profile %q", s, parts[0])
	}
	t.Profile, t.Kind = profile, Kind(parts[1])
	switch t.Kind {
	case KindImg, KindSnp:
	default:
		return t, fmt.Errorf("templateID %q: unknown kind %q", s, t.Kind)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return TemplateID{}, fmt.Errorf("templateID %q: decode ref: %w", s, err)
	}
	if base64.RawURLEncoding.EncodeToString(decoded) != parts[2] {
		return TemplateID{}, fmt.Errorf("templateID %q: ref encoding is not canonical base64url", s)
	}
	if len(decoded) == 0 || len(decoded) > MaxPortableRefBytes {
		return TemplateID{}, fmt.Errorf("templateID %q: ref length must be 1..%d bytes", s, MaxPortableRefBytes)
	}
	t.Ref = string(decoded)
	ref, err := ParsePortableRef(t.Ref)
	if err != nil {
		return TemplateID{}, fmt.Errorf("templateID %q: %w", s, err)
	}
	if ref.Scheme == manifest.RefSchemeFile {
		ext := ".image"
		if t.Kind == KindSnp {
			ext = ".snapshot"
		}
		if !strings.HasSuffix(ref.Path, ext) {
			return TemplateID{}, fmt.Errorf("templateID %q: %s ref must name a %s artifact", s, t.Kind, ext)
		}
	}
	return t, nil
}

func (t TemplateID) String() string {
	return fmt.Sprintf("%s-%s-%s", t.Profile, t.Kind, base64.RawURLEncoding.EncodeToString([]byte(t.Ref)))
}

// ParsePortableRef accepts only a canonical manifest or located-file reference.
func ParsePortableRef(raw string) (manifest.Ref, error) {
	if len(raw) == 0 || len(raw) > MaxPortableRefBytes {
		return manifest.Ref{}, fmt.Errorf("portable ref length must be 1..%d bytes", MaxPortableRefBytes)
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return manifest.Ref{}, fmt.Errorf("portable ref: %w", err)
	}
	if !ref.Portable() || ref.String() != raw {
		return manifest.Ref{}, fmt.Errorf("portable ref %q is not canonical and portable", raw)
	}
	return ref, nil
}

func IsPortableRef(raw string) bool {
	_, err := ParsePortableRef(raw)
	return err == nil
}

// Sandbox is one managed sandbox instance.
type Sandbox struct {
	ID                 string
	Profile            Profile
	Cluster            *ClusterSandboxContext
	AuthSandboxIDValue string // optional stable credential subject; empty falls back to ID
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
	APISecret          string // per-tenant API authentication root (hex); never written to env/yaml
	ManifestKey        string // per-tenant manifest encryption root (hex); never written to env/yaml
	SnapshotRef        string // latest local path or canonical portable snapshot ref; empty if never paused
	ServiceSecret      string // per-sandbox service authentication root (hex); never exposed publicly
	EnvdAccessToken    string
	TrafficAccessToken string
	ForwardAccessToken string
	Metadata           map[string]string
	Env                map[string]string
	CreatedUnix        int64
}

// ClusterSandboxContext is trusted node-local ownership state supplied by the
// cluster control plane. It is persisted separately from user metadata. A nil
// context identifies a standalone sandbox.
type ClusterSandboxContext struct {
	Group    string
	RouteKey string
}

// AuthSandboxID returns the stable subject used by sandbox service credentials.
// Standalone sandboxes normally leave AuthSandboxIDValue empty and therefore use
// their local ID. Imports may preserve a non-local subject without becoming
// cluster-owned.
func (s *Sandbox) AuthSandboxID() string {
	if s == nil {
		return ""
	}
	if s.AuthSandboxIDValue != "" {
		return s.AuthSandboxIDValue
	}
	return s.ID
}

// PidFile is where sandbox-ctl writes its pid (config-socket auth reads it).
// The unit-instance name and its ctl/vmm cgroup topology are local launcher state.
func (s *Sandbox) PidFile() string { return s.RunDir + "/" + s.ID + ".pid" }

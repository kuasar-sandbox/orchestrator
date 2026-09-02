// Package types holds the core domain types shared across node-ctl.
package types

import (
	"encoding/base64"
	"errors"
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

func (t ResumeTrigger) Valid() bool {
	switch t {
	case ResumeTriggerConnect, ResumeTriggerWake, ResumeTriggerRoute,
		ResumeTriggerExec, ResumeTriggerExecSession:
		return true
	default:
		return false
	}
}

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
	KindSbx Kind = "sbx" // cold boot from a portable sandbox
	KindSnp Kind = "snp" // restore from a snapshot manifest
)

// CaptureKind selects the artifact produced by a pause operation. Capture and
// resume are deliberately independent: the saved artifact constrains, but does
// not by itself request, a later launch mode.
type CaptureKind string

const (
	CaptureSnapshot CaptureKind = "snapshot"
	CaptureSandbox  CaptureKind = "sandbox"
)

func (k CaptureKind) Valid() bool { return k == CaptureSnapshot || k == CaptureSandbox }

// ResumeSourceKind identifies the durable artifact owned by a paused sandbox.
type ResumeSourceKind string

const (
	ResumeSourceSnapshot ResumeSourceKind = "snapshot"
	ResumeSourceSandbox  ResumeSourceKind = "sandbox"
)

func (k ResumeSourceKind) Valid() bool {
	return k == ResumeSourceSnapshot || k == ResumeSourceSandbox
}

// ResumeSource is the durable artifact from which a paused sandbox may resume.
type ResumeSource struct {
	Kind ResumeSourceKind `json:"kind"`
	Ref  string           `json:"ref"`
}

func (s ResumeSource) Empty() bool { return s.Kind == "" && s.Ref == "" }

func (s ResumeSource) Valid() bool { return s.Kind.Valid() && s.Ref != "" }

type ArtifactDiskMode string

const (
	ArtifactDiskSingle  ArtifactDiskMode = "single"
	ArtifactDiskOverlay ArtifactDiskMode = "overlay"
)

func (m ArtifactDiskMode) Valid() bool {
	return m == ArtifactDiskSingle || m == ArtifactDiskOverlay
}

// ArtifactDiskShape is the minimum non-secret disk projection needed to place
// host-owned active diff fields without disclosing the artifact's immutable
// refs. HasActiveBase means the portable graph supplies a captured ext4 base;
// false requires the node's formatted diff template.
type ArtifactDiskShape struct {
	Name          string           `json:"name,omitempty"`
	Mode          ArtifactDiskMode `json:"mode"`
	HasActiveBase bool             `json:"has_active_base"`
}

// ArtifactDiskTopology preserves root plus data-disk order. sandboxer checks
// the generated host document against the same PortableSandboxConfig before
// applying any binding.
type ArtifactDiskTopology struct {
	Root  ArtifactDiskShape   `json:"root"`
	Disks []ArtifactDiskShape `json:"disks,omitempty"`
}

// ResumeMode is an admission request. Auto resolves against the durable source.
type ResumeMode string

const (
	ResumeAuto   ResumeMode = "auto"
	ResumeMemory ResumeMode = "memory"
	ResumeCold   ResumeMode = "cold"
)

func (m ResumeMode) Valid() bool {
	return m == ResumeAuto || m == ResumeMemory || m == ResumeCold
}

// LaunchMode is the already-resolved launch path persisted while state is
// starting. It is authoritative across conductor restarts.
type LaunchMode string

const (
	LaunchImage  LaunchMode = "image"
	LaunchCold   LaunchMode = "cold"
	LaunchMemory LaunchMode = "memory"
)

func (m LaunchMode) Valid() bool {
	return m == LaunchImage || m == LaunchCold || m == LaunchMemory
}

// ResumeTrigger records why resume admission was requested. It never selects a
// launch mode and never controls whether a paused artifact may wake.
type ResumeTrigger string

const (
	ResumeTriggerConnect     ResumeTrigger = "connect"
	ResumeTriggerWake        ResumeTrigger = "wake"
	ResumeTriggerRoute       ResumeTrigger = "route"
	ResumeTriggerExec        ResumeTrigger = "exec"
	ResumeTriggerExecSession ResumeTrigger = "exec-session"
)

type ResumeRequest struct {
	Trigger ResumeTrigger
	Mode    ResumeMode
}

var ErrMemoryUnavailable = errors.New("memory resume is unavailable for a sandbox artifact")
var ErrLaunchModeConflict = errors.New("requested resume mode conflicts with the accepted launch mode")

// ResumeModeForMemory preserves the tri-state Connect extension: absence means
// automatic resolution, true requests memory, and false requests cold.
func ResumeModeForMemory(memory *bool) ResumeMode {
	if memory == nil {
		return ResumeAuto
	}
	if *memory {
		return ResumeMemory
	}
	return ResumeCold
}

// ResolveLaunchMode combines a durable paused source with one admission request.
func ResolveLaunchMode(source ResumeSource, requested ResumeMode) (LaunchMode, error) {
	if !source.Valid() {
		return "", fmt.Errorf("invalid resume source kind=%q ref=%q", source.Kind, source.Ref)
	}
	if !requested.Valid() {
		return "", fmt.Errorf("invalid resume mode %q", requested)
	}
	switch source.Kind {
	case ResumeSourceSnapshot:
		if requested == ResumeCold {
			return LaunchCold, nil
		}
		return LaunchMemory, nil
	case ResumeSourceSandbox:
		if requested == ResumeMemory {
			return "", ErrMemoryUnavailable
		}
		return LaunchCold, nil
	default:
		panic("unreachable resume source kind")
	}
}

func LaunchModeForTemplate(kind Kind) (LaunchMode, error) {
	switch kind {
	case KindImg:
		return LaunchImage, nil
	case KindSbx:
		return LaunchCold, nil
	case KindSnp:
		return LaunchMemory, nil
	default:
		return "", fmt.Errorf("unsupported template kind %q", kind)
	}
}

// State is the lifecycle state persisted in the store.
type State string

const (
	StateStarting State = "starting"
	StateRunning  State = "running"
	StatePaused   State = "paused"
	StateDeleting State = "deleting"
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
	case KindImg, KindSbx, KindSnp:
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
		if t.Kind == KindImg && !strings.HasSuffix(ref.Path, ".image") {
			return TemplateID{}, fmt.Errorf("templateID %q: %s ref must name a .image artifact", s, t.Kind)
		}
		if t.Kind == KindSbx &&
			!strings.HasSuffix(ref.Path, ".sandbox") && !strings.HasSuffix(ref.Path, ".bundle") {
			return TemplateID{}, fmt.Errorf("templateID %q: %s ref must name a .sandbox or .bundle artifact", s, t.Kind)
		}
		if t.Kind == KindSnp &&
			!strings.HasSuffix(ref.Path, ".snapshot") && !strings.HasSuffix(ref.Path, ".bundle") {
			return TemplateID{}, fmt.Errorf("templateID %q: %s ref must name a .snapshot or .bundle artifact", s, t.Kind)
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
	StableIDValue      string // optional stable identity; empty falls back to ID
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
	ResumeSource       ResumeSource
	AutoPauseMemory    bool
	LaunchMode         LaunchMode
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

// StableID returns the sandbox identity preserved across node-local ID changes.
// Standalone sandboxes normally leave StableIDValue empty and therefore use
// their local ID. Imports may preserve a source StableID without becoming
// cluster-owned.
func (s *Sandbox) StableID() string {
	if s == nil {
		return ""
	}
	if s.StableIDValue != "" {
		return s.StableIDValue
	}
	return s.ID
}

// PidFile is where sandbox-ctl writes its pid (config-socket auth reads it).
// The unit-instance name and its ctl/vmm cgroup topology are local launcher state.
func (s *Sandbox) PidFile() string { return s.RunDir + "/" + s.ID + ".pid" }

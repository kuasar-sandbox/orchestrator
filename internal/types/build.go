package types

import (
	"fmt"
	"regexp"
	"strings"
)

// MaxBuildIDBytes keeps a verbatim BuildID directory leaf and its longest
// phase socket within the supported Unix-domain socket path on default roots.
const MaxBuildIDBytes = 48

var buildIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)

// ValidateBuildID enforces the one shared direct/cluster Build business
// identity contract. The accepted value is also the verbatim builds/ leaf.
func ValidateBuildID(id string) error {
	if !buildIDRe.MatchString(id) {
		return fmt.Errorf("build ID must match [A-Za-z0-9_-]{1,%d}", MaxBuildIDBytes)
	}
	return nil
}

func ValidBuildID(id string) bool { return ValidateBuildID(id) == nil }

// TransientPrefix marks the immutable registration handle. Standalone and
// cluster registrations use their existing ID generators. Keep this handle for
// Build status/cancellation/deletion; successful canonical IDs identify artifacts.
const TransientPrefix = "transient-"

// IsTransientID reports whether s is a register-time transient templateID.
func IsTransientID(s string) bool { return strings.HasPrefix(s, TransientPrefix) }

// ValidateTransientID accepts the registration handles produced by both node
// and cluster registration without imposing a UUID-only suffix.
func ValidateTransientID(id string) error {
	if !IsTransientID(id) || !buildIDRe.MatchString(strings.TrimPrefix(id, TransientPrefix)) {
		return fmt.Errorf("template ID must be a registered transient ID")
	}
	return nil
}

// BuildState is the lifecycle of a template build. The builds row is retained
// only as bounded status/index history; a canonical TemplateID and its portable
// artifact remain the long-lived launch authority without a templates table.
type BuildState string

const (
	BuildRegistered BuildState = "registered" // POST /v3/templates done, not yet triggered
	BuildWaiting    BuildState = "waiting"    // triggered, queued for the builder pool
	BuildBuilding   BuildState = "building"   // a builder run is executing it
	BuildReady      BuildState = "ready"      // persist id available in PersistID/names/aliases
	BuildError      BuildState = "error"      // see Reason
)

// SDKStatus maps the internal state onto the e2b status the CLI/SDK polls. The
// e2b CLI's build-wait loop continues only while status == "building" (it has no
// "waiting" case and exits the loop on any other value), so every in-progress
// state — registered, waiting (queued), building — must report as "building".
// Only the terminal states map through: ready -> ready, error -> error.
func (s BuildState) SDKStatus() string {
	switch s {
	case BuildReady:
		return string(BuildReady)
	case BuildError:
		return string(BuildError)
	default: // registered | waiting | building -> in progress
		return string(BuildBuilding)
	}
}

// TemplateStep is one e2b v2 build step (TemplateStep in the e2b API):
// RUN executes in the build sandbox; ENV/ARG/WORKDIR/USER are host-side
// build-context transforms (ENV/WORKDIR/USER persist into the image's
// runtime config, ARG only substitutes). COPY is rejected at submit
// until the files endpoint exists.
type TemplateStep struct {
	Type      string   `json:"type"`
	Args      []string `json:"args,omitempty"`
	FilesHash string   `json:"filesHash,omitempty"`
	Force     bool     `json:"force,omitempty"`
}

// BuildTargetKind is the public, registration-time output family. It uses
// complete words deliberately; img/sbx/snp remain canonical artifact kinds.
type BuildTargetKind string

const (
	BuildTargetImage   BuildTargetKind = "image"
	BuildTargetSandbox BuildTargetKind = "sandbox"
)

// BuildTarget is both the requested target value and the resolved worker
// result. A nil *BuildTarget in BuildOptions means auto selection. Memory is
// meaningful only for the sandbox target.
type BuildTarget struct {
	Kind   BuildTargetKind `json:"kind" yaml:"kind"`
	Memory bool            `json:"memory,omitempty" yaml:"memory,omitempty"`
}

func (t BuildTarget) Validate() error {
	switch t.Kind {
	case BuildTargetImage:
		if t.Memory {
			return fmt.Errorf("build target image does not support memory=true")
		}
	case BuildTargetSandbox:
	default:
		return fmt.Errorf("unknown build target kind %q", t.Kind)
	}
	return nil
}

// ResolveBuildTarget applies the sole auto rule: any effective start/ready
// command selects a memory Sandbox; otherwise the output is an Image.
func ResolveBuildTarget(requested *BuildTarget, startCmd, readyCmd string) BuildTarget {
	if requested != nil {
		return *requested
	}
	if startCmd != "" || readyCmd != "" {
		return BuildTarget{Kind: BuildTargetSandbox, Memory: true}
	}
	return BuildTarget{Kind: BuildTargetImage}
}

// ArtifactKind returns the one canonical artifact kind represented by target.
func (t BuildTarget) ArtifactKind() Kind {
	switch {
	case t.Kind == BuildTargetImage:
		return KindImg
	case t.Kind == BuildTargetSandbox && !t.Memory:
		return KindSbx
	case t.Kind == BuildTargetSandbox && t.Memory:
		return KindSnp
	default:
		return ""
	}
}

// BuildOptions are build-only controls. They are intentionally separate from
// Build.Metadata, which configures artifact construction but is never recovered
// from a retained Build row by a later canonical TemplateID Create.
type BuildOptions struct {
	// Target is immutable registration input. Nil means auto; resolution occurs
	// in the tenant task after source-E defaults are available.
	Target *BuildTarget `json:"target,omitempty" yaml:"target,omitempty"`
	// Resources is accepted only in the registration-time builder namespace.
	// Core normalizes it into Build.Resources and clears this definition copy
	// before persistence so there is one durable resource authority.
	Resources *BuildResources       `json:"resources,omitempty" yaml:"resources,omitempty"`
	Referer   *BuildRefererOptions  `json:"referer,omitempty" yaml:"referer,omitempty"`
	Registry  *BuildRegistryOptions `json:"registry,omitempty" yaml:"registry,omitempty"`
}

type BuildRefererOptions struct {
	Enabled   *bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Writeback *bool `json:"writeback,omitempty" yaml:"writeback,omitempty"`
}

// BuildRegistryOptions carries the per-build registry source trust policy.
// Register-time only: a trigger-time builder.registry is rejected (400) and
// does not participate in buildcfg.Merge — the register-time base is kept.
type BuildRegistryOptions struct {
	TLS *BuildRegistryTLSOptions `json:"tls,omitempty" yaml:"tls,omitempty"`
}

// BuildRegistryTLSOptions tunes how the build's Phase A import sandbox verifies
// the source registry's HTTPS certificate. CABundlePEM is an inline PEM bundle
// (appended to the guest system root CAs); InsecureSkipVerify disables cert
// verification entirely. They are mutually exclusive. Projected into the guest
// via a flatten-ctl config YAML (--config); never persisted into template
// metadata, never inherited by other builds.
type BuildRegistryTLSOptions struct {
	CABundlePEM        string `json:"ca_bundle_pem,omitempty" yaml:"ca_bundle_pem,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty" yaml:"insecure_skip_verify,omitempty"`
}

// BuildResult is the immutable pipeline result accepted from run-builder. It
// is persisted while the execution claim is still held so a controller restart
// can finish unit/runtime cleanup without losing a successful result. None of
// these fields contains tenant credentials.
type BuildResult struct {
	Target       BuildTarget `json:"target"`
	ImageRef     string      `json:"image_ref,omitempty"`
	SandboxRef   string      `json:"sandbox_ref,omitempty"`
	SnapshotRef  string      `json:"snapshot_ref,omitempty"`
	StartCmd     string      `json:"start_cmd,omitempty"`
	ReadyCmd     string      `json:"ready_cmd,omitempty"`
	Error        string      `json:"error,omitempty"`
	FailureStage string      `json:"failure_stage,omitempty"`
}

// Build is one template build plus its retention-bounded status/index history.
type Build struct {
	BuildID      string  // node or cluster Build business identity
	TemplateID   string  // immutable transient registration handle
	PersistID    string  // <profile>-<kind>-<base64url(portable-ref)>, set when ready
	APISecret    string  // per-tenant API authentication root (hex)
	ManifestKey  string  // per-tenant manifest encryption root (hex)
	Profile      Profile // immutable output profile selected at registration
	Kind         Kind    // terminal resolved artifact kind; empty while nonterminal
	FromImage    string  // OCI base image (the Dockerfile FROM); mutually exclusive with FromTemplate
	FromTemplate string  // base template ref (its portable config supplies the base image + start/ready defaults)
	RegistryAuth string  // resolved registry pull creds (regcreds.Creds JSON; "" = anonymous), stored encrypted
	// RegistrationImageRepo and RegistrationRegistryAuth retain the exact
	// registry-owned cluster Register input for durable BuildID replay checks.
	// They are node-internal, never become template metadata, and the credential
	// is encrypted at rest independently from the trigger work order above.
	RegistrationImageRepo    string
	RegistrationRegistryAuth string
	// RegistrationMMDSRoutesDigest retains the immutable registration identity
	// of builder-only MMDS routes after terminal cleanup removes those routes and
	// their confidential values from the portable template record.
	RegistrationMMDSRoutesDigest string
	// RegistrationMMDSValuesDigest is a keyed, irreversible identity for the
	// initial confidential MMDS values. It lets an exact registration replay be
	// verified after terminal cleanup has deliberately removed the ciphertext.
	RegistrationMMDSValuesDigest string
	// RegistrationRequestDigest is a tenant-keyed identity of the original
	// canonical cluster BuildRegister command before an Extension may modify its
	// mutable candidate. Exact ACK replay checks this digest without re-running
	// the Hook or retaining its confidential input in plaintext.
	RegistrationRequestDigest string
	StartCmd                  string // e2b trigger command; participates in auto target resolution
	ReadyCmd                  string // e2b readiness probe; participates in auto target resolution
	Steps                     []TemplateStep
	Status                    BuildState
	Reason                    string   // error detail
	RunID                     string   // current systemd builder runner instance id
	Names                     []string // user-supplied name(s) + persist id (when ready)
	Aliases                   []string // user-supplied alias(es) + persist id (when ready)
	// Resources is the immutable Build demand used by registration/execution
	// admission and A/B execution sandbox resource resolution. Final Sandbox
	// resources are resolved independently; this vector is not snapshot metadata.
	Resources BuildResources
	// Metadata configures the portable artifact produced by this Build. Build-only
	// options and instance-only secrets are removed before it is stored here;
	// post-Build Create never treats this retained copy as template authority.
	Metadata map[string]string
	Env      map[string]string
	Secure   bool
	// Registration-time instance credentials are encrypted at rest and used only
	// by a memory target's Phase C. They never enter portable E/S configuration.
	ServiceSecret      string
	EnvdAccessToken    string
	TrafficAccessToken string
	Builder            BuildOptions
	// ClusterGroup is node-internal durable ownership for registry-driven
	// Builds. It is deliberately separate from portable template Metadata.
	ClusterGroup string

	// Durable execution ownership and timestamps make both admission ledgers
	// reconstructable from SQLite after a controller restart.
	WaitingUnix          int64
	WaitingSequence      int64
	ExecutionClaimed     bool
	ExecutionClaimedUnix int64
	Phase                string
	PhaseSandboxID       string
	// Runtime network ownership is persisted only while execution is claimed so
	// a controller restart can reattach to a live builder unit or safely reclaim
	// its host resources. The token is encrypted by Store and never enters
	// portable metadata or a template artifact.
	RuntimeVswitchPort     string
	RuntimeFloatingIP      string
	RuntimePortMAC         string
	RuntimeEnvdAccessToken string
	// RuntimePrepareJSON is the non-secret canonical final host preparation
	// committed atomically with the runtime port. It lets a new conductor return
	// an equivalent BuildSpec without rereading the source artifact or applying
	// potentially changed node defaults.
	RuntimePrepareJSON string
	// ExecutionResult is set atomically before the config-socket acknowledges
	// run-builder's report. Terminal persistence clears it together with the
	// execution claim after the unit and host runtime have been reclaimed.
	ExecutionResult *BuildResult

	// Intent timestamps are monotonic lifecycle data, never BuildOptions.
	CancelRequestedUnix int64
	DeleteRequestedUnix int64

	CreatedUnix  int64
	FinishedUnix int64 // terminal ready/error commit time; 0 while nonterminal
}

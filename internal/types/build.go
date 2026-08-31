package types

import "strings"

// TransientPrefix marks the register-time templateID the e2b SDK first receives,
// "transient-<uuidv7>". It is a throwaway handle: once the build is ready, the
// self-describing persist id "<profile>-<kind>-<base64url(portable-ref)>" is surfaced via the
// template's names+aliases and used for everything afterwards.
const TransientPrefix = "transient-"

// IsTransientID reports whether s is a register-time transient templateID.
func IsTransientID(s string) bool { return strings.HasPrefix(s, TransientPrefix) }

// BuildState is the lifecycle of a template build (persisted in the builds table,
// which doubles as the template registry — there is no separate templates table).
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

// BuildOptions are build-only controls. They are intentionally separate from
// Build.Metadata, which becomes the template's default sandbox config.
type BuildOptions struct {
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
	ImageRef     string `json:"image_ref,omitempty"`
	SnapshotRef  string `json:"snapshot_ref,omitempty"`
	StartCmd     string `json:"start_cmd,omitempty"`
	ReadyCmd     string `json:"ready_cmd,omitempty"`
	Error        string `json:"error,omitempty"`
	FailureStage string `json:"failure_stage,omitempty"`
}

// Build is one template build, doubling as the template record.
type Build struct {
	BuildID      string  // e2b build id (uuidv7)
	TemplateID   string  // transient-<uuidv7>, the register-time handle
	PersistID    string  // <profile>-<kind>-<base64url(portable-ref)>, set when ready
	APISecret    string  // per-tenant API authentication root (hex)
	ManifestKey  string  // per-tenant manifest encryption root (hex)
	Profile      Profile // immutable output profile selected at registration
	Kind         Kind    // img (flatten only) | snp (boot+snapshot)
	FromImage    string  // OCI base image (the Dockerfile FROM); mutually exclusive with FromTemplate
	FromTemplate string  // base template ref (its snapshot cfg supplies the base image + start/ready defaults)
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
	StartCmd                  string // e2b only; non-empty => snapshot build (kind=snp)
	ReadyCmd                  string // e2b only; readiness probe run after StartCmd (poll until exit 0)
	Steps                     []TemplateStep
	Status                    BuildState
	Reason                    string   // error detail
	RunID                     string   // current systemd builder runner instance id
	Names                     []string // user-supplied name(s) + persist id (when ready)
	Aliases                   []string // user-supplied alias(es) + persist id (when ready)
	// Resources is the immutable outer Build demand used by registration and
	// execution admission plus systemd enforcement. It never becomes sandbox
	// capacity/allocatable/startup and never enters a snapshot.
	Resources BuildResources
	// PhaseResourcePatch is canonical kuasar-sandbox.resource JSON used only to
	// resolve the A/B/C phase sandboxes on this node. It is not portable template
	// metadata and therefore cannot affect a later IMG Create.
	PhaseResourcePatch string
	// Metadata is portable template metadata. Build-only options and the phase
	// resource patch are removed before it is stored here.
	Metadata map[string]string
	Builder  BuildOptions
	// ClusterGroup is node-internal durable ownership for registry-driven
	// Builds. It is deliberately separate from portable template Metadata.
	ClusterGroup string

	// Durable execution ownership and timestamps make both admission ledgers
	// reconstructable from SQLite after a controller restart.
	WaitingUnix          int64
	WaitingSequence      int64
	ExecutionClaimed     bool
	ExecutionClaimedUnix int64
	EnforcementStatus    string
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
	// an equivalent BuildSpec without rereading the source snapshot or applying
	// potentially changed node defaults.
	RuntimePrepareJSON string
	// ExecutionResult is set atomically before the config-socket acknowledges
	// run-builder's report. Terminal persistence clears it together with the
	// execution claim after the unit and host runtime have been reclaimed.
	ExecutionResult *BuildResult

	CreatedUnix int64
}

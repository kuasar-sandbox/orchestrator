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
	Referer  *BuildRefererOptions  `json:"referer,omitempty" yaml:"referer,omitempty"`
	Registry *BuildRegistryOptions `json:"registry,omitempty" yaml:"registry,omitempty"`
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
	StartCmd     string  // e2b only; non-empty => snapshot build (kind=snp)
	ReadyCmd     string  // e2b only; readiness probe run after StartCmd (poll until exit 0)
	Steps        []TemplateStep
	Status       BuildState
	Reason       string   // error detail
	RunID        string   // current systemd builder runner instance id
	Names        []string // user-supplied name(s) + persist id (when ready)
	Aliases      []string // user-supplied alias(es) + persist id (when ready)
	// Metadata is the template's default sandbox config — the same kuasar-sandbox.<ns>
	// namespaced keys a create carries (register cpu/memory + X-Kuasar-Sandbox-*
	// headers land here; trigger overrides). It drives the build's phase-C capacity
	// and is layered under a create's own config (create wins) when launching from
	// this template.
	Metadata map[string]string
	Builder  BuildOptions

	CreatedUnix int64
}

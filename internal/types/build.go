package types

import "strings"

// TransientPrefix marks the register-time templateID the e2b SDK first receives,
// "transient-<uuidv7>". It is a throwaway handle: once the build is ready, the
// self-describing persist id "<profile>-<kind>-<key>" is surfaced via the
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
	BuildBuilding   BuildState = "building"   // a pool slot is executing it
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

// Build is one template build, doubling as the template record.
type Build struct {
	BuildID     string  // e2b build id (uuidv7)
	TemplateID  string  // transient-<uuidv7>, the register-time handle
	PersistID   string  // <profile>-<kind>-<key>, set when ready
	ManifestKey string  // per-tenant manifest key (hex); ownership + crypto root
	Profile     Profile // e2b (API builds are always e2b)
	Kind        Kind    // img (flatten only) | snp (boot+snapshot)
	FromImage    string // OCI base image (the Dockerfile FROM); mutually exclusive with FromTemplate
	FromTemplate string // base template ref (its snapshot cfg supplies the base image + start/ready defaults)
	RegistryAuth string // resolved registry pull creds (regcreds.Creds JSON; "" = anonymous), stored encrypted
	StartCmd     string // non-empty => snapshot build (kind=snp)
	ReadyCmd     string // readiness probe run after StartCmd (poll until exit 0)
	Steps        []TemplateStep
	Status       BuildState
	Reason      string   // error detail
	Names       []string // user-supplied name(s) + persist id (when ready)
	Aliases     []string // user-supplied alias(es) + persist id (when ready)
	CreatedUnix int64
}

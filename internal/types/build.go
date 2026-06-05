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

// Build is one template build, doubling as the template record.
type Build struct {
	BuildID     string  // e2b build id (uuidv7)
	TemplateID  string  // transient-<uuidv7>, the register-time handle
	PersistID   string  // <profile>-<kind>-<key>, set when ready
	ManifestKey string  // per-tenant manifest key (hex); ownership + crypto root
	Profile     Profile // e2b (API builds are always e2b)
	Kind        Kind    // img (flatten only) | snp (boot+snapshot)
	FromImage   string  // OCI base image (the Dockerfile FROM)
	StartCmd    string  // non-empty => snapshot build (kind=snp)
	Status      BuildState
	Reason      string   // error detail
	Names       []string // user-supplied name(s) + persist id (when ready)
	Aliases     []string // user-supplied alias(es) + persist id (when ready)
	CreatedUnix int64
}

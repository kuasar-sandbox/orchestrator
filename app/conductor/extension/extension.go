// Package extension defines the public runtime-extension contract for a
// statically linked conductor. Extensions are trusted, in-process code; these
// interfaces organize access to core state without exposing internal packages.
package extension

import "context"

// Extension is the one statically linked runtime extension for a conductor
// process. Start is called exactly once after the store and core have been
// constructed and before reconciliation, pools, node-link, or listeners start.
// Cancellation of ctx is the extension's process-shutdown notification.
type Extension interface {
	Start(context.Context, Host) error
}

// Host exposes conductor-owned object sources to an Extension.
type Host interface {
	Sandboxes() SandboxSource
	Builds() BuildSource
}

// Profile is a sandbox or build runtime profile.
type Profile string

const (
	ProfileE2B  Profile = "e2b"
	ProfileBare Profile = "bare"
)

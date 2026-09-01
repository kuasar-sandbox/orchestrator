// Package extension defines the public runtime-extension contract for a
// statically linked conductor. Extensions are trusted, in-process code; these
// interfaces organize access to core state without exposing internal packages.
package extension

import (
	"context"
	"errors"
	"net/http"
)

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

// APIWrapper is an optional capability implemented by the same Extension
// object. WrapAPI is called once after Start succeeds. The returned handler may
// add, rewrite, or replace any API route; returning nil fails startup.
type APIWrapper interface {
	WrapAPI(http.Handler) http.Handler
}

// SandboxHook is an optional lifecycle capability implemented by the same
// Extension object. The operation and all nested values are independent
// copies. Core state is authoritatively re-read after the callback returns.
type SandboxHook interface {
	PrepareSandbox(context.Context, *SandboxOperation) error
}

// BuildHook is an optional build-lifecycle capability implemented by the same
// Extension object. It is not called for waiting-to-building execution claims.
type BuildHook interface {
	PrepareBuild(context.Context, *BuildOperation) error
}

// ErrRejected is the minimal policy-rejection contract for lifecycle Hooks.
// The conductor maps it to a fixed public rejection without returning private
// error details. Every other Hook error means the Extension is unavailable.
var ErrRejected = errors.New("operation rejected by extension")

// Profile is a sandbox or build runtime profile.
type Profile string

const (
	ProfileE2B  Profile = "e2b"
	ProfileBare Profile = "bare"
)

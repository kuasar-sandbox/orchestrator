package extension

import (
	"context"
	"net/http"
)

// Role identifies the current proxy App process.
type Role string

const (
	RoleMaster Role = "master"
	RoleWorker Role = "worker"
)

// Process identifies one process-local Runtime binding invocation. WorkerID
// and WorkerEpoch are zero values for the master.
type Process struct {
	Role        Role
	WorkerID    string
	WorkerEpoch uint64
}

// WorkerExtension is the one trusted, statically linked extension for one
// proxy worker epoch. Start is called exactly once after initial route sync and
// before the sandbox data ingress listener starts serving. Cancellation of ctx is the
// worker epoch's shutdown notification.
type WorkerExtension interface {
	Start(context.Context, WorkerHost) error
}

// WorkerHost exposes point-in-time worker state and the core forwarding
// pipeline. It deliberately has no route Watch; dynamic route observation is a
// proxy-master capability.
type WorkerHost interface {
	Process() Process
	GetRoute(sandboxID string) (RouteView, bool)

	// ForwardAuthorized owns w and completes the HTTP response on every path.
	// The caller must authenticate the private request before calling it and
	// must not write another error after it returns. This helper does not verify
	// Kuasar's X-Access-Token.
	ForwardAuthorized(http.ResponseWriter, *http.Request, ForwardRequest)
}

// IngressWrapper is an optional capability implemented by the same
// WorkerExtension object. WrapIngress is called once after Start succeeds. The
// returned handler receives the node's raw sandbox data ingress before the
// built-in sandbox and CONNECT parsers; returning nil fails this worker epoch.
type IngressWrapper interface {
	WrapIngress(http.Handler) http.Handler
}

// ConnectService is the logical backend selected by ForwardAuthorized. The
// empty value retains the legacy profile + port mapping.
type ConnectService string

const (
	ConnectServiceLegacy         ConnectService = ""
	ConnectServiceForward        ConnectService = "forward"
	ConnectServiceE2BEnvd        ConnectService = "e2b:envd"
	ConnectServiceE2BInterpreter ConnectService = "e2b:code-interpreter"
	ConnectServiceExec           ConnectService = "exec"
)

// ConnectTarget is an independent public value describing a guest target.
// Legacy and explicit forward targets require a port in [1, 65535]. Native
// exec is intentionally rejected by ForwardAuthorized and must use the
// built-in handler's KAT and per-command CEL path.
type ConnectTarget struct {
	Service ConnectService
	Port    int
}

// ForwardRequest describes one request whose private authentication has
// already completed in an ingress wrapper.
type ForwardRequest struct {
	SandboxID string
	Target    ConnectTarget

	// Revalidate runs after activation and binding revalidation succeed, but
	// before the backend is dialed. An error produces a fixed stale-policy
	// response; private error details are logged only by the core.
	Revalidate func(context.Context) error

	// Rewrite runs only for the independent request clone sent to an ordinary
	// HTTP guest backend. It is never called for CONNECT. An error produces a
	// fixed response before any guest request bytes are written.
	Rewrite func(*http.Request) error
}

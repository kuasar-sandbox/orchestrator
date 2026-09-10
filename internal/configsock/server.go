// Package configsock implements the orchestrator's local control socket: a single
// UDS that multiplexes several planes over HTTP (h2c, with HTTP/1.1 fallback), each
// with its own authentication:
//
//   - run    (POST /internal/run/assignment, /internal/run/build-result):
//     prestarted run-id units wait for their sandbox/build assignment; run-builder
//     posts its result back here. Authed by SO_PEERCRED peer pid == the run-id
//     pidfile (/run/sandbox/runners/<run-id>.pid).
//   - task   (POST /internal/task/{sandbox,build}/{bootstrap,prepare}):
//     assigned tasks fetch their LaunchSpec or BuildSpec by business id. Authed by
//     SO_PEERCRED peer pid == the task pidfile
//     (/run/sandbox/sandboxes/<sid>/<sid>.pid or
//     /run/sandbox/builds/<build-id>/builder.pid).
//   - admin  (/internal/admin/manifest-keys and sandbox MMDS route-value paths):
//     manifest-key allowlist management plus bounded secret PUT/DELETE. Authed by
//     SO_PEERCRED peer pid ∈ admin_pidfile (or, when that is unset, by the socket's
//     0600 permissions alone = same uid / root).
//   - plugin (PUT /internal/plugin/{id}/register): a subscriber (the independent proxy
//     master, or a route observer such as the platform agent) registers its
//     capabilities and holds the connection open as its route stream + lease (see
//     internal/routesync). Authed by SO_PEERCRED peer pid ∈ plugin_pidfile (or socket
//     perms when unset).
//   - api    (everything else): the e2b-compatible control plane — the SAME
//     http.Handler served over TLS at api.<domain> — reached locally over plain h2c
//     and authed by X-API-KEY. Includes the export-sandbox / import-sandbox routes.
//
// The socket is the sole channel for the secret-bearing task env (manifest key)
// and run assignment/result traffic; bulky non-secret config is a plain file
// referenced by the spec's args.
package configsock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/unixcred"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Plane paths. The task/admin planes live under /internal/ (a prefix the e2b SDK
// never uses); every other path falls through to the api handler.
const (
	PathTaskSandboxBootstrap       = "/internal/task/sandbox/bootstrap"
	PathTaskSandboxPrepare         = "/internal/task/sandbox/prepare"
	PathTaskBuildBootstrap         = "/internal/task/build/bootstrap"
	PathTaskBuildPrepare           = "/internal/task/build/prepare"
	PathRunAssignment              = "/internal/run/assignment"
	PathRunBuildResult             = "/internal/run/build-result"
	PathRunBuildPhase              = "/internal/run/build-phase"
	PathAdminManifestKey           = "/internal/admin/manifest-keys"
	PathAdminBuilderAdmission      = "/internal/admin/builder-admission"
	PathAdminMMDSRouteSecretPut    = "PUT /internal/admin/sandboxes/{id}/mmds/secrets/{name}"
	PathAdminMMDSRouteSecretDelete = "DELETE /internal/admin/sandboxes/{id}/mmds/secrets/{name}"

	// Bootstrap requests carry only exact task identifiers. Prepare requests may
	// additionally carry up to 1 MiB of decoded artifact network metadata; the
	// larger wire bound accommodates JSON string escaping while keeping parsing
	// bounded before any task authentication or provider call.
	taskBootstrapRequestMaxBytes int64 = 64 << 10
	taskPrepareRequestMaxBytes   int64 = 8 << 20
)

// journald SYSLOG_IDENTIFIER tags the sandbox stack writes under (shared so the
// producers — sandbox-ctl, the build pipeline — and the orchestrator's log query
// all agree on one vocabulary). Producers also include KUASAR_* fields so user-
// facing log queries can select by sandbox/build id instead of unit instance name.
//
//   - BuildLogTag ("build")   curated build progress with KUASAR_BUILD_ID:
//     run-builder's milestones + relayed RUN output + the phase sandboxes' own
//     app stdio. This is the ONLY tag the orchestrator surfaces to the SDK.
//   - RunnerLogTag ("sandbox") a live sandbox's app stdio with KUASAR_SANDBOX_ID;
//     host-only telemetry.
//   - ConsoleTag ("console")  guest kernel dmesg, written by BOTH runner and
//     builder sandboxes; host-only (deliberately NOT in the SDK build log).
const (
	BuildLogTag  = "build"
	RunnerLogTag = "sandbox"
	ConsoleTag   = "console"
)

type AssignmentRequest struct {
	Kind  string `json:"kind"`
	RunID string `json:"run_id"`
}

type AssignmentResponse struct {
	Kind   string `json:"kind,omitempty"`
	RunID  string `json:"run_id,omitempty"`
	TaskID string `json:"task_id,omitempty"`
	Error  string `json:"error,omitempty"`
}

// BuildResult is the config-socket wire representation of the durable core
// result. Keeping this an alias prevents the report and recovery paths from
// acquiring subtly different schemas.
type BuildResult = types.BuildResult

type BuildResultRequest struct {
	RunID   string      `json:"run_id"`
	BuildID string      `json:"build_id"`
	Result  BuildResult `json:"result"`
}

type BuildResultResponse struct {
	Error string `json:"error,omitempty"`
}

type BuildPhaseRequest struct {
	RunID     string `json:"run_id"`
	BuildID   string `json:"build_id"`
	Phase     string `json:"phase"`
	SandboxID string `json:"sandbox_id"`
	State     string `json:"state"`
}

type BuildPhaseResponse struct {
	Error string `json:"error,omitempty"`
}

// BuildReportRejection marks a worker report that the provider definitively
// rejected (unknown/stale ownership or a conflicting replay). The server maps
// it to 409 so run-builder does not retry it; unmarked provider failures remain
// 5xx and are retryable because result/phase writes are idempotent.
type BuildReportRejection struct{ Err error }

func (e *BuildReportRejection) Error() string { return e.Err.Error() }
func (e *BuildReportRejection) Unwrap() error { return e.Err }

func RejectBuildReport(err error) error {
	if err == nil {
		return nil
	}
	return &BuildReportRejection{Err: err}
}

func IsBuildReportRejection(err error) bool {
	var rejection *BuildReportRejection
	return errors.As(err, &rejection)
}

// LaunchSpec is the generic launch config the launcher applies and then exec-replaces
// into: the absolute target binary, its args (after argv0), the working dir, and
// env added to the inherited environment (secrets — e.g. MANIFEST_KEY — ride here,
// never on disk).
type LaunchSpec struct {
	Exec    string            `json:"exec"`
	Args    []string          `json:"args,omitempty"`
	Workdir string            `json:"workdir,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// Provider resolves a config-id to its LaunchSpec + the pidfile used to
// authenticate the caller (SO_PEERCRED). ok=false means the id is unknown.
type Provider interface {
	// SandboxTaskAuth returns only the non-secret pidfile identity for one exact
	// active assignment. The server calls it before SandboxTaskSpecFor or
	// CompleteSandboxPrepare so an unauthorized peer cannot trigger secret reads.
	SandboxTaskAuth(ctx context.Context, sandboxID, runID string) (auth SandboxTaskAuth, ok bool, err error)
	SandboxTaskSpecFor(ctx context.Context, sandboxID, runID string) (resp *SandboxTaskSpec, ok bool, err error)
	CompleteSandboxPrepare(ctx context.Context, sandboxID, runID string, summary ArtifactPrepareSummary) (*LaunchSpec, error)
	BuildTaskAuth(ctx context.Context, buildID, runID string) (auth BuildTaskAuth, ok bool, err error)
	BuildTaskSpecFor(ctx context.Context, buildID, runID string) (resp *BuildTaskSpec, ok bool, err error)
	CompleteBuildPrepare(ctx context.Context, buildID, runID string, summary ArtifactPrepareSummary) (*BuildSpec, error)
	RunPidFile(kind, runID string) (pidFile string, ok bool)
	WaitAssignment(ctx context.Context, kind, runID string) (taskID string, ok bool, err error)
	PostBuildResult(ctx context.Context, runID, buildID string, result BuildResult) error
	PostBuildPhase(ctx context.Context, runID, buildID, phase, sandboxID, state string) error
}

// BuildSpec is the final work order an authenticated run-builder receives:
// everything its target-selected import/materialize/capture pipeline needs.
// Secrets ride in BuildTaskSpec.Env, never on disk.
type BuildSpec struct {
	BuildID          string             `json:"build_id"`
	Profile          string             `json:"profile"`
	RunID            string             `json:"run_id,omitempty"`
	RunDir           string             `json:"run_dir"`  // volatile BuildRunDir
	BaseDir          string             `json:"base_dir"` // persistent BuildBaseDir
	FromImage        string             `json:"from_image,omitempty"`
	FromTemplateRef  string             `json:"from_template_ref,omitempty"`
	FromTemplateKind string             `json:"from_template_kind,omitempty"`
	RefLocations     map[string]string  `json:"ref_locations,omitempty"`
	CheckpointMode   string             `json:"checkpoint_mode"`
	RequestedTarget  *types.BuildTarget `json:"requested_target,omitempty"`
	// Source* is populated only inside run-builder after task-local preparation.
	// A Snapshot source has already converged to its cold Sandbox E at this boundary.
	SourceSandboxRef    string                          `json:"-"`
	SourceSandboxConfig *rtconfig.PortableSandboxConfig `json:"-"`
	SourceImageConfig   []byte                          `json:"-"`
	// CheckpointRefLocationParent controls only Phase-C checkpoint-class graph
	// publication. CheckpointRemoteManifest independently selects named-location
	// Bundle publication for image-class roots. The builder derives each actual
	// publication name immediately before that publication starts.
	CheckpointRefLocationParent string                    `json:"checkpoint_ref_location_parent,omitempty"`
	CheckpointRemoteManifest    bool                      `json:"checkpoint_remote_manifest,omitempty"`
	Steps                       []BuildStep               `json:"steps,omitempty"`
	StartCmd                    string                    `json:"start_cmd,omitempty"`
	ReadyCmd                    string                    `json:"ready_cmd,omitempty"`
	Env                         map[string]string         `json:"env,omitempty"` // run-builder local only after merging authenticated BuildTaskSpec.Env
	Paths                       BuildPaths                `json:"paths"`
	Net                         BuildNet                  `json:"net"`
	TemplateNetwork             sandboxcfg.NetworkSpec    `json:"template_network"` // persisted in phase-C snapshot metadata; not guest BuildNet
	Resources                   rtconfig.ResourcesConfig  `json:"resources"`        // A/B execution resources
	SandboxResources            rtconfig.ResourcesConfig  `json:"sandbox_resources"`
	SandboxSpec                 sandboxcfg.SandboxSpec    `json:"sandbox_spec"`
	SandboxNamespaces           []string                  `json:"sandbox_namespaces,omitempty"`
	SandboxEnv                  map[string]string         `json:"sandbox_env,omitempty"`
	HasSandboxConfig            bool                      `json:"has_sandbox_config,omitempty"` // inputs requiring a Sandbox artifact; excludes execution network
	HasInstanceConfig           bool                      `json:"has_instance_config,omitempty"`
	CheckpointPolicy            sandboxcfg.SnapshotPolicy `json:"checkpoint_policy,omitempty"`
	MMDSEnabled                 bool                      `json:"mmds_enabled"`
	EnvdToken                   string                    `json:"envd_token,omitempty"` // phase C envd /init token (mmds posture)
	Insecure                    bool                      `json:"insecure,omitempty"`   // registry plain-HTTP/skip-TLS
	Platform                    string                    `json:"platform,omitempty"`
	ImportReferer               BuildImportReferer        `json:"import_referer,omitempty"`
	// RegistryTLS carries the per-build registry TLS trust (inline CA bundle
	// PEM and/or skip-verify) projected into the Phase A import sandbox as a
	// flatten-ctl config YAML. Nil = use system root CAs. Register-time only;
	// not inherited by templates.
	RegistryTLS *BuildRegistryTLS `json:"registry_tls,omitempty"`
	Timeouts    BuildTimeouts     `json:"timeouts"`
	Error       string            `json:"error,omitempty"`
}

// BuildRegistryTLS is the flattened, resolved per-build registry TLS policy
// handed to the build unit (mirrors how BuildImportReferer flattens the
// referer options). CABundlePEM is inline PEM content (not a host path);
// InsecureSkipVerify disables cert verification. Mutually exclusive.
type BuildRegistryTLS struct {
	CABundlePEM        string `json:"ca_bundle_pem,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

type BuildImportReferer struct {
	Enabled   bool   `json:"enabled,omitempty"`
	Fallback  bool   `json:"fallback,omitempty"`
	Writeback bool   `json:"writeback,omitempty"`
	Owner     string `json:"owner,omitempty"`
	Validity  string `json:"validity,omitempty"`
}

// BuildStep mirrors types.TemplateStep (kept dependency-free here).
type BuildStep struct {
	Type string   `json:"type"`
	Args []string `json:"args,omitempty"`
	// COPY only: FilesHash identifies the uploaded context object; FilesURL is
	// the presigned GET the build sandbox fetches it from (minted per build in
	// BuildSpecFor, TTL covering the whole build).
	FilesHash string `json:"files_hash,omitempty"`
	FilesURL  string `json:"files_url,omitempty"`
}

// BuildPaths is every host artifact/binary path the pipeline shells out to.
type BuildPaths struct {
	Kernel         string `json:"kernel"`
	Runtime        string `json:"runtime"` // guest runtime erofs
	OverlayDiffTpl string `json:"overlay_diff_tpl"`
	BuilderDiffTpl string `json:"builder_diff_tpl"`
	SandboxCtl     string `json:"sandbox_ctl"`
	FlattenCtl     string `json:"flatten_ctl"` // host-side referer-hit validation only
	ManifestConfig string `json:"manifest_config"`
}

// BuildNet is the ONE pre-attached vswitch slot the build's phase
// sandboxes reuse sequentially (the tapfd handoff re-acquires the same
// port's queue fd each boot).
type BuildNet struct {
	TapFD    TapFDConfig `json:"tapfd"`
	MAC      string      `json:"mac"`
	InnerIP  string      `json:"inner_ip"` // CIDR
	Nexthop  string      `json:"nexthop"`
	Hostname string      `json:"hostname"`
	DNS      []string    `json:"dns,omitempty"`
}

type TapFDConfig struct {
	Exec    []string `json:"exec,omitempty"`
	Socket  string   `json:"socket,omitempty"`
	Request string   `json:"request,omitempty"`
	Timeout string   `json:"timeout,omitempty"`
}

// BuildTimeouts are per-phase budgets in seconds. The absolute deadline covers
// task/host preparation, pipeline execution, and result reporting; it excludes
// the conductor's separately bounded fencing and cleanup allowance.
type BuildTimeouts struct {
	PullSec                  int   `json:"pull_sec"`
	StepSec                  int   `json:"step_sec"`
	ReadySec                 int   `json:"ready_sec"`
	TotalSec                 int   `json:"total_sec"`
	AbsoluteDeadlineUnixNano int64 `json:"absolute_deadline_unix_nano,omitempty"`
}

// AdminKeyInfo is one tenant key-pair allowlist entry. Only complete
// fingerprints are returned; secret material never leaves the daemon.
type AdminKeyInfo struct {
	APISecretFingerprint   string `json:"api_secret_fingerprint"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	Label                  string `json:"label"`
	CreatedUnix            int64  `json:"created_unix"`
	ExpiresUnix            int64  `json:"expires_unix"` // 0 = never expires
}

// Admin is the tenant key-pair allowlist management exposed by the admin plane.
type Admin interface {
	AddKeyPair(ctx context.Context, manifestKey, apiSecret, label string, ttlSec int64, registryAuth string) (added bool, apiFP, manifestFP string, err error)
	RemoveKeyPair(ctx context.Context, manifestKey, apiSecret string) (removed bool, apiFP, manifestFP string, err error)
	HasKeyPair(ctx context.Context, manifestKey, apiSecret string) (present bool, apiFP, manifestFP string, err error)
	ListKeyPairs(ctx context.Context) ([]AdminKeyInfo, error)
}

// MMDSRouteSecretAdmin mutates sandbox-owned opaque values. Content type is
// deliberately absent: it belongs to the immutable route declaration.
type MMDSRouteSecretAdmin interface {
	PutMMDSRouteSecretValue(ctx context.Context, sandboxID, name string, value []byte) error
	DeleteMMDSRouteSecretValue(ctx context.Context, sandboxID, name string) error
}

// BuilderAdmissionAdmin exposes a single consistent snapshot of the durable
// registration/execution ledgers. It contains no tenant credentials or host
// paths and is restricted by the same local admin gate as manifest-key status.
type BuilderAdmissionAdmin interface {
	BuilderAdmissionStatus(ctx context.Context) (BuilderAdmissionStatus, error)
}

type BuildAdmissionHeadroom struct {
	MaxBuilds *int64 `json:"max_builds,omitempty"`
	CPU       *int64 `json:"cpu_milli,omitempty"`
	Memory    *int64 `json:"memory_bytes,omitempty"`
	Storage   *int64 `json:"storage_bytes,omitempty"`
}

type BuildAdmissionLevelStatus struct {
	Configured types.BuildAdmissionLimit `json:"configured"`
	UsedBuilds int64                     `json:"used_builds"`
	Used       types.BuildResources      `json:"used_resources"`
	Available  BuildAdmissionHeadroom    `json:"available"`
}

type BuilderAdmissionStatus struct {
	Registration        BuildAdmissionLevelStatus `json:"registration"`
	Execution           BuildAdmissionLevelStatus `json:"execution"`
	WaitingBuilds       int64                     `json:"waiting_builds"`
	OldestWaitAgeSec    int64                     `json:"oldest_wait_age_seconds,omitempty"`
	RegistrationReject  map[string]int64          `json:"registration_rejections,omitempty"`
	ExecutionReject     map[string]int64          `json:"execution_rejections,omitempty"`
	ExecutionWouldWait  int64                     `json:"execution_would_wait"`
	RegistrationExpired int64                     `json:"registration_expired"`
	QueueExpired        int64                     `json:"queue_expired"`
}

// AdminKeyRequest / AdminKeyResponse are the admin-plane add/remove/check messages.
type AdminKeyRequest struct {
	Op           string `json:"op"` // add | remove | check
	ManifestKey  string `json:"manifest_key"`
	APISecret    string `json:"api_secret,omitempty"`
	Label        string `json:"label,omitempty"`
	TTLSeconds   int64  `json:"ttl_seconds,omitempty"`   // add: 0 = never expires
	RegistryAuth string `json:"registry_auth,omitempty"` // add: tenant-default docker config.json
}

type AdminKeyResponse struct {
	Op                     string `json:"op"`
	APISecretFingerprint   string `json:"api_secret_fingerprint,omitempty"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint,omitempty"`
	Status                 string `json:"status"` // added | refreshed | removed | absent | present
	Error                  string `json:"error,omitempty"`
}

// Deps wires the planes for New.
type Deps struct {
	Provider                     Provider // exact-run task bootstrap/prepare and reports
	Admin                        Admin    // admin plane (manifest-key allowlist)
	MMDSRouteSecretAdmin         MMDSRouteSecretAdmin
	BuilderAdmissionAdmin        BuilderAdmissionAdmin
	MaxMMDSRouteSecretValueBytes int
	API                          http.Handler     // api plane (e2b control plane + export/import); the fallback
	AdminPidfile                 string           // optional PID allowlist gating the admin plane ("" => socket perms only)
	RouteSource                  routesync.Source // plugin plane: route authority a subscriber streams from (nil => plane off)
	Plugins                      *Registry        // plugin plane: live registration registry and Proxy route barrier
	PluginPidfile                string           // optional PID allowlist gating the plugin plane ("" => socket perms only)
}

type Server struct {
	path string
	deps Deps
	log  *slog.Logger
}

func New(path string, deps Deps, log *slog.Logger) *Server {
	return &Server{path: path, deps: deps, log: log}
}

// peerPIDKey carries the SO_PEERCRED peer pid (captured at accept) into handlers.
type peerPIDKey struct{}

func peerFrom(ctx context.Context) (int, bool) {
	v, ok := ctx.Value(peerPIDKey{}).(int)
	return v, ok
}

// Serve binds the UDS (0600) and serves all control planes over h2c until ctx ends.
func (s *Server) Serve(ctx context.Context) error {
	return s.ServeReady(ctx, nil)
}

func (s *Server) ServeReady(ctx context.Context, ready chan<- struct{}) error {
	_ = os.Remove(s.path)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: s.path, Net: "unix"})
	if err != nil {
		return fmt.Errorf("configsock: listen %s: %w", s.path, err)
	}
	if err := os.Chmod(s.path, 0o600); err != nil {
		ln.Close()
		return fmt.Errorf("configsock: chmod: %w", err)
	}
	if ready != nil {
		close(ready)
	}
	srv := &http.Server{
		Handler:           h2c.NewHandler(s.router(), &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
		// Capture the connecting process's pid via SO_PEERCRED at accept time so
		// every request on the connection can be authorized by peer pid.
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			if uc, ok := c.(*net.UnixConn); ok {
				if pid, err := unixcred.PeerPID(uc); err == nil {
					return context.WithValue(ctx, peerPIDKey{}, pid)
				}
			}
			return ctx
		},
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *Server) router() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+PathTaskSandboxBootstrap, s.handleSandboxBootstrap)
	mux.HandleFunc("POST "+PathTaskSandboxPrepare, s.handleSandboxPrepare)
	mux.HandleFunc("POST "+PathTaskBuildBootstrap, s.handleBuildBootstrap)
	mux.HandleFunc("POST "+PathTaskBuildPrepare, s.handleBuildPrepare)
	mux.HandleFunc("POST "+PathRunAssignment, s.handleRunAssignment)
	mux.HandleFunc("POST "+PathRunBuildResult, s.handleBuildResult)
	mux.HandleFunc("POST "+PathRunBuildPhase, s.handleBuildPhase)
	mux.HandleFunc(PathAdminManifestKey, s.handleAdminKeys) // GET=list, POST=add/remove/check
	if s.deps.BuilderAdmissionAdmin != nil {
		mux.HandleFunc("GET "+PathAdminBuilderAdmission, s.handleAdminBuilderAdmission)
	}
	if s.deps.MMDSRouteSecretAdmin != nil {
		mux.HandleFunc(PathAdminMMDSRouteSecretPut, s.handleAdminMMDSRouteSecretPut)
		mux.HandleFunc(PathAdminMMDSRouteSecretDelete, s.handleAdminMMDSRouteSecretDelete)
	}
	if s.deps.RouteSource != nil && s.deps.Plugins != nil {
		mux.HandleFunc(routesync.PluginRegisterPattern, s.handlePluginRegister) // plugin plane: register + route stream
	}
	// Everything else is the api plane (e2b control plane + export/import), authed
	// by X-API-KEY inside the api handler. The "/internal/" routes above are more
	// specific, so they win over this catch-all.
	if s.deps.API != nil {
		mux.Handle("/", s.deps.API)
	}
	return mux
}

// --- task plane ---

func (s *Server) handleSandboxBootstrap(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &SandboxTaskSpec{Error: "no peer credentials"})
		return
	}
	var req SandboxTaskRequest
	if status := decodeTaskRequest(w, r, taskBootstrapRequestMaxBytes, &req); status != 0 || req.SandboxID == "" || req.RunID == "" || req.Version != ArtifactPrepareSchemaVersion {
		if status == 0 {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, &SandboxTaskSpec{Error: "bad request"})
		return
	}
	auth, found, err := s.deps.Provider.SandboxTaskAuth(r.Context(), req.SandboxID, req.RunID)
	if err != nil {
		s.log.Warn("configsock sandbox auth provider", "sid", req.SandboxID, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &SandboxTaskSpec{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &SandboxTaskSpec{Error: "unknown task"})
		return
	}
	if !s.taskAuthed("sandbox:"+req.SandboxID+":"+req.RunID, auth.PidFile, peer) {
		writeJSON(w, http.StatusForbidden, &SandboxTaskSpec{Error: "not authorized"})
		return
	}
	// This provider may decrypt MANIFEST_KEY. It is deliberately unreachable
	// until the exact-run peer identity above has succeeded.
	spec, found, err := s.deps.Provider.SandboxTaskSpecFor(r.Context(), req.SandboxID, req.RunID)
	if err != nil {
		s.log.Warn("configsock sandbox bootstrap provider", "sid", req.SandboxID, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &SandboxTaskSpec{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &SandboxTaskSpec{Error: "unknown task"})
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

func (s *Server) handleSandboxPrepare(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &ArtifactPrepareResponse{Error: "no peer credentials"})
		return
	}
	var req ArtifactPrepareRequest
	if status := decodeTaskRequest(w, r, taskPrepareRequestMaxBytes, &req); status != 0 || req.SandboxID == "" || req.RunID == "" {
		if status == 0 {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, &ArtifactPrepareResponse{Error: "bad request"})
		return
	}
	auth, found, err := s.deps.Provider.SandboxTaskAuth(r.Context(), req.SandboxID, req.RunID)
	if err != nil {
		s.log.Warn("configsock sandbox prepare auth provider", "sid", req.SandboxID, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &ArtifactPrepareResponse{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &ArtifactPrepareResponse{Error: "unknown task"})
		return
	}
	if !s.taskAuthed("sandbox-prepare:"+req.SandboxID+":"+req.RunID, auth.PidFile, peer) {
		writeJSON(w, http.StatusForbidden, &ArtifactPrepareResponse{Error: "not authorized"})
		return
	}
	final, err := s.deps.Provider.CompleteSandboxPrepare(r.Context(), req.SandboxID, req.RunID, req.Summary)
	if err != nil {
		status := http.StatusInternalServerError
		if IsArtifactPrepareRejection(err) {
			status = http.StatusConflict
		}
		writeJSON(w, status, &ArtifactPrepareResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, &ArtifactPrepareResponse{Final: final})
}

// handleBuildBootstrap authenticates the exact assigned task before invoking
// the secret-bearing provider. In particular, MANIFEST_KEY cannot be loaded on
// an unauthorized request.
func (s *Server) handleBuildBootstrap(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &BuildTaskSpec{Error: "no peer credentials"})
		return
	}
	var req BuildTaskRequest
	if status := decodeTaskRequest(w, r, taskBootstrapRequestMaxBytes, &req); status != 0 || req.BuildID == "" || req.RunID == "" || req.Version != BuildTaskSchemaVersion {
		if status == 0 {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, &BuildTaskSpec{Error: "bad request"})
		return
	}
	auth, found, err := s.deps.Provider.BuildTaskAuth(r.Context(), req.BuildID, req.RunID)
	if err != nil {
		s.log.Warn("configsock build auth provider", "build_id", req.BuildID, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &BuildTaskSpec{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &BuildTaskSpec{Error: "unknown build run"})
		return
	}
	if !s.taskAuthed("build:"+req.BuildID+":"+req.RunID, auth.PidFile, peer) {
		writeJSON(w, http.StatusForbidden, &BuildTaskSpec{Error: "not authorized"})
		return
	}
	spec, found, err := s.deps.Provider.BuildTaskSpecFor(r.Context(), req.BuildID, req.RunID)
	if err != nil {
		s.log.Warn("configsock build bootstrap provider", "build_id", req.BuildID, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &BuildTaskSpec{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &BuildTaskSpec{Error: "unknown build run"})
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

func (s *Server) handleBuildPrepare(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &BuildPrepareResponse{Error: "no peer credentials"})
		return
	}
	var req BuildPrepareRequest
	if status := decodeTaskRequest(w, r, taskPrepareRequestMaxBytes, &req); status != 0 || req.BuildID == "" || req.RunID == "" || req.Version != BuildTaskSchemaVersion {
		if status == 0 {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, &BuildPrepareResponse{Error: "bad request"})
		return
	}
	auth, found, err := s.deps.Provider.BuildTaskAuth(r.Context(), req.BuildID, req.RunID)
	if err != nil {
		s.log.Warn("configsock build prepare auth provider", "build_id", req.BuildID, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &BuildPrepareResponse{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &BuildPrepareResponse{Error: "unknown build run"})
		return
	}
	if !s.taskAuthed("build-prepare:"+req.BuildID+":"+req.RunID, auth.PidFile, peer) {
		writeJSON(w, http.StatusForbidden, &BuildPrepareResponse{Error: "not authorized"})
		return
	}
	final, err := s.deps.Provider.CompleteBuildPrepare(r.Context(), req.BuildID, req.RunID, req.Summary)
	if err != nil {
		s.log.Warn("configsock build prepare provider", "build_id", req.BuildID, "run_id", req.RunID, "err", err)
		status := http.StatusInternalServerError
		if IsBuildPrepareRejection(err) {
			status = http.StatusConflict
		}
		writeJSON(w, status, &BuildPrepareResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, &BuildPrepareResponse{Final: final})
}

func decodeTaskRequest(w http.ResponseWriter, r *http.Request, maxBytes int64, out any) int {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return http.StatusRequestEntityTooLarge
		}
		return http.StatusBadRequest
	}
	if err := strictjson.Decode(raw, out); err != nil {
		return http.StatusBadRequest
	}
	return 0
}

func (s *Server) handleRunAssignment(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &AssignmentResponse{Error: "no peer credentials"})
		return
	}
	var req AssignmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Kind == "" || req.RunID == "" {
		writeJSON(w, http.StatusBadRequest, &AssignmentResponse{Error: "bad request"})
		return
	}
	pidFile, ok := s.deps.Provider.RunPidFile(req.Kind, req.RunID)
	if !ok {
		writeJSON(w, http.StatusNotFound, &AssignmentResponse{Error: "unknown run"})
		return
	}
	if !s.taskAuthed(req.Kind+":"+req.RunID, pidFile, peer) {
		writeJSON(w, http.StatusForbidden, &AssignmentResponse{Error: "not authorized"})
		return
	}
	taskID, ok, err := s.deps.Provider.WaitAssignment(r.Context(), req.Kind, req.RunID)
	if err != nil {
		s.log.Warn("configsock assignment", "kind", req.Kind, "run_id", req.RunID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &AssignmentResponse{Error: err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, &AssignmentResponse{Error: "unknown run"})
		return
	}
	writeJSON(w, http.StatusOK, &AssignmentResponse{Kind: req.Kind, RunID: req.RunID, TaskID: taskID})
}

func (s *Server) handleBuildResult(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &BuildResultResponse{Error: "no peer credentials"})
		return
	}
	var req BuildResultRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.RunID == "" || req.BuildID == "" {
		writeJSON(w, http.StatusBadRequest, &BuildResultResponse{Error: "bad request"})
		return
	}
	pidFile, ok := s.deps.Provider.RunPidFile("build", req.RunID)
	if !ok {
		writeJSON(w, http.StatusNotFound, &BuildResultResponse{Error: "unknown run"})
		return
	}
	if !s.taskAuthed("build-result:"+req.RunID, pidFile, peer) {
		writeJSON(w, http.StatusForbidden, &BuildResultResponse{Error: "not authorized"})
		return
	}
	if err := s.deps.Provider.PostBuildResult(r.Context(), req.RunID, req.BuildID, req.Result); err != nil {
		s.log.Warn("configsock build result", "run_id", req.RunID, "build_id", req.BuildID, "err", err)
		status := http.StatusInternalServerError
		if IsBuildReportRejection(err) {
			status = http.StatusConflict
		}
		writeJSON(w, status, &BuildResultResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, &BuildResultResponse{})
}

func (s *Server) handleBuildPhase(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &BuildPhaseResponse{Error: "no peer credentials"})
		return
	}
	var req BuildPhaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
		req.RunID == "" || req.BuildID == "" || req.SandboxID == "" ||
		(req.Phase != "a" && req.Phase != "b" && req.Phase != "c") ||
		(req.State != "starting" && req.State != "finished" && req.State != "failed") {
		writeJSON(w, http.StatusBadRequest, &BuildPhaseResponse{Error: "bad request"})
		return
	}
	pidFile, ok := s.deps.Provider.RunPidFile("build", req.RunID)
	if !ok {
		writeJSON(w, http.StatusNotFound, &BuildPhaseResponse{Error: "unknown run"})
		return
	}
	if !s.taskAuthed("build-phase:"+req.RunID, pidFile, peer) {
		writeJSON(w, http.StatusForbidden, &BuildPhaseResponse{Error: "not authorized"})
		return
	}
	if err := s.deps.Provider.PostBuildPhase(r.Context(), req.RunID, req.BuildID, req.Phase, req.SandboxID, req.State); err != nil {
		s.log.Warn("configsock build phase", "run_id", req.RunID, "build_id", req.BuildID,
			"phase", req.Phase, "state", req.State, "err", err)
		status := http.StatusInternalServerError
		if IsBuildReportRejection(err) {
			status = http.StatusConflict
		}
		writeJSON(w, status, &BuildPhaseResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, &BuildPhaseResponse{})
}

// taskAuthed verifies the connecting pid matches the id's pidfile (SO_PEERCRED).
func (s *Server) taskAuthed(id, pidFile string, peer int) bool {
	want, err := readPID(pidFile)
	if err != nil {
		s.log.Warn("configsock pidfile", "id", id, "err", err)
		return false
	}
	if want != peer {
		s.log.Warn("configsock pid mismatch", "id", id, "want", want, "peer", peer)
		return false
	}
	return true
}

// --- admin plane ---

func (s *Server) handleAdminKeys(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		writeJSON(w, http.StatusForbidden, &AdminKeyResponse{Error: "not authorized (admin)"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		infos, err := s.deps.Admin.ListKeyPairs(r.Context())
		if err != nil {
			s.log.Warn("configsock admin list", "err", err)
			writeJSON(w, http.StatusInternalServerError, &AdminKeyResponse{Error: "internal error"})
			return
		}
		writeJSON(w, http.StatusOK, infos)
	case http.MethodPost:
		var req AdminKeyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ManifestKey == "" {
			writeJSON(w, http.StatusBadRequest, &AdminKeyResponse{Op: req.Op, Error: "manifest_key required"})
			return
		}
		resp := s.adminOp(r.Context(), req)
		code := http.StatusOK
		if resp.Error != "" {
			code = http.StatusBadRequest
		}
		writeJSON(w, code, resp)
	default:
		writeJSON(w, http.StatusMethodNotAllowed, &AdminKeyResponse{Error: "method not allowed"})
	}
}

func (s *Server) handleAdminBuilderAdmission(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "not authorized (admin)"})
		return
	}
	status, err := s.deps.BuilderAdmissionAdmin.BuilderAdmissionStatus(r.Context())
	if err != nil {
		s.log.Warn("configsock builder admission status", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) adminOp(ctx context.Context, req AdminKeyRequest) *AdminKeyResponse {
	out := &AdminKeyResponse{Op: req.Op}
	switch req.Op {
	case "add":
		added, apiFP, manifestFP, err := s.deps.Admin.AddKeyPair(ctx, req.ManifestKey, req.APISecret, req.Label, req.TTLSeconds, req.RegistryAuth)
		if err != nil {
			out.Error = err.Error()
			return out
		}
		out.APISecretFingerprint, out.ManifestKeyFingerprint = apiFP, manifestFP
		out.Status = statusWord(added, "added", "refreshed")
	case "remove":
		removed, apiFP, manifestFP, err := s.deps.Admin.RemoveKeyPair(ctx, req.ManifestKey, req.APISecret)
		if err != nil {
			out.Error = err.Error()
			return out
		}
		out.APISecretFingerprint, out.ManifestKeyFingerprint = apiFP, manifestFP
		out.Status = statusWord(removed, "removed", "absent")
	case "check":
		present, apiFP, manifestFP, err := s.deps.Admin.HasKeyPair(ctx, req.ManifestKey, req.APISecret)
		if err != nil {
			out.Error = err.Error()
			return out
		}
		out.APISecretFingerprint, out.ManifestKeyFingerprint = apiFP, manifestFP
		out.Status = statusWord(present, "present", "absent")
	default:
		out.Error = "unknown op (want add|remove|check)"
	}
	return out
}

func (s *Server) handleAdminMMDSRouteSecretPut(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		http.Error(w, "not authorized", http.StatusForbidden)
		return
	}
	sandboxID, name := r.PathValue("id"), r.PathValue("name")
	if sandboxID == "" || name == "" || r.URL.RawQuery != "" || r.URL.ForceQuery {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	limit := s.deps.MaxMMDSRouteSecretValueBytes
	if limit <= 0 {
		limit = 16 * 1024
	}
	r.Body = http.MaxBytesReader(w, r.Body, int64(limit))
	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if err := s.deps.MMDSRouteSecretAdmin.PutMMDSRouteSecretValue(r.Context(), sandboxID, name, value); err != nil {
		s.log.Warn("configsock admin MMDS route secret PUT", "peer", peer, "sandbox_id", sandboxID, "name", name, "err", err)
		http.Error(w, "request failed", mmdsRouteSecretAdminErrorCode(err))
		return
	}
	s.log.Info("configsock admin MMDS route secret PUT", "peer", peer, "sandbox_id", sandboxID, "name", name)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleAdminMMDSRouteSecretDelete(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		http.Error(w, "not authorized", http.StatusForbidden)
		return
	}
	sandboxID, name := r.PathValue("id"), r.PathValue("name")
	if sandboxID == "" || name == "" || r.URL.RawQuery != "" || r.URL.ForceQuery {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if err := s.deps.MMDSRouteSecretAdmin.DeleteMMDSRouteSecretValue(r.Context(), sandboxID, name); err != nil {
		s.log.Warn("configsock admin MMDS route secret DELETE", "peer", peer, "sandbox_id", sandboxID, "name", name, "err", err)
		http.Error(w, "request failed", mmdsRouteSecretAdminErrorCode(err))
		return
	}
	s.log.Info("configsock admin MMDS route secret DELETE", "peer", peer, "sandbox_id", sandboxID, "name", name)
	w.WriteHeader(http.StatusNoContent)
}

func mmdsRouteSecretAdminErrorCode(err error) int {
	switch {
	case errors.Is(err, api.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, api.ErrBadRequest):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

// adminAuthed gates the admin plane: when admin_pidfile is set the peer pid must be
// listed; otherwise the socket's 0600 perms (same uid / root) are the only gate.
func (s *Server) adminAuthed(peer int) bool {
	if s.deps.AdminPidfile == "" {
		return true
	}
	pids, err := readPIDs(s.deps.AdminPidfile)
	if err != nil {
		s.log.Warn("configsock admin pidfile", "path", s.deps.AdminPidfile, "err", err)
		return false
	}
	if slices.Contains(pids, peer) {
		return true
	}
	s.log.Warn("configsock admin pid not allowlisted", "peer", peer, "pidfile", s.deps.AdminPidfile)
	return false
}

func statusWord(b bool, yes, no string) string {
	if b {
		return yes
	}
	return no
}

// --- shared helpers ---

func readPID(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(b)))
}

// readPIDs parses a multi-line PID allowlist (one PID per line; blank lines and
// '#' comments ignored).
func readPIDs(path string) ([]int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pids []int
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("configsock: admin pidfile %s: bad pid %q", path, line)
		}
		pids = append(pids, p)
	}
	return pids, nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

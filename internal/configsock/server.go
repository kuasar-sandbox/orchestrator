// Package configsock implements the orchestrator's local control socket: a single
// UDS that multiplexes several planes over HTTP (h2c, with HTTP/1.1 fallback), each
// with its own authentication:
//
//   - run    (POST /internal/run/assignment, /internal/run/build-result):
//     prestarted run-id units wait for their sandbox/build assignment; run-builder
//     posts its result back here. Authed by SO_PEERCRED peer pid == the run-id
//     pidfile (/run/sandbox/runs/<run-id>.pid).
//   - task   (POST /internal/task/launchspec, /internal/task/buildspec):
//     assigned tasks fetch their LaunchSpec or BuildSpec by business id. Authed by
//     SO_PEERCRED peer pid == the task pidfile (/run/sandbox/<id>/<id>.pid).
//   - admin  (/internal/admin/manifest-keys): manifest-key allowlist management.
//     Authed by SO_PEERCRED peer pid ∈ admin_pidfile (or, when that is unset, by the
//     socket's 0600 permissions alone = same uid / root).
//   - plugin (PUT /internal/plugin/{id}/register): a subscriber (an external proxy
//     worker, or a route observer such as the platform agent) registers its
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
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sys/unix"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// Plane paths. The task/admin planes live under /internal/ (a prefix the e2b SDK
// never uses); every other path falls through to the api handler.
const (
	PathTaskLaunchSpec   = "/internal/task/launchspec"
	PathTaskBuildSpec    = "/internal/task/buildspec"
	PathRunAssignment    = "/internal/run/assignment"
	PathRunBuildResult   = "/internal/run/build-result"
	PathAdminManifestKey = "/internal/admin/manifest-keys"
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

// Request is what a task client (node-ctl run-sandbox / run-builder) sends.
type Request struct {
	ConfigID string `json:"config_id"`
	Version  int    `json:"version"`
}

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

type BuildResult struct {
	ImageKey    string `json:"image_key,omitempty"`
	SnapshotKey string `json:"snapshot_key,omitempty"`
	StartCmd    string `json:"start_cmd,omitempty"`
	ReadyCmd    string `json:"ready_cmd,omitempty"`
	Error       string `json:"error,omitempty"`
}

type BuildResultRequest struct {
	RunID   string      `json:"run_id"`
	BuildID string      `json:"build_id"`
	Result  BuildResult `json:"result"`
}

type BuildResultResponse struct {
	Error string `json:"error,omitempty"`
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
	LaunchSpecFor(ctx context.Context, configID string) (resp *LaunchSpec, pidFile string, ok bool, err error)
	// BuildSpecFor resolves "build:<bid>" to the build pipeline spec the
	// run-builder orchestrates (it does NOT exec-replace — the spec is a
	// work order, not a launch).
	BuildSpecFor(ctx context.Context, configID string) (resp *BuildSpec, pidFile string, ok bool, err error)
	RunPidFile(kind, runID string) (pidFile string, ok bool)
	WaitAssignment(ctx context.Context, kind, runID string) (taskID string, ok bool, err error)
	PostBuildResult(ctx context.Context, runID, buildID string, result BuildResult) error
}

// BuildSpec is the work order node-ctl run-builder fetches for
// "build:<bid>": everything the three-phase pipeline (import → steps →
// template snapshot) needs. Secrets (manifest key, tenant registry
// creds) ride here over the socket, never on disk.
type BuildSpec struct {
	BuildID          string             `json:"build_id"`
	Profile          string             `json:"profile"`
	RunID            string             `json:"run_id,omitempty"`
	Workdir          string             `json:"workdir"` // build scratch dir (artifacts, run roots)
	FromImage        string             `json:"from_image,omitempty"`
	FromTemplate     string             `json:"from_template,omitempty"` // snapshot manifest key (hex) of the base template
	FromTemplateKind string             `json:"from_template_kind,omitempty"`
	Steps            []BuildStep        `json:"steps,omitempty"`
	StartCmd         string             `json:"start_cmd,omitempty"`
	ReadyCmd         string             `json:"ready_cmd,omitempty"`
	Env              map[string]string  `json:"env,omitempty"` // secret env: MANIFEST_KEY + FLATTEN_REGISTRY_* (guest exec gets only the FLATTEN_* subset)
	Paths            BuildPaths         `json:"paths"`
	Net              BuildNet           `json:"net"`
	VCPU             int                `json:"vcpu"`
	Memory           string             `json:"memory"`
	MMDSEnabled      bool               `json:"mmds_enabled"`
	EnvdToken        string             `json:"envd_token,omitempty"` // phase C envd /init token (mmds posture)
	Insecure         bool               `json:"insecure,omitempty"`   // registry plain-HTTP/skip-TLS
	Platform         string             `json:"platform,omitempty"`
	ImportReferer    BuildImportReferer `json:"import_referer,omitempty"`
	Timeouts         BuildTimeouts      `json:"timeouts"`
	Error            string             `json:"error,omitempty"`
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
	FlattenCtl     string `json:"flatten_ctl"` // host-side: info --json over local artifacts
	ManifestCtl    string `json:"manifest_ctl"`
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

// BuildTimeouts are per-phase budgets in seconds.
type BuildTimeouts struct {
	PullSec  int `json:"pull_sec"`
	StepSec  int `json:"step_sec"`
	ReadySec int `json:"ready_sec"`
	TotalSec int `json:"total_sec"`
}

// AdminKeyInfo is one manifest-key allowlist entry (fingerprint only — never the key).
type AdminKeyInfo struct {
	Fingerprint string `json:"fingerprint"`
	Label       string `json:"label"`
	CreatedUnix int64  `json:"created_unix"`
	ExpiresUnix int64  `json:"expires_unix"` // 0 = never expires
}

// Admin is the manifest-key allowlist management the admin plane exposes. Every
// method returns the key's fingerprint (24-hex) so the daemon never echoes key
// material back to the client.
type Admin interface {
	AddManifestKey(ctx context.Context, key, label string, ttlSec int64, registryAuth string) (added bool, fp string, err error)
	RemoveManifestKey(ctx context.Context, key string) (removed bool, fp string, err error)
	HasManifestKey(ctx context.Context, key string) (present bool, fp string, err error)
	ListManifestKeys(ctx context.Context) ([]AdminKeyInfo, error)
}

// AdminKeyRequest / AdminKeyResponse are the admin-plane add/remove/check messages.
type AdminKeyRequest struct {
	Op           string `json:"op"` // add | remove | check
	Key          string `json:"key"`
	Label        string `json:"label,omitempty"`
	TTLSeconds   int64  `json:"ttl_seconds,omitempty"`   // add: 0 = never expires
	RegistryAuth string `json:"registry_auth,omitempty"` // add: tenant-default docker config.json
}

type AdminKeyResponse struct {
	Op          string `json:"op"`
	Fingerprint string `json:"fingerprint"`
	Status      string `json:"status"` // added | exists | removed | absent | present
	Error       string `json:"error,omitempty"`
}

// Deps wires the planes for New.
type Deps struct {
	Provider      Provider         // task plane (LaunchSpec by config-id)
	Admin         Admin            // admin plane (manifest-key allowlist)
	API           http.Handler     // api plane (e2b control plane + export/import); the fallback
	AdminPidfile  string           // optional PID allowlist gating the admin plane ("" => socket perms only)
	RouteSource   routesync.Source // plugin plane: route authority a subscriber streams from (nil => plane off)
	Plugins       *Registry        // plugin plane: live registration registry (shared with proxyForwarder)
	PluginPidfile string           // optional PID allowlist gating the plugin plane ("" => socket perms only)
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
				if pid, err := peerPID(uc); err == nil {
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
	mux.HandleFunc("POST "+PathTaskLaunchSpec, s.handleTask)
	mux.HandleFunc("POST "+PathTaskBuildSpec, s.handleBuildTask)
	mux.HandleFunc("POST "+PathRunAssignment, s.handleRunAssignment)
	mux.HandleFunc("POST "+PathRunBuildResult, s.handleBuildResult)
	mux.HandleFunc(PathAdminManifestKey, s.handleAdminKeys) // GET=list, POST=add/remove/check
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

func (s *Server) handleTask(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &LaunchSpec{Error: "no peer credentials"})
		return
	}
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ConfigID == "" {
		writeJSON(w, http.StatusBadRequest, &LaunchSpec{Error: "bad request"})
		return
	}
	spec, pidFile, found, err := s.deps.Provider.LaunchSpecFor(r.Context(), req.ConfigID)
	if err != nil {
		s.log.Warn("configsock provider", "id", req.ConfigID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &LaunchSpec{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &LaunchSpec{Error: "unknown task"})
		return
	}
	if !s.taskAuthed(req.ConfigID, pidFile, peer) {
		writeJSON(w, http.StatusForbidden, &LaunchSpec{Error: "not authorized"})
		return
	}
	writeJSON(w, http.StatusOK, spec)
}

// handleBuildTask serves the build-spec plane: same auth as handleTask
// (SO_PEERCRED pid == the build's pidfile), different payload.
func (s *Server) handleBuildTask(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, &BuildSpec{Error: "no peer credentials"})
		return
	}
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ConfigID == "" {
		writeJSON(w, http.StatusBadRequest, &BuildSpec{Error: "bad request"})
		return
	}
	spec, pidFile, found, err := s.deps.Provider.BuildSpecFor(r.Context(), req.ConfigID)
	if err != nil {
		s.log.Warn("configsock build provider", "id", req.ConfigID, "err", err)
		writeJSON(w, http.StatusInternalServerError, &BuildSpec{Error: "internal error"})
		return
	}
	if !found {
		writeJSON(w, http.StatusNotFound, &BuildSpec{Error: "unknown build"})
		return
	}
	if !s.taskAuthed(req.ConfigID, pidFile, peer) {
		writeJSON(w, http.StatusForbidden, &BuildSpec{Error: "not authorized"})
		return
	}
	writeJSON(w, http.StatusOK, spec)
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
		writeJSON(w, http.StatusInternalServerError, &BuildResultResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, &BuildResultResponse{})
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
		infos, err := s.deps.Admin.ListManifestKeys(r.Context())
		if err != nil {
			s.log.Warn("configsock admin list", "err", err)
			writeJSON(w, http.StatusInternalServerError, &AdminKeyResponse{Error: "internal error"})
			return
		}
		writeJSON(w, http.StatusOK, infos)
	case http.MethodPost:
		var req AdminKeyRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Key == "" {
			writeJSON(w, http.StatusBadRequest, &AdminKeyResponse{Op: req.Op, Error: "key required"})
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

func (s *Server) adminOp(ctx context.Context, req AdminKeyRequest) *AdminKeyResponse {
	out := &AdminKeyResponse{Op: req.Op}
	switch req.Op {
	case "add":
		added, fp, err := s.deps.Admin.AddManifestKey(ctx, req.Key, req.Label, req.TTLSeconds, req.RegistryAuth)
		if err != nil {
			out.Error = err.Error()
			return out
		}
		out.Fingerprint, out.Status = fp, statusWord(added, "added", "refreshed")
	case "remove":
		removed, fp, err := s.deps.Admin.RemoveManifestKey(ctx, req.Key)
		if err != nil {
			out.Error = err.Error()
			return out
		}
		out.Fingerprint, out.Status = fp, statusWord(removed, "removed", "absent")
	case "check":
		present, fp, err := s.deps.Admin.HasManifestKey(ctx, req.Key)
		if err != nil {
			out.Error = err.Error()
			return out
		}
		out.Fingerprint, out.Status = fp, statusWord(present, "present", "absent")
	default:
		out.Error = "unknown op (want add|remove|check)"
	}
	return out
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

// peerPID returns the connecting process's pid via SO_PEERCRED.
func peerPID(c *net.UnixConn) (int, error) {
	raw, err := c.SyscallConn()
	if err != nil {
		return 0, err
	}
	var ucred *unix.Ucred
	var serr error
	if cerr := raw.Control(func(fd uintptr) {
		ucred, serr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); cerr != nil {
		return 0, cerr
	}
	if serr != nil {
		return 0, serr
	}
	return int(ucred.Pid), nil
}

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

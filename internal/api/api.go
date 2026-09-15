// Package api implements the e2b-compatible control-plane REST surface that the
// unmodified e2b SDK calls. It validates X-API-KEY and delegates to Core.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/execsession"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// configHeaderNs maps each X-Kuasar-Sandbox-<Ns> request header to the namespaced
// metadata key it normalizes into. Headers are an alternate config-injection surface
// (create + template build); on conflict with an e2b metadata key of the same
// namespace the header wins. The header value is the same JSON the metadata key holds.
var configHeaderNs = []struct{ header, metaKey string }{
	{"X-Kuasar-Sandbox-Resource", sandboxcfg.NsResource},
	{"X-Kuasar-Sandbox-Traffic", sandboxcfg.NsTraffic},
	{"X-Kuasar-Sandbox-Network", sandboxcfg.NsNetwork},
	{"X-Kuasar-Sandbox-Launch", sandboxcfg.NsLaunch},
	{"X-Kuasar-Sandbox-Init", sandboxcfg.NsInit},
	{"X-Kuasar-Sandbox-Mounts", sandboxcfg.NsMounts},
	{"X-Kuasar-Sandbox-Files", sandboxcfg.NsFiles},
	{"X-Kuasar-Sandbox-Metadata", sandboxcfg.NsMetadata},
}

const (
	builderHeader      = "X-Kuasar-Sandbox-Builder"
	clusterGroupHeader = "X-Kuasar-Sandbox-Group"
	restoreHeader      = "X-Kuasar-Sandbox-Restore"
	credentialsHeader  = "X-Kuasar-Sandbox-Credentials"
	checkpointHeader   = "X-Kuasar-Sandbox-Checkpoint"
	mmdsHeader         = "X-Kuasar-Sandbox-MMDS"
)

func singleOptionalHeader(h http.Header, name string) (*string, error) {
	canonical := http.CanonicalHeaderKey(name)
	values, present := h[canonical]
	if !present {
		return nil, nil
	}
	if len(values) != 1 {
		return nil, fmt.Errorf("%s must appear exactly once", name)
	}
	value := values[0]
	return &value, nil
}

// mergeConfigHeaders folds the X-Kuasar-Sandbox-<Ns> headers into meta. Resource
// and traffic leaves merge independently; every other namespace retains
// whole-value header precedence. Presence is checked explicitly so an empty
// leaf-merged header fails strict parsing instead of disappearing.
func mergeConfigHeaders(meta map[string]string, h http.Header) (map[string]string, error) {
	out, err := sandboxcfg.MergeMetadata(nil, meta)
	if err != nil {
		return nil, err
	}
	for _, m := range configHeaderNs {
		values, present := h[http.CanonicalHeaderKey(m.header)]
		if !present {
			continue
		}
		value := h.Get(m.header)
		if m.metaKey == sandboxcfg.NsResource || m.metaKey == sandboxcfg.NsTraffic {
			if len(values) != 1 {
				return nil, fmt.Errorf("%s must appear exactly once", m.header)
			}
			value = values[0]
		} else if value == "" {
			// Preserve the existing whole-namespace header behavior outside
			// resource: an empty generic config header is absent.
			continue
		}
		out, err = sandboxcfg.MergeMetadata(out, map[string]string{m.metaKey: value})
		if err != nil {
			return nil, fmt.Errorf("%s: %w", m.header, err)
		}
	}
	return out, nil
}

func mergeCreateConfigHeaders(meta map[string]string, h http.Header) (map[string]string, error) {
	var err error
	meta, err = mergeConfigHeaders(meta, h)
	if err != nil {
		return nil, err
	}
	meta, err = mergeIdentityHeader(meta, h)
	if err != nil {
		return nil, err
	}
	for _, item := range []struct{ header, metaKey string }{
		{restoreHeader, sandboxcfg.NsRestore},
		{credentialsHeader, sandboxcfg.NsCredentials},
	} {
		if _, present := h[http.CanonicalHeaderKey(item.header)]; !present {
			continue
		}
		if meta == nil {
			meta = map[string]string{}
		}
		meta[item.metaKey] = h.Get(item.header)
	}
	bodyPolicy := sandboxcfg.SnapshotPolicy{}
	bodyPresent := false
	if raw, ok := meta[sandboxcfg.NsCheckpoint]; ok {
		bodyPresent = true
		var err error
		bodyPolicy, err = sandboxcfg.ParseSnapshotPolicyJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("metadata %s: %w", sandboxcfg.NsCheckpoint, err)
		}
	}
	_, headerPresent := h[http.CanonicalHeaderKey(checkpointHeader)]
	if !bodyPresent && !headerPresent {
		return meta, nil
	}
	policy := bodyPolicy
	if headerPresent {
		headerPolicy, err := sandboxcfg.ParseSnapshotPolicyJSON(h.Get(checkpointHeader))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", checkpointHeader, err)
		}
		policy = sandboxcfg.OverlaySnapshotPolicy(bodyPolicy, headerPolicy)
	}
	if policy.Empty() {
		delete(meta, sandboxcfg.NsCheckpoint)
		return meta, nil
	}
	canonical, err := sandboxcfg.MarshalSnapshotPolicyJSON(policy)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		meta = map[string]string{}
	}
	meta[sandboxcfg.NsCheckpoint] = canonical
	return meta, nil
}

func mergeBuildConfigHeaders(meta map[string]string, h http.Header) (map[string]string, error) {
	var err error
	// Build registration accepts the same Create configuration headers. The
	// core applies target-specific instance/portable validation after target
	// resolution; restore is always rejected there.
	meta, err = mergeCreateConfigHeaders(meta, h)
	if err != nil {
		return nil, err
	}
	if _, present := meta[sandboxcfg.NsIdentity]; present {
		return nil, fmt.Errorf("%s is not valid for template builds", sandboxcfg.NsIdentity)
	}
	if values, present := h[http.CanonicalHeaderKey(builderHeader)]; present {
		if len(values) != 1 {
			return nil, fmt.Errorf("%s must appear exactly once", builderHeader)
		}
		// Builder keeps the existing whole-namespace header precedence, but an
		// invalid lower-priority definition must not disappear behind a valid
		// header. Strict registration input is validated independently at every
		// supplied layer before the selected definition is extracted below.
		if _, supplied := meta[buildcfg.NsBuilder]; supplied {
			if _, _, err := buildcfg.Extract(meta); err != nil {
				return nil, err
			}
		}
		if meta == nil {
			meta = map[string]string{}
		}
		meta[buildcfg.NsBuilder] = values[0]
	}
	return meta, nil
}

const maxBuildRequestBytes = 1 << 20

func decodeBuildRequest(r io.Reader, out any) error {
	raw, err := io.ReadAll(io.LimitReader(r, maxBuildRequestBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxBuildRequestBytes {
		return fmt.Errorf("request body exceeds %d bytes", maxBuildRequestBytes)
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("request body must be a JSON object")
	}
	if err := strictjson.DecodeAllowUnknown(raw, out); err != nil {
		return err
	}
	return nil
}

// ErrAlreadyPaused is returned by Core.Pause when the sandbox is already paused.
var ErrAlreadyPaused = errors.New("already paused")

// ErrSandboxStarting is returned when an operation such as Pause cannot run
// until the accepted asynchronous launch reaches a terminal state.
var ErrSandboxStarting = errors.New("sandbox starting")

// ErrNotFound is returned by Core methods when the sandbox id is unknown.
var ErrNotFound = errors.New("sandbox not found")

// ErrProxyUnavailable means Create could not establish its route-applied
// barrier. It is a temporary admission failure, not an accepted sandbox.
var ErrProxyUnavailable = errors.New("proxy temporarily unavailable")

// ErrExtensionUnavailable is the fixed internal classification for a Hook
// error other than extension.ErrRejected. The original error is logged by the
// orchestrator and never returned over direct or cluster APIs.
var ErrExtensionUnavailable = errors.New("extension temporarily unavailable")

// ErrSandboxChanged reports that authoritative lifecycle state changed while
// an Extension Hook was running outside the lifecycle lock.
var ErrSandboxChanged = errors.New("sandbox changed during extension hook")

// ErrBuildChanged reports that authoritative registration state changed while
// an Extension Hook was running outside the store transaction.
var ErrBuildChanged = errors.New("build changed during extension hook")

// Stats errors have deliberately coarse public mappings. Providers may carry
// richer internal causes, but the API must not expose node topology or worker
// state details.
var (
	ErrStatsUnsupported = errors.New("sandbox stats unsupported")
	ErrStatsConflict    = errors.New("sandbox state has no applicable stats")
	ErrStatsUnavailable = errors.New("sandbox stats temporarily unavailable")
)

// ErrNotAllowed is returned by Create/RegisterBuild when the API key does not
// resolve to an APISecret/ManifestKey pair in the node allowlist (=> 403).
var ErrNotAllowed = errors.New("credential pair not allowed to create")

// ErrFilesUnsupported is returned by FilesUpload / TriggerBuild when a COPY
// step is used but builder.files_storage is unconfigured (=> 501).
var ErrFilesUnsupported = errors.New("COPY build contexts unsupported (builder.files_storage not configured)")

// ErrBuildAdmission means the node cannot accept another durable Build
// definition under its registration limits. It is a definitive no-side-effect
// rejection and maps to 429 for direct callers.
var ErrBuildAdmission = errors.New("builder registration admission capacity exceeded")

// ErrBadRequest maps a Core-side validation failure (e.g. a COPY referencing
// an unuploaded context) to 400.
var ErrBadRequest = errors.New("bad request")

// BuildStateConflictError reports the exact internal state that prevents a
// one-shot build trigger. It is handled only by the trigger endpoint; SDKStatus
// deliberately collapses in-progress states and is not precise enough here.
type BuildStateConflictError struct {
	State types.BuildState
}

func (e *BuildStateConflictError) Error() string {
	return fmt.Sprintf("build cannot be triggered from state %s", e.State)
}

// ErrAlreadyExists is returned when a fresh Create or migration import target exists.
var ErrAlreadyExists = errors.New("sandbox already exists")

// ErrExportPreempted is returned when a durable sandbox resume wins before an
// instance export enters source finalization.
var ErrExportPreempted = errors.New("sandbox export preempted by resume")

// PullTokenHeader is the api_headers header carrying the opaque registry pull token.
const PullTokenHeader = "X-Kuasar-Pull-Token"

// MigrationTokenHeader is the api_headers header carrying an export-sandbox migration
// token on connect/resume: an absent sandbox is auto-imported from it, then resumed.
const MigrationTokenHeader = "X-Kuasar-Migration-Token"

// BuildAuth carries the per-build registry pull credentials a build trigger may
// supply: PullToken is the opaque, manifest-key-sealed token from the api_headers
// X-Kuasar-Pull-Token (preferred); RegistryUsername/Password are the SDK's cleartext
// from_image(username, password) (fromImageRegistry). Both empty => the tenant
// default (manifest_keys) or anonymous applies. Resolved in Core.TriggerBuild.
type BuildAuth struct {
	PullToken        string
	RegistryUsername string
	RegistryPassword string
}

// RegisterSpec is the immutable identity/config selected when a template build
// is registered. The e2b-compatible HTTP boundary supplies ProfileE2B when the
// request omits profile; downstream build records always carry it explicitly.
type RegisterSpec struct {
	Name       string
	Tags       []string
	Profile    types.Profile
	Resources  types.BuildResources
	Metadata   map[string]string
	EnvVars    map[string]string
	Secure     bool
	Builder    types.BuildOptions
	MMDSHeader *string
}

// TriggerSpec is the parsed build request (e2b TemplateBuildStartV2).
type TriggerSpec struct {
	FromImage    string
	FromTemplate string
	Steps        []types.TemplateStep
	StartCmd     string
	ReadyCmd     string
	// ResourceAssertion is the optional legacy E2B cpuCount/memoryMB assertion.
	// It is compared with the immutable registration resources and never merged.
	ResourceAssertion buildcfg.ResourcePatch
}

// BuildLogEntry is one build-progress line surfaced to the SDK (e2b BuildLogEntry:
// {timestamp, level, message}). Level is one of debug|info|warn|error.
type BuildLogEntry struct {
	Timestamp time.Time
	Level     string
	Message   string
}

// CreateReq is the decoded POST /sandboxes body (subset the SDK sends).
type CreateReq struct {
	TemplateID      string            `json:"templateID"`
	TimeoutSec      int               `json:"timeout"`
	Metadata        map[string]string `json:"metadata"`
	EnvVars         map[string]string `json:"envVars"`
	Secure          bool              `json:"secure"`
	AutoPauseMemory *bool             `json:"autoPauseMemory,omitempty"`
	APIKey          string            `json:"-"` // injected from X-API-KEY
	MMDSHeader      *string           `json:"-"` // nil = header absent; preserves top-level merge presence
}

// PauseRequest carries the E2B capture selector and action-scoped Snapshot-only
// policy. A nil Memory value has the E2B default true.
type PauseRequest struct {
	Memory               *bool `json:"memory,omitempty"`
	CheckpointMergeRef   *bool `json:"checkpoint_merge_ref,omitempty"`
	CheckpointDropCaches *bool `json:"checkpoint_drop_caches,omitempty"`
}

type ConnectOptions struct {
	TimeoutSec int
	Memory     *bool
}

const (
	maxCreateRequestBytes  = 16 << 20
	maxPauseRequestBytes   = 4 << 10
	maxConnectRequestBytes = 64 << 10
)

// Core is the orchestrator behaviour the API needs. Every per-resource method
// takes the raw API key; Core verifies it against the encrypted APISecret saved
// on the target resource and treats a mismatch as not-found. Create/RegisterBuild
// additionally require the APISecret/ManifestKey pair to be allowlisted.
type Core interface {
	Create(ctx context.Context, req CreateReq) (*types.Sandbox, error) // req.APIKey carries the key
	Get(ctx context.Context, id, apiKey string) (*types.Sandbox, error)
	List(ctx context.Context, apiKey, state string, limit int, cursor string) ([]*types.Sandbox, string, error)
	Kill(ctx context.Context, id, apiKey string) (bool, error)
	Connect(ctx context.Context, id, apiKey, migrationToken string, options ConnectOptions) (*types.Sandbox, error)
	ExecSession(ctx context.Context, id, apiKey, migrationToken string, ttlSeconds int64, conditions []string) (string, error)
	Pause(ctx context.Context, id, apiKey string, request sandboxcfg.CaptureRequest) error // ErrAlreadyPaused / ErrSandboxStarting / ErrNotFound
	SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error)
	ResourceStats(ctx context.Context, id, apiKey string) (*ResourceStats, error)
	TrafficStats(ctx context.Context, id, apiKey string) (*TrafficStats, error)
	UsageStats(ctx context.Context, id, apiKey string, query conductorextension.UsageQuery) (json.RawMessage, error)

	// Template builds (e2b v2/v3 build system, what the SDK uses): POST /v3/templates
	// (register name/cpu/memory) → POST /v2/templates/{tid}/builds/{bid} (start, carries
	// fromImage + fromImageRegistry + steps) → GET …/status (poll). The node pulls +
	// flattens the named image server-side — no client-side docker build/push.
	RegisterBuild(ctx context.Context, apiKey string, spec RegisterSpec) (*types.Build, error)
	TriggerBuild(ctx context.Context, apiKey, templateID, buildID string, spec TriggerSpec, auth BuildAuth) error
	BuildStatus(ctx context.Context, apiKey, templateID, buildID string) (*types.Build, error)
	CancelBuild(ctx context.Context, apiKey, templateID, buildID string) (BuildActionResult, error)
	DeleteBuild(ctx context.Context, apiKey, templateID string, options DeleteBuildOptions) (BuildActionResult, error)
	// BuildLogs returns the build's progress log entries from offset onward
	// (the SDK polls /status with ?logsOffset and streams them via on_build_logs).
	BuildLogs(ctx context.Context, apiKey, templateID, buildID string, offset int) ([]BuildLogEntry, error)
	ListTemplates(ctx context.Context, apiKey string) ([]*types.Build, error)
	// FilesUpload backs GET /templates/{tid}/files/{hash} (COPY build contexts):
	// resolves+authorizes the build, reports whether the object is already
	// present, and returns a presigned PUT URL for the client to upload to.
	// ErrFilesUnsupported when builder.files_storage is unconfigured.
	FilesUpload(ctx context.Context, apiKey, templateID, hash string) (present bool, url string, err error)

	// Sandbox export/import (orchestrator extension to the e2b surface). Export
	// publishes a paused Sandbox E or Snapshot S as a same-kind reusable template
	// (toTemplate) or an opaque kmt1 migration token; keepSource independently
	// controls source finalization.
	ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error)
	ImportSandbox(ctx context.Context, apiKey, token, targetID string) (string, error)
}

// ConnectMMDSCore is the standalone-only extension for migration-time initial
// MMDS secrets. Cluster router implementations intentionally do not implement
// it, so no CONNECT config is added to node-link or cluster command schemas.
type ConnectMMDSCore interface {
	ConnectWithMMDS(ctx context.Context, id, apiKey, migrationToken string, options ConnectOptions, metadata map[string]string, header *string) (*types.Sandbox, error)
}

// Resources are the node-uniform VM resources surfaced in e2b list/get responses.
// Every sandbox runs with the configured vcpu/memory (orch builds the launchspec
// from the same config); DiskMB is the writable overlay seed. e2b's ListedSandbox
// requires cpuCount/memoryMB/diskSizeMB, so the API must carry them.
type Resources struct {
	VCPU     int
	MemoryMB int
	DiskMB   int
}

// ResourceStats combines the native owner's effective specification and VMM
// observations with the node's observed reservation. Pointer fields distinguish
// an observed zero from an unavailable sample.
type ResourceStats = conductorextension.ResourceStats

type TrafficInflight = conductorextension.TrafficInflight
type ServiceTrafficStats = conductorextension.ServiceTrafficStats
type TrafficStats = conductorextension.TrafficStats
type TrafficCounters = conductorextension.TrafficCounters

type API struct {
	core         Core
	domain       string
	res          Resources
	log          *slog.Logger
	metricsProxy http.Handler
}

func New(core Core, domain string, res Resources, log *slog.Logger) *API {
	return &API{core: core, domain: domain, res: res, log: log}
}

// WithMetricsProxy installs the opaque telemetry query channel at construction.
func (a *API) WithMetricsProxy(handler http.Handler) *API {
	a.metricsProxy = handler
	return a
}

// Handler returns the routed http.Handler for api.<domain>.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("POST /sandboxes", a.auth(a.create))
	mux.HandleFunc("GET /sandboxes/{id}", a.auth(a.get))
	mux.HandleFunc("GET /sandboxes/{id}/stats/resource", a.auth(a.resourceStats))
	mux.HandleFunc("GET /sandboxes/{id}/metrics", a.auth(a.sandboxMetrics))
	mux.HandleFunc("GET /sandboxes/{id}/stats/traffic", a.auth(a.trafficStats))
	mux.HandleFunc("GET /sandboxes/{id}/stats/usage", a.auth(a.usageStats))
	mux.HandleFunc("GET /v2/sandboxes", a.auth(a.list))
	mux.HandleFunc("DELETE /sandboxes/{id}", a.auth(a.kill))
	mux.HandleFunc("POST /sandboxes/{id}/connect", a.auth(a.connect))
	mux.HandleFunc("POST /sandboxes/{id}/exec-sessions", a.authAPIKey(a.execSession))
	mux.HandleFunc("POST /sandboxes/{id}/pause", a.auth(a.pause))
	mux.HandleFunc("POST /sandboxes/{id}/timeout", a.auth(a.timeout))
	// Template build (e2b v3). The CLI authenticates with a Bearer access token;
	// treated identically to X-API-KEY (both derive the tenant via apikey).
	mux.HandleFunc("POST /v3/templates", a.auth(a.registerTemplate))
	mux.HandleFunc("POST /v2/templates/{tid}/builds/{bid}", a.auth(a.triggerBuild))
	mux.HandleFunc("GET /templates/{tid}/builds/{bid}/status", a.auth(a.buildStatus))
	mux.HandleFunc("POST /templates/{tid}/builds/{bid}/cancel", a.auth(a.cancelBuild))
	mux.HandleFunc("DELETE /templates/{tid}", a.auth(a.deleteBuild))
	mux.HandleFunc("GET /templates/{tid}/files/{hash}", a.auth(a.buildFiles))
	mux.HandleFunc("GET /templates", a.auth(a.listTemplates))
	// Sandbox export / import (orchestrator extension; api-key authed like the rest,
	// so it scopes to the caller's own sandboxes). Reached over both the TLS api
	// listener and the local control socket's api plane.
	mux.HandleFunc("POST /sandboxes/{id}/export", a.auth(a.exportSandbox))
	mux.HandleFunc("POST /sandboxes/import", a.auth(a.importSandbox))
	return mux
}

// apiKeyCtxKey carries the validated key to Core via the request context.
type apiKeyCtxKey struct{}

func (a *API) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		k := r.Header.Get("X-API-KEY")
		if k == "" {
			// Template/account endpoints use Bearer; we don't serve those.
			if b := r.Header.Get("Authorization"); strings.HasPrefix(b, "Bearer ") {
				k = strings.TrimPrefix(b, "Bearer ")
			}
		}
		// Format-only check here; Core verifies the MAC against the stored key.
		if _, err := apikey.Parse(k); err != nil {
			writeErr(w, 401, "unauthorized")
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), apiKeyCtxKey{}, k)))
	}
}

// authAPIKey is the capability-issuance authentication adapter. Unlike the e2b
// template/account compatibility surface, exec-session creation accepts only
// the explicit X-API-KEY carrier.
func (a *API) authAPIKey(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-KEY")
		if _, err := apikey.Parse(key); err != nil {
			writeErr(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), apiKeyCtxKey{}, key)))
	}
}

func apiKeyFrom(ctx context.Context) string {
	k, _ := ctx.Value(apiKeyCtxKey{}).(string)
	return k
}

// --- handlers ---

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	var req CreateReq
	if r.ContentLength > maxCreateRequestBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxCreateRequestBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeErr(w, http.StatusBadRequest, "bad body")
		return
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' || strictjson.DecodeAllowUnknown(trimmed, &req) != nil {
		writeErr(w, 400, "bad body")
		return
	}
	req.APIKey = apiKeyFrom(r.Context())
	req.MMDSHeader, err = singleOptionalHeader(r.Header, mmdsHeader)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Headers are an alternate config-injection surface; fold them into the e2b
	// metadata (header wins) so the orchestrator sees one uniform carrier.
	req.Metadata, err = mergeCreateConfigHeaders(req.Metadata, r.Header)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	sb, err := a.core.Create(r.Context(), req)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 201, a.sandboxResp(sb))
}

func (a *API) get(w http.ResponseWriter, r *http.Request) {
	sb, err := a.core.Get(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()))
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 200, a.sandboxDetail(sb))
}

func (a *API) resourceStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	stats, err := a.core.ResourceStats(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()))
	if err != nil {
		a.failStats(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (a *API) trafficStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	stats, err := a.core.TrafficStats(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()))
	if err != nil {
		a.failStats(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (a *API) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := atoiDefault(q.Get("limit"), 100)
	items, next, err := a.core.List(r.Context(), apiKeyFrom(r.Context()), q.Get("state"), limit, q.Get("nextToken"))
	if err != nil {
		a.fail(w, err)
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, sb := range items {
		out = append(out, a.listed(sb))
	}
	if next != "" {
		w.Header().Set("x-next-token", next)
	}
	writeJSON(w, 200, out)
}

func (a *API) kill(w http.ResponseWriter, r *http.Request) {
	ok, err := a.core.Kill(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()))
	if err != nil {
		a.fail(w, err)
		return
	}
	if !ok {
		writeErr(w, 404, "not found")
		return
	}
	w.WriteHeader(204)
}

func (a *API) connect(w http.ResponseWriter, r *http.Request) {
	migrationToken := r.Header.Get(MigrationTokenHeader)
	var body struct {
		Timeout  int               `json:"timeout"`
		Metadata map[string]string `json:"metadata"`
		Memory   *bool             `json:"memory,omitempty"`
	}
	if r.ContentLength > maxConnectRequestBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxConnectRequestBytes)
	if err := decodeOptionalStrictObject(r.Body, &body); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeErr(w, http.StatusBadRequest, "bad body")
		return
	}
	options := ConnectOptions{TimeoutSec: body.Timeout, Memory: body.Memory}
	// CONNECT must let an already-existing target ignore MMDS input completely.
	// Defer duplicate/value validation to the standalone import path; an empty
	// sentinel is malformed if import really happens and harmless if ignored.
	header := deferredOptionalHeader(r.Header, mmdsHeader)
	var sb *types.Sandbox
	var err error
	if standalone, ok := a.core.(ConnectMMDSCore); ok {
		sb, err = standalone.ConnectWithMMDS(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), migrationToken, options, body.Metadata, header)
	} else {
		// Cluster remains intentionally unaware of CONNECT MMDS config.
		if len(migrationToken) > migrationtoken.MaxWireSize {
			writeErr(w, http.StatusRequestHeaderFieldsTooLarge, migrationtoken.ErrTokenTooLarge.Error())
			return
		}
		sb, err = a.core.Connect(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), migrationToken, options)
	}
	if err != nil {
		if errors.Is(err, migrationtoken.ErrTokenTooLarge) {
			writeErr(w, http.StatusRequestHeaderFieldsTooLarge, migrationtoken.ErrTokenTooLarge.Error())
			return
		}
		if migrationToken != "" {
			a.failMigrate(w, err) // surface the concrete import error (runtime mismatch, wrong tenant, …)
		} else {
			a.fail(w, err)
		}
		return
	}
	writeJSON(w, 200, a.sandboxResp(sb))
}

func deferredOptionalHeader(header http.Header, name string) *string {
	values := header.Values(name)
	if len(values) == 0 {
		return nil
	}
	value := ""
	if len(values) == 1 {
		value = values[0]
	}
	return &value
}

func (a *API) execSession(w http.ResponseWriter, r *http.Request) {
	migrationToken := r.Header.Get(MigrationTokenHeader)
	if len(migrationToken) > migrationtoken.MaxWireSize {
		writeErr(w, http.StatusRequestHeaderFieldsTooLarge, migrationtoken.ErrTokenTooLarge.Error())
		return
	}
	request, err := execsession.DecodeRequest(r.Body, r.ContentLength)
	if err != nil {
		if errors.Is(err, execsession.ErrRequestTooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		writeErr(w, http.StatusBadRequest, execsession.ErrInvalidRequest.Error())
		return
	}
	token, err := a.core.ExecSession(
		r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), migrationToken,
		request.TTLSeconds, request.Expressions(),
	)
	if err != nil {
		a.failExecSession(w, err)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusCreated, map[string]string{"execAccessToken": token})
}

// failExecSession preserves the public control-plane status contract without
// forwarding node paths, node-local sandbox IDs, command state, or other
// internal error details.
func (a *API) failExecSession(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, conductorextension.ErrRejected):
		writeErr(w, http.StatusForbidden, conductorextension.ErrRejected.Error())
	case errors.Is(err, ErrExtensionUnavailable):
		writeErr(w, http.StatusServiceUnavailable, ErrExtensionUnavailable.Error())
	case errors.Is(err, ErrSandboxChanged):
		writeErr(w, http.StatusConflict, ErrSandboxChanged.Error())
	case errors.Is(err, migrationtoken.ErrAuthentication),
		errors.Is(err, migrationtoken.ErrCredentialMismatch),
		errors.Is(err, ErrNotAllowed):
		writeErr(w, http.StatusForbidden, "exec session credential not allowed")
	case errors.Is(err, migrationtoken.ErrMalformedToken),
		errors.Is(err, migrationtoken.ErrInvalidPayload),
		errors.Is(err, ErrBadRequest):
		writeErr(w, http.StatusBadRequest, "invalid exec session request")
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found")
	default:
		a.log.Warn("exec session error", "err", err)
		writeErr(w, http.StatusServiceUnavailable, "exec session unavailable")
	}
}

func (a *API) pause(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > maxPauseRequestBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxPauseRequestBytes)
	req, presence, err := decodePauseRequest(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeErr(w, http.StatusBadRequest, "bad body")
		return
	}
	kind := types.CaptureSnapshot
	if req.Memory != nil && !*req.Memory {
		kind = types.CaptureSandbox
	}
	override := sandboxcfg.SnapshotPolicy{
		MergeRef:   req.CheckpointMergeRef,
		DropCaches: req.CheckpointDropCaches,
	}
	snapshotOptionsPresent := presence.checkpointMergeRef || presence.checkpointDropCaches
	if _, present := r.Header[http.CanonicalHeaderKey(checkpointHeader)]; present {
		rawHeader := r.Header.Get(checkpointHeader)
		headerPolicy, parseErr := sandboxcfg.ParseSnapshotPolicyJSON(rawHeader)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, parseErr.Error())
			return
		}
		snapshotOptionsPresent = snapshotOptionsPresent || snapshotPolicyFieldsPresent(rawHeader)
		override = sandboxcfg.OverlaySnapshotPolicy(override, headerPolicy)
	} else {
		override = sandboxcfg.CloneSnapshotPolicy(override)
	}
	if kind == types.CaptureSandbox && snapshotOptionsPresent {
		writeErr(w, http.StatusBadRequest, "snapshot-only checkpoint options require memory=true")
		return
	}
	err = a.core.Pause(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), sandboxcfg.CaptureRequest{
		Kind: kind, SnapshotPolicy: override,
	})
	switch {
	case errors.Is(err, ErrAlreadyPaused):
		w.WriteHeader(409)
	case errors.Is(err, ErrSandboxStarting):
		writeErr(w, http.StatusConflict, ErrSandboxStarting.Error())
	case err != nil:
		a.fail(w, err)
	default:
		w.WriteHeader(204)
	}
}

type pauseRequestPresence struct {
	checkpointMergeRef   bool
	checkpointDropCaches bool
}

func decodePauseRequest(body io.Reader) (PauseRequest, pauseRequestPresence, error) {
	raw, err := io.ReadAll(body)
	if err != nil {
		return PauseRequest{}, pauseRequestPresence{}, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return PauseRequest{}, pauseRequestPresence{}, nil
	}
	if trimmed[0] != '{' {
		return PauseRequest{}, pauseRequestPresence{}, ErrBadRequest
	}

	var req PauseRequest
	if err := strictjson.DecodeAllowNull(trimmed, &req); err != nil {
		return PauseRequest{}, pauseRequestPresence{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &fields); err != nil {
		return PauseRequest{}, pauseRequestPresence{}, err
	}
	_, mergeRef := fields["checkpoint_merge_ref"]
	_, dropCaches := fields["checkpoint_drop_caches"]
	return req, pauseRequestPresence{
		checkpointMergeRef:   mergeRef,
		checkpointDropCaches: dropCaches,
	}, nil
}

func snapshotPolicyFieldsPresent(raw string) bool {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		return false // ParseSnapshotPolicyJSON validates the same input first.
	}
	_, mergeRef := fields["merge_ref"]
	_, dropCaches := fields["drop_caches"]
	return mergeRef || dropCaches
}

func decodeOptionalStrictObject(body io.Reader, out any) error {
	raw, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	if bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	if trimmed[0] != '{' {
		return ErrBadRequest
	}
	return strictjson.DecodeAllowNull(trimmed, out)
}

func (a *API) timeout(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Timeout int `json:"timeout"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad body")
		return
	}
	ok, err := a.core.SetTimeout(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), body.Timeout)
	if err != nil {
		a.fail(w, err)
		return
	}
	if !ok {
		writeErr(w, 404, "not found")
		return
	}
	w.WriteHeader(204)
}

// --- template build handlers (e2b v3) ---

func (a *API) registerTemplate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name            string            `json:"name"`
		Tags            []string          `json:"tags"`
		Profile         string            `json:"profile"`
		CPUCount        json.RawMessage   `json:"cpuCount"`
		CPUCountSn      json.RawMessage   `json:"cpu_count"`
		MemoryMB        json.RawMessage   `json:"memoryMB"`
		MemoryMBSn      json.RawMessage   `json:"memory_mb"`
		Metadata        map[string]string `json:"metadata"`
		EnvVars         map[string]string `json:"envVars"`
		Secure          bool              `json:"secure"`
		Cancel          json.RawMessage   `json:"cancel"`
		AutoPauseMemory json.RawMessage   `json:"autoPauseMemory"`
	}
	if err := decodeBuildRequest(r.Body, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid Build query")
		return
	}
	if len(body.Cancel) != 0 || query.Has("cancel") {
		writeErr(w, http.StatusBadRequest, "cancel is a Build action, not a Build definition")
		return
	}
	if len(body.AutoPauseMemory) != 0 {
		writeErr(w, http.StatusBadRequest, "autoPauseMemory is not valid for template builds; select builder.target.memory")
		return
	}
	profile, err := requestedBuildProfile(body.Profile)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Template/phase sandbox configuration remains independent from the E2B
	// first-class Build resource fields.
	meta, err := mergeBuildConfigHeaders(body.Metadata, r.Header)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	meta, builderOpts, err := buildcfg.Extract(meta)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	bodyResources, err := buildcfg.ParseFirstClassResourceJSON(body.CPUCount, body.CPUCountSn, body.MemoryMB, body.MemoryMBSn)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	headerResources := buildcfg.ResourcePatch{}
	if builderOpts.Resources != nil {
		headerResources = buildcfg.PatchFromResources(*builderOpts.Resources)
	}
	mergedResources, err := buildcfg.MergeResourcePatches(bodyResources, headerResources)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	resources, err := buildcfg.ResolveResources(mergedResources)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	builderOpts.Resources = nil
	mmdsValue, err := singleOptionalHeader(r.Header, mmdsHeader)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	b, err := a.core.RegisterBuild(r.Context(), apiKeyFrom(r.Context()), RegisterSpec{
		Name: body.Name, Tags: body.Tags, Profile: profile, Resources: resources,
		Metadata: meta, EnvVars: body.EnvVars, Secure: body.Secure,
		Builder: builderOpts, MMDSHeader: mmdsValue,
	})
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{
		"templateID": b.TemplateID,
		"buildID":    b.BuildID,
		"public":     false,
		"names":      b.Names,
		"tags":       b.Aliases,
		"aliases":    b.Aliases,
		"profile":    b.Profile,
		"target":     b.Builder.Target,
	})
}

func requestedBuildProfile(raw string) (types.Profile, error) {
	if raw == "" {
		return types.ProfileE2B, nil
	}
	return types.ParseProfile(raw)
}

func (a *API) triggerBuild(w http.ResponseWriter, r *http.Request) {
	// Accept both the simple shape ({fromImage,startCmd}) and the real e2b CLI
	// body ({dockerfile,template_name,start_cmd,ready_cmd,cpu_count,memory_mb,
	// team_id}). The CLI client-side `docker build`s the dockerfile and pushes the
	// result to <mask>/{templateID}:{buildID}; it sends no image ref, so when
	// fromImage is empty the orchestrator derives it from builder_image_uri_mask.
	var body struct {
		FromImage         string `json:"fromImage"`
		FromTemplate      string `json:"fromTemplate"`
		FromImageRegistry struct {
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"fromImageRegistry"`
		Steps        []types.TemplateStep `json:"steps"`
		Force        bool                 `json:"force"`
		StartCmd     string               `json:"startCmd"`
		StartCmdE2B  string               `json:"start_cmd"`
		ReadyCmd     string               `json:"readyCmd"`
		ReadyCmdE2B  string               `json:"ready_cmd"`
		Dockerfile   string               `json:"dockerfile"`
		TemplateName string               `json:"template_name"`
		CPUCount     json.RawMessage      `json:"cpuCount"`
		CPUCountSn   json.RawMessage      `json:"cpu_count"`
		MemoryMB     json.RawMessage      `json:"memoryMB"`
		MemoryMBSn   json.RawMessage      `json:"memory_mb"`
		Metadata     map[string]string    `json:"metadata"`
		Cancel       json.RawMessage      `json:"cancel"`
	}
	if err := decodeBuildRequest(r.Body, &body); err != nil {
		writeErr(w, http.StatusBadRequest, "bad body: "+err.Error())
		return
	}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid Build query")
		return
	}
	if len(body.Cancel) != 0 || query.Has("cancel") {
		writeErr(w, http.StatusBadRequest, "cancel is a Build action, not a Build definition")
		return
	}
	if _, present := r.Header[http.CanonicalHeaderKey(builderHeader)]; present {
		writeErr(w, http.StatusBadRequest, "trigger-time builder configuration is deprecated; configure it at registration")
		return
	}
	if len(body.Metadata) != 0 || hasTriggerSandboxConfigHeader(r.Header) {
		writeErr(w, http.StatusBadRequest, "trigger-time sandbox metadata is deprecated; configure template metadata at registration")
		return
	}
	startCmd := body.StartCmd
	if startCmd == "" {
		startCmd = body.StartCmdE2B
	}
	readyCmd := body.ReadyCmd
	if readyCmd == "" {
		readyCmd = body.ReadyCmdE2B
	}
	// COPY is gated in Core.TriggerBuild: it needs builder.files_storage AND
	// each context object already uploaded (via the files endpoint) — a missing
	// store maps to 501, a missing object to 400, both fail fast there.
	auth := BuildAuth{
		PullToken:        r.Header.Get(PullTokenHeader),
		RegistryUsername: body.FromImageRegistry.Username,
		RegistryPassword: body.FromImageRegistry.Password,
	}
	resourceAssertion, err := buildcfg.ParseFirstClassResourceJSON(body.CPUCount, body.CPUCountSn, body.MemoryMB, body.MemoryMBSn)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	err = a.core.TriggerBuild(r.Context(), apiKeyFrom(r.Context()),
		r.PathValue("tid"), r.PathValue("bid"), TriggerSpec{
			FromImage:         body.FromImage,
			FromTemplate:      body.FromTemplate,
			Steps:             body.Steps,
			StartCmd:          startCmd,
			ReadyCmd:          readyCmd,
			ResourceAssertion: resourceAssertion,
		}, auth)
	if err != nil {
		var stateConflict *BuildStateConflictError
		switch {
		case errors.As(err, &stateConflict):
			writeJSON(w, http.StatusConflict, map[string]string{
				"message": stateConflict.Error(),
				"state":   string(stateConflict.State),
			})
		case errors.Is(err, ErrFilesUnsupported):
			writeErr(w, 501, err.Error())
		case errors.Is(err, ErrBadRequest):
			writeErr(w, 400, err.Error())
		default:
			a.fail(w, err)
		}
		return
	}
	w.WriteHeader(202)
}

func hasTriggerSandboxConfigHeader(header http.Header) bool {
	for name := range header {
		if strings.HasPrefix(strings.ToLower(name), "x-kuasar-sandbox-") &&
			!strings.EqualFold(name, clusterGroupHeader) {
			return true
		}
	}
	return false
}

// buildFiles is the e2b v2 build-files endpoint (COPY contexts by hash): the
// client GETs it to learn whether the context is already uploaded and, if not,
// where to PUT it. Returns 201 {present, url}; the url is a presigned PUT
// straight to the object store (bytes never transit here). 404 on unknown/
// unowned template, 501 when files_storage is unconfigured.
func (a *API) buildFiles(w http.ResponseWriter, r *http.Request) {
	present, url, err := a.core.FilesUpload(r.Context(),
		apiKeyFrom(r.Context()), r.PathValue("tid"), r.PathValue("hash"))
	if err != nil {
		if errors.Is(err, ErrFilesUnsupported) {
			writeErr(w, 501, err.Error())
			return
		}
		a.fail(w, err) // ErrNotFound → 404, ErrNotAllowed → 403, else 500
		return
	}
	writeJSON(w, 201, map[string]any{"present": present, "url": url})
}

func (a *API) buildStatus(w http.ResponseWriter, r *http.Request) {
	b, err := a.core.BuildStatus(r.Context(), apiKeyFrom(r.Context()), r.PathValue("tid"), r.PathValue("bid"))
	if err != nil {
		a.fail(w, err)
		return
	}
	// The Python SDK's BuildInfo.template_id is taken from this response's templateID,
	// and that is what the caller then creates from — so once ready report the durable,
	// self-describing persist id (the transient handle isn't creatable). logEntries +
	// logs are required by the SDK's TemplateBuildInfo; reason is a BuildStatusReason
	// object (omitted when empty).
	tid := b.TemplateID
	if b.PersistID != "" {
		tid = b.PersistID
	}
	// Build progress: the SDK polls with ?logsOffset (count already seen) and
	// streams the new entries via on_build_logs. logEntries is structured;
	// logs mirrors the messages (the SDK requires both fields).
	offset, _ := strconv.Atoi(r.URL.Query().Get("logsOffset"))
	entries, _ := a.core.BuildLogs(r.Context(), apiKeyFrom(r.Context()),
		r.PathValue("tid"), r.PathValue("bid"), offset)
	logEntries := make([]any, 0, len(entries))
	logs := make([]string, 0, len(entries))
	for _, e := range entries {
		logEntries = append(logEntries, map[string]any{
			"timestamp": e.Timestamp.UTC().Format(time.RFC3339Nano),
			"level":     e.Level,
			"message":   e.Message,
		})
		logs = append(logs, e.Message)
	}
	resp := map[string]any{
		"templateID": tid,
		"buildID":    b.BuildID,
		"profile":    b.Profile,
		"target":     b.Builder.Target,
		"status":     b.Status.SDKStatus(),
		"logs":       logs,
		"logEntries": logEntries,
		"resources": map[string]any{
			"cpuMilli":     b.Resources.CPU,
			"memoryBytes":  b.Resources.Memory,
			"storageBytes": b.Resources.Storage,
		},
		"executionClaimed":   b.ExecutionClaimed,
		"cancelRequested":    b.CancelRequestedUnix != 0,
		"deleteRequested":    b.DeleteRequestedUnix != 0,
		"runID":              b.RunID,
		"storageEnforcement": "admission-only",
	}
	if b.Kind != "" {
		resp["kind"] = b.Kind
	}
	if b.Phase != "" {
		resp["phase"] = map[string]any{"name": b.Phase, "sandboxID": b.PhaseSandboxID}
	}
	if b.Reason != "" {
		resp["reason"] = map[string]any{"message": b.Reason}
	}
	if b.PersistID != "" { // also surface names/aliases (the CLI reads the persist id here)
		resp["names"] = b.Names
		resp["aliases"] = b.Aliases
	}
	writeJSON(w, 200, resp)
}

func (a *API) listTemplates(w http.ResponseWriter, r *http.Request) {
	items, err := a.core.ListTemplates(r.Context(), apiKeyFrom(r.Context()))
	if err != nil {
		a.fail(w, err)
		return
	}
	out := make([]map[string]any, 0, len(items))
	for _, b := range items {
		out = append(out, map[string]any{
			"templateID":  b.PersistID, // list shows the persist id as the canonical template id
			"buildID":     b.BuildID,
			"profile":     b.Profile,
			"target":      b.Builder.Target,
			"kind":        b.Kind,
			"names":       b.Names,
			"aliases":     b.Aliases,
			"public":      false,
			"buildStatus": b.Status.SDKStatus(),
			"createdAt":   b.CreatedUnix,
		})
	}
	writeJSON(w, 200, out)
}

// --- sandbox export / import (orchestrator extension) ---

const (
	// Token and target ID retain their independent field limits. Allow a small,
	// bounded JSON envelope budget so ordinary serializers may add whitespace
	// without making a maximum-size valid pair depend on compact encoding.
	maxImportJSONEnvelopeBytes = 256
	maxImportRequestBytes      = migrationtoken.MaxWireSize + types.MaxLocalSandboxIDBytes + len(`{"token":"","sandboxID":""}`) + maxImportJSONEnvelopeBytes
)

func (a *API) exportSandbox(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ToTemplate bool `json:"toTemplate"`
		KeepSource bool `json:"keepSource"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	out, err := a.core.ExportSandbox(r.Context(), apiKeyFrom(r.Context()), r.PathValue("id"), body.ToTemplate, body.KeepSource)
	if err != nil {
		a.failMigrate(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"result": out})
}

func (a *API) importSandbox(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(maxImportRequestBytes))
	var body struct {
		Token     string `json:"token"`
		SandboxID string `json:"sandboxID"`
	}
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&body)
	if err == nil {
		var extra any
		if trailingErr := decoder.Decode(&extra); trailingErr != io.EOF {
			if trailingErr == nil {
				err = ErrBadRequest
			} else {
				err = trailingErr
			}
		}
	}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, migrationtoken.ErrTokenTooLarge.Error())
			return
		}
		writeErr(w, 400, "token required")
		return
	}
	if body.Token == "" {
		writeErr(w, 400, "token required")
		return
	}
	if len(body.Token) > migrationtoken.MaxWireSize {
		writeErr(w, http.StatusRequestEntityTooLarge, migrationtoken.ErrTokenTooLarge.Error())
		return
	}
	id, err := a.core.ImportSandbox(r.Context(), apiKeyFrom(r.Context()), body.Token, body.SandboxID)
	if err != nil {
		if errors.Is(err, migrationtoken.ErrTokenTooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, err.Error())
			return
		}
		a.failMigrate(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"sandboxID": id})
}

// failMigrate maps typed migration failures without returning storage paths,
// credential material, token fragments, or other internal diagnostics.
func (a *API) failMigrate(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, conductorextension.ErrRejected):
		writeErr(w, http.StatusForbidden, conductorextension.ErrRejected.Error())
	case errors.Is(err, ErrExtensionUnavailable):
		writeErr(w, http.StatusServiceUnavailable, ErrExtensionUnavailable.Error())
	case errors.Is(err, ErrExportPreempted):
		writeErr(w, http.StatusConflict, ErrExportPreempted.Error())
	case errors.Is(err, ErrAlreadyExists):
		writeErr(w, http.StatusConflict, "target sandbox already exists")
	case errors.Is(err, types.ErrMemoryUnavailable):
		writeErr(w, http.StatusConflict, types.ErrMemoryUnavailable.Error())
	case errors.Is(err, types.ErrLaunchModeConflict):
		writeErr(w, http.StatusConflict, types.ErrLaunchModeConflict.Error())
	case errors.Is(err, migrationtoken.ErrIncompatible):
		writeErr(w, http.StatusConflict, "target environment incompatible")
	case errors.Is(err, migrationtoken.ErrAuthentication),
		errors.Is(err, migrationtoken.ErrCredentialMismatch),
		errors.Is(err, ErrNotAllowed):
		writeErr(w, http.StatusForbidden, "migration credential not allowed")
	case errors.Is(err, migrationtoken.ErrMalformedToken),
		errors.Is(err, migrationtoken.ErrInvalidPayload):
		writeErr(w, http.StatusBadRequest, "invalid migration token")
	case errors.Is(err, migrationtoken.ErrTokenTooLarge):
		writeErr(w, http.StatusRequestEntityTooLarge, migrationtoken.ErrTokenTooLarge.Error())
	case errors.Is(err, ErrBadRequest):
		writeErr(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrNotFound):
		writeErr(w, http.StatusNotFound, "not found")
	default:
		a.log.Warn("migrate error", "err", err)
		writeErr(w, http.StatusInternalServerError, "internal error")
	}
}

// --- response shaping ---

func (a *API) envdVersion(sb *types.Sandbox) string {
	if sb.Profile == types.ProfileE2B {
		return "0.6.1"
	}
	return "0.1.0" // bare stub: >=0.1.0 so the SDK does not self-destruct
}

func (a *API) sandboxResp(sb *types.Sandbox) map[string]any {
	response := a.sandboxBaseResp(sb)
	response["forwardAccessToken"] = sb.ForwardAccessToken
	if sb.Profile == types.ProfileE2B {
		response["envdAccessToken"] = sb.EnvdAccessToken
		response["trafficAccessToken"] = sb.TrafficAccessToken
	}
	return response
}

func (a *API) sandboxBaseResp(sb *types.Sandbox) map[string]any {
	return map[string]any{
		"sandboxID":   sb.ID,
		"templateID":  sb.TemplateID,
		"clientID":    "orchestrator",
		"domain":      a.domain,
		"envdVersion": a.envdVersion(sb),
		"alias":       "",
	}
}

func (a *API) sandboxDetail(sb *types.Sandbox) map[string]any {
	d := a.sandboxBaseResp(sb)
	d["state"] = string(sb.State)
	d["cpuCount"] = a.res.VCPU
	d["memoryMB"] = a.res.MemoryMB
	d["diskSizeMB"] = a.res.DiskMB
	d["startedAt"] = isoUnix(sb.CreatedUnix)
	d["endAt"] = isoUnix(sandboxEndUnix(sb))
	d["metadata"] = sb.Metadata
	return d
}

// listed renders one item of GET /v2/sandboxes. The e2b SDK's ListedSandbox model
// requires clientID/cpuCount/diskSizeMB/memoryMB/sandboxID/templateID/envdVersion/
// state plus startedAt/endAt as ISO-8601 (it isoparse()s them) — a Unix int or a
// missing field crashes next_items(). With no state filter the store returns only
// running/paused, matching the SDK's SandboxState enum; explicit internal-state
// filters remain a diagnostic surface.
func (a *API) listed(sb *types.Sandbox) map[string]any {
	return map[string]any{
		"sandboxID":   sb.ID,
		"templateID":  sb.TemplateID,
		"clientID":    "orchestrator",
		"state":       string(sb.State),
		"cpuCount":    a.res.VCPU,
		"memoryMB":    a.res.MemoryMB,
		"diskSizeMB":  a.res.DiskMB,
		"envdVersion": a.envdVersion(sb),
		"startedAt":   isoUnix(sb.CreatedUnix),
		"endAt":       isoUnix(sandboxEndUnix(sb)),
		"metadata":    sb.Metadata,
	}
}

func sandboxEndUnix(sb *types.Sandbox) int64 {
	if sb.DeadlineUnix == 0 { // no deadline: report start so endAt is still a valid timestamp
		return sb.CreatedUnix
	}
	return sb.DeadlineUnix
}

// isoUnix formats a Unix-seconds timestamp as RFC3339 (ISO-8601) for the SDK.
func isoUnix(sec int64) string { return time.Unix(sec, 0).UTC().Format(time.RFC3339) }

func (a *API) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrAlreadyExists):
		writeErr(w, http.StatusConflict, ErrAlreadyExists.Error())
	case errors.Is(err, conductorextension.ErrRejected):
		writeErr(w, http.StatusForbidden, conductorextension.ErrRejected.Error())
	case errors.Is(err, ErrExtensionUnavailable):
		writeErr(w, http.StatusServiceUnavailable, ErrExtensionUnavailable.Error())
	case errors.Is(err, ErrSandboxChanged):
		writeErr(w, http.StatusConflict, ErrSandboxChanged.Error())
	case errors.Is(err, ErrBuildChanged):
		writeErr(w, http.StatusConflict, ErrBuildChanged.Error())
	case errors.Is(err, types.ErrMemoryUnavailable):
		writeErr(w, http.StatusConflict, types.ErrMemoryUnavailable.Error())
	case errors.Is(err, types.ErrLaunchModeConflict):
		writeErr(w, http.StatusConflict, types.ErrLaunchModeConflict.Error())
	case errors.Is(err, ErrNotFound):
		writeErr(w, 404, "not found")
	case errors.Is(err, ErrNotAllowed):
		writeErr(w, 403, "credential pair not allowed")
	case errors.Is(err, ErrBadRequest):
		writeErr(w, 400, err.Error())
	case errors.Is(err, ErrBuildAdmission):
		writeErr(w, http.StatusTooManyRequests, ErrBuildAdmission.Error())
	case errors.Is(err, ErrProxyUnavailable):
		writeErr(w, http.StatusServiceUnavailable, ErrProxyUnavailable.Error())
	default:
		a.log.Warn("api error", "err", err)
		writeErr(w, 500, "internal error")
	}
}

func (a *API) failStats(w http.ResponseWriter, err error) {
	status, message := StatsErrorStatus(err)
	if status == http.StatusInternalServerError {
		a.log.Warn("sandbox stats error", "err", err)
	}
	writeErr(w, status, message)
}

// StatsErrorStatus is shared by the public API and the trusted local adapter.
func StatsErrorStatus(err error) (int, string) {
	switch {
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest, "invalid stats query"
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound, "not found"
	case errors.Is(err, ErrStatsUnsupported):
		return http.StatusNotImplemented, "stats unsupported"
	case errors.Is(err, ErrStatsConflict):
		return http.StatusConflict, "stats unavailable for sandbox state"
	case errors.Is(err, ErrStatsUnavailable):
		return http.StatusServiceUnavailable, "stats temporarily unavailable"
	default:
		return http.StatusInternalServerError, "internal error"
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"message": msg})
}

func atoiDefault(s string, def int) int {
	if s == "" {
		return def
	}
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}

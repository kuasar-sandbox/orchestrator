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
	"strconv"
	"strings"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/execsession"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// configHeaderNs maps each X-Kuasar-Sandbox-<Ns> request header to the namespaced
// metadata key it normalizes into. Headers are an alternate config-injection surface
// (create + template build); on conflict with an e2b metadata key of the same
// namespace the header wins. The header value is the same JSON the metadata key holds.
var configHeaderNs = []struct{ header, metaKey string }{
	{"X-Kuasar-Sandbox-Resource", sandboxcfg.NsResource},
	{"X-Kuasar-Sandbox-Network", sandboxcfg.NsNetwork},
	{"X-Kuasar-Sandbox-Launch", sandboxcfg.NsLaunch},
	{"X-Kuasar-Sandbox-Init", sandboxcfg.NsInit},
	{"X-Kuasar-Sandbox-Mounts", sandboxcfg.NsMounts},
	{"X-Kuasar-Sandbox-Files", sandboxcfg.NsFiles},
	{"X-Kuasar-Sandbox-Metadata", sandboxcfg.NsMetadata},
}

const (
	builderHeader     = "X-Kuasar-Sandbox-Builder"
	restoreHeader     = "X-Kuasar-Sandbox-Restore"
	credentialsHeader = "X-Kuasar-Sandbox-Credentials"
	checkpointHeader  = "X-Kuasar-Sandbox-Checkpoint"
)

// pickInt returns a if non-zero, else b (camelCase vs snake_case e2b field aliases).
func pickInt(a, b int) int {
	if a != 0 {
		return a
	}
	return b
}

// mergeConfigHeaders folds the X-Kuasar-Sandbox-<Ns> headers into meta, the header
// overriding an e2b metadata key of the same namespace. Returns the merged map
// (allocating one only if a header is present and meta was nil).
func mergeConfigHeaders(meta map[string]string, h http.Header) map[string]string {
	for _, m := range configHeaderNs {
		v := h.Get(m.header)
		if v == "" {
			continue
		}
		if meta == nil {
			meta = map[string]string{}
		}
		meta[m.metaKey] = v
	}
	return meta
}

func mergeCreateConfigHeaders(meta map[string]string, h http.Header) (map[string]string, error) {
	meta = mergeConfigHeaders(meta, h)
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
	bodyPolicy := sandboxcfg.CheckpointPolicy{}
	bodyPresent := false
	if raw, ok := meta[sandboxcfg.NsCheckpoint]; ok {
		bodyPresent = true
		var err error
		bodyPolicy, err = sandboxcfg.ParseCheckpointPolicyJSON(raw)
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
		headerPolicy, err := sandboxcfg.ParseCheckpointPolicyJSON(h.Get(checkpointHeader))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", checkpointHeader, err)
		}
		policy = sandboxcfg.OverlayCheckpointPolicy(bodyPolicy, headerPolicy)
	}
	if policy.Empty() {
		delete(meta, sandboxcfg.NsCheckpoint)
		return meta, nil
	}
	canonical, err := sandboxcfg.MarshalCheckpointPolicyJSON(policy)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		meta = map[string]string{}
	}
	meta[sandboxcfg.NsCheckpoint] = canonical
	return meta, nil
}

func mergeBuildConfigHeaders(meta map[string]string, h http.Header) map[string]string {
	meta = mergeConfigHeaders(meta, h)
	if v := h.Get(builderHeader); v != "" {
		if meta == nil {
			meta = map[string]string{}
		}
		meta[buildcfg.NsBuilder] = v
	}
	return meta
}

// ErrAlreadyPaused is returned by Core.Pause when the sandbox is already paused.
var ErrAlreadyPaused = errors.New("already paused")

// ErrNotFound is returned by Core methods when the sandbox id is unknown.
var ErrNotFound = errors.New("sandbox not found")

// ErrNotAllowed is returned by Create/RegisterBuild when the API key does not
// resolve to an APISecret/ManifestKey pair in the node allowlist (=> 403).
var ErrNotAllowed = errors.New("credential pair not allowed to create")

// ErrFilesUnsupported is returned by FilesUpload / TriggerBuild when a COPY
// step is used but builder.files_storage is unconfigured (=> 501).
var ErrFilesUnsupported = errors.New("COPY build contexts unsupported (builder.files_storage not configured)")

// ErrBadRequest maps a Core-side validation failure (e.g. a COPY referencing
// an unuploaded context) to 400.
var ErrBadRequest = errors.New("bad request")

// ErrAlreadyExists is returned when a sandbox migration import target exists.
var ErrAlreadyExists = errors.New("sandbox already exists")

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
	Name     string
	Tags     []string
	Profile  types.Profile
	Metadata map[string]string
}

// TriggerSpec is the parsed build request (e2b TemplateBuildStartV2).
type TriggerSpec struct {
	FromImage    string
	FromTemplate string
	Steps        []types.TemplateStep
	StartCmd     string
	ReadyCmd     string
	// Metadata is the trigger-time template config (kuasar-sandbox.<ns> keys from
	// cpu/memory + X-Kuasar-Sandbox-* headers); it overrides the register-time config.
	Metadata map[string]string
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
	TemplateID string            `json:"templateID"`
	TimeoutSec int               `json:"timeout"`
	Metadata   map[string]string `json:"metadata"`
	EnvVars    map[string]string `json:"envVars"`
	Secure     bool              `json:"secure"`
	APIKey     string            `json:"-"` // injected from X-API-KEY
}

// PauseRequest carries action-scoped local checkpoint policy. Nil fields inherit
// lower-priority sandbox/node policy. memory=false is unsupported because Pause
// always captures memory.
type PauseRequest struct {
	Memory               *bool `json:"memory,omitempty"`
	CheckpointMergeRef   *bool `json:"checkpoint_merge_ref,omitempty"`
	CheckpointDropCaches *bool `json:"checkpoint_drop_caches,omitempty"`
}

const maxPauseRequestBytes = 4 << 10

// Core is the orchestrator behaviour the API needs. Every per-resource method
// takes the raw API key; Core verifies it against the encrypted APISecret saved
// on the target resource and treats a mismatch as not-found. Create/RegisterBuild
// additionally require the APISecret/ManifestKey pair to be allowlisted.
type Core interface {
	Create(ctx context.Context, req CreateReq) (*types.Sandbox, error) // req.APIKey carries the key
	Get(ctx context.Context, id, apiKey string) (*types.Sandbox, error)
	List(ctx context.Context, apiKey, state string, limit int, cursor string) ([]*types.Sandbox, string, error)
	Kill(ctx context.Context, id, apiKey string) (bool, error)
	Connect(ctx context.Context, id, apiKey, migrationToken string, timeoutSec int) (*types.Sandbox, error)
	ExecSession(ctx context.Context, id, apiKey, migrationToken string, ttlSeconds int64) (string, error)
	Pause(ctx context.Context, id, apiKey string, override sandboxcfg.CheckpointPolicy) error // ErrAlreadyPaused / ErrNotFound
	SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error)

	// Template builds (e2b v2/v3 build system, what the SDK uses): POST /v3/templates
	// (register name/cpu/memory) → POST /v2/templates/{tid}/builds/{bid} (start, carries
	// fromImage + fromImageRegistry + steps) → GET …/status (poll). The node pulls +
	// flattens the named image server-side — no client-side docker build/push.
	RegisterBuild(ctx context.Context, apiKey string, spec RegisterSpec) (*types.Build, error)
	TriggerBuild(ctx context.Context, apiKey, templateID, buildID string, spec TriggerSpec, auth BuildAuth) error
	BuildStatus(ctx context.Context, apiKey, templateID, buildID string) (*types.Build, error)
	// BuildLogs returns the build's progress log entries from offset onward
	// (the SDK polls /status with ?logsOffset and streams them via on_build_logs).
	BuildLogs(ctx context.Context, apiKey, templateID, buildID string, offset int) ([]BuildLogEntry, error)
	ListTemplates(ctx context.Context, apiKey string) ([]*types.Build, error)
	// FilesUpload backs GET /templates/{tid}/files/{hash} (COPY build contexts):
	// resolves+authorizes the build, reports whether the object is already
	// present, and returns a presigned PUT URL for the client to upload to.
	// ErrFilesUnsupported when builder.files_storage is unconfigured.
	FilesUpload(ctx context.Context, apiKey, templateID, hash string) (present bool, url string, err error)

	// Sandbox export/import (orchestrator extension to the e2b surface). Export turns
	// a paused sandbox's remote snapshot into a reusable template (toTemplate) or a
	// one-line opaque kmt1 migration token; import restores a paused logical sandbox.
	ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error)
	ImportSandbox(ctx context.Context, apiKey, token, targetID string) (string, error)
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

type API struct {
	core   Core
	domain string
	res    Resources
	log    *slog.Logger
}

func New(core Core, domain string, res Resources, log *slog.Logger) *API {
	return &API{core: core, domain: domain, res: res, log: log}
}

// Handler returns the routed http.Handler for api.<domain>.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("POST /sandboxes", a.auth(a.create))
	mux.HandleFunc("GET /sandboxes/{id}", a.auth(a.get))
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, 400, "bad body")
		return
	}
	req.APIKey = apiKeyFrom(r.Context())
	// Headers are an alternate config-injection surface; fold them into the e2b
	// metadata (header wins) so the orchestrator sees one uniform carrier.
	var err error
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
	if len(migrationToken) > migrationtoken.MaxWireSize {
		writeErr(w, http.StatusRequestHeaderFieldsTooLarge, migrationtoken.ErrTokenTooLarge.Error())
		return
	}
	var body struct {
		Timeout int `json:"timeout"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	sb, err := a.core.Connect(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), migrationToken, body.Timeout)
	if err != nil {
		if migrationToken != "" {
			a.failMigrate(w, err) // surface the concrete import error (runtime mismatch, wrong tenant, …)
		} else {
			a.fail(w, err)
		}
		return
	}
	writeJSON(w, 200, a.sandboxResp(sb))
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
		r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), migrationToken, request.TTLSeconds,
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
	req, err := decodePauseRequest(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeErr(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeErr(w, http.StatusBadRequest, "bad body")
		return
	}
	if req.Memory != nil && !*req.Memory {
		writeErr(w, http.StatusBadRequest, "memory=false is not supported")
		return
	}
	override := sandboxcfg.CheckpointPolicy{
		MergeRef:   req.CheckpointMergeRef,
		DropCaches: req.CheckpointDropCaches,
	}
	if _, present := r.Header[http.CanonicalHeaderKey(checkpointHeader)]; present {
		headerPolicy, parseErr := sandboxcfg.ParseCheckpointPolicyJSON(r.Header.Get(checkpointHeader))
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, parseErr.Error())
			return
		}
		override = sandboxcfg.OverlayCheckpointPolicy(override, headerPolicy)
	} else {
		override = sandboxcfg.CloneCheckpointPolicy(override)
	}
	err = a.core.Pause(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), override)
	switch {
	case errors.Is(err, ErrAlreadyPaused):
		w.WriteHeader(409)
	case err != nil:
		a.fail(w, err)
	default:
		w.WriteHeader(204)
	}
}

func decodePauseRequest(body io.Reader) (PauseRequest, error) {
	dec := json.NewDecoder(body)
	var raw json.RawMessage
	if err := dec.Decode(&raw); err != nil {
		if errors.Is(err, io.EOF) {
			return PauseRequest{}, nil
		}
		return PauseRequest{}, err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return PauseRequest{}, ErrBadRequest
		}
		return PauseRequest{}, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return PauseRequest{}, ErrBadRequest
	}
	var req PauseRequest
	if err := json.Unmarshal(trimmed, &req); err != nil {
		return PauseRequest{}, err
	}
	return req, nil
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
		Name       string   `json:"name"`
		Tags       []string `json:"tags"`
		Profile    string   `json:"profile"`
		CPUCount   int      `json:"cpuCount"`
		CPUCountSn int      `json:"cpu_count"`
		MemoryMB   int      `json:"memoryMB"`
		MemoryMBSn int      `json:"memory_mb"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	profile, err := requestedBuildProfile(body.Profile)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	// Template config: X-Kuasar-Sandbox-* headers, with the e2b cpu/memory folded
	// into the resource namespace (cpu/memory win over a resource header).
	meta := mergeBuildConfigHeaders(nil, r.Header)
	meta = sandboxcfg.SetCapacity(meta, pickInt(body.CPUCount, body.CPUCountSn), pickInt(body.MemoryMB, body.MemoryMBSn))
	b, err := a.core.RegisterBuild(r.Context(), apiKeyFrom(r.Context()), RegisterSpec{
		Name: body.Name, Tags: body.Tags, Profile: profile, Metadata: meta,
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
		CPUCount     int                  `json:"cpuCount"`
		CPUCountSn   int                  `json:"cpu_count"`
		MemoryMB     int                  `json:"memoryMB"`
		MemoryMBSn   int                  `json:"memory_mb"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad body")
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
	// Trigger-time template config overrides register: headers + e2b cpu/memory.
	meta := mergeBuildConfigHeaders(nil, r.Header)
	meta = sandboxcfg.SetCapacity(meta, pickInt(body.CPUCount, body.CPUCountSn), pickInt(body.MemoryMB, body.MemoryMBSn))
	err := a.core.TriggerBuild(r.Context(), apiKeyFrom(r.Context()),
		r.PathValue("tid"), r.PathValue("bid"), TriggerSpec{
			FromImage:    body.FromImage,
			FromTemplate: body.FromTemplate,
			Steps:        body.Steps,
			StartCmd:     startCmd,
			ReadyCmd:     readyCmd,
			Metadata:     meta,
		}, auth)
	if err != nil {
		switch {
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
		"status":     b.Status.SDKStatus(),
		"logs":       logs,
		"logEntries": logEntries,
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
	case errors.Is(err, ErrAlreadyExists):
		writeErr(w, http.StatusConflict, "target sandbox already exists")
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
	case errors.Is(err, ErrNotFound):
		writeErr(w, 404, "not found")
	case errors.Is(err, ErrNotAllowed):
		writeErr(w, 403, "credential pair not allowed")
	case errors.Is(err, ErrBadRequest):
		writeErr(w, 400, err.Error())
	default:
		a.log.Warn("api error", "err", err)
		writeErr(w, 500, "internal error")
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

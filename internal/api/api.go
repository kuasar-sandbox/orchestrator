// Package api implements the e2b-compatible control-plane REST surface that the
// unmodified e2b SDK calls. It validates X-API-KEY and delegates to Core.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
)

// ErrAlreadyPaused is returned by Core.Pause when the sandbox is already paused.
var ErrAlreadyPaused = errors.New("already paused")

// ErrNotFound is returned by Core methods when the sandbox id is unknown.
var ErrNotFound = errors.New("sandbox not found")

// ErrNotAllowed is returned by Create/RegisterBuild when the api key's manifest
// key is not in the manifest_keys allowlist (=> 403).
var ErrNotAllowed = errors.New("manifest key not allowed to create")

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

// CreateReq is the decoded POST /sandboxes body (subset the SDK sends).
type CreateReq struct {
	TemplateID string            `json:"templateID"`
	TimeoutSec int               `json:"timeout"`
	Metadata   map[string]string `json:"metadata"`
	EnvVars    map[string]string `json:"envVars"`
	Secure     bool              `json:"secure"`
	APIKey     string            `json:"-"` // injected from X-API-KEY
}

// Core is the orchestrator behaviour the API needs. Every per-resource method
// takes the raw api key; Core resolves it to the tenant manifest key (verifying
// the MAC against the stored, encrypted key) and treats a mismatch as not-found.
// Create/RegisterBuild additionally require the manifest key to be allowlisted.
type Core interface {
	Create(ctx context.Context, req CreateReq) (*types.Sandbox, error) // req.APIKey carries the key
	Get(ctx context.Context, id, apiKey string) (*types.Sandbox, error)
	List(ctx context.Context, apiKey, state string, limit int, cursor string) ([]*types.Sandbox, string, error)
	Kill(ctx context.Context, id, apiKey string) (bool, error)
	Connect(ctx context.Context, id, apiKey, migrationToken string, timeoutSec int) (*types.Sandbox, error)
	Pause(ctx context.Context, id, apiKey string) error // ErrAlreadyPaused / ErrNotFound
	SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error)

	// Template builds (e2b v2/v3 build system, what the SDK uses): POST /v3/templates
	// (register name/cpu/memory) → POST /v2/templates/{tid}/builds/{bid} (start, carries
	// fromImage + fromImageRegistry + steps) → GET …/status (poll). The node pulls +
	// flattens the named image server-side — no client-side docker build/push.
	RegisterBuild(ctx context.Context, apiKey, name string, tags []string) (*types.Build, error)
	TriggerBuild(ctx context.Context, apiKey, templateID, buildID, fromImage, startCmd string, auth BuildAuth) error
	BuildStatus(ctx context.Context, apiKey, templateID, buildID string) (*types.Build, error)
	ListTemplates(ctx context.Context, apiKey string) ([]*types.Build, error)

	// Sandbox export/import (orchestrator extension to the e2b surface). Export turns
	// a paused sandbox's remote snapshot into a reusable template (toTemplate) or a
	// one-line base64 migration token; import recreates a paused sandbox from a token.
	ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error)
	ImportSandbox(ctx context.Context, apiKey, token string) (string, error)
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
	mux.HandleFunc("POST /sandboxes/{id}/pause", a.auth(a.pause))
	mux.HandleFunc("POST /sandboxes/{id}/timeout", a.auth(a.timeout))
	// Template build (e2b v3). The CLI authenticates with a Bearer access token;
	// treated identically to X-API-KEY (both derive the tenant via apikey).
	mux.HandleFunc("POST /v3/templates", a.auth(a.registerTemplate))
	mux.HandleFunc("POST /v2/templates/{tid}/builds/{bid}", a.auth(a.triggerBuild))
	mux.HandleFunc("GET /templates/{tid}/builds/{bid}/status", a.auth(a.buildStatus))
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
	var body struct {
		Timeout int `json:"timeout"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	migrationToken := r.Header.Get(MigrationTokenHeader)
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

func (a *API) pause(w http.ResponseWriter, r *http.Request) {
	err := a.core.Pause(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()))
	switch {
	case errors.Is(err, ErrAlreadyPaused):
		w.WriteHeader(409)
	case err != nil:
		a.fail(w, err)
	default:
		w.WriteHeader(204)
	}
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
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	b, err := a.core.RegisterBuild(r.Context(), apiKeyFrom(r.Context()), body.Name, body.Tags)
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
	})
}

func (a *API) triggerBuild(w http.ResponseWriter, r *http.Request) {
	// Accept both the simple shape ({fromImage,startCmd}) and the real e2b CLI
	// body ({dockerfile,template_name,start_cmd,ready_cmd,cpu_count,memory_mb,
	// team_id}). The CLI client-side `docker build`s the dockerfile and pushes the
	// result to <mask>/{templateID}:{buildID}; it sends no image ref, so when
	// fromImage is empty the orchestrator derives it from builder_image_uri_mask.
	var body struct {
		FromImage         string `json:"fromImage"`
		FromImageRegistry struct {
			Username string `json:"username"`
			Password string `json:"password"`
		} `json:"fromImageRegistry"`
		StartCmd     string `json:"startCmd"`
		StartCmdE2B  string `json:"start_cmd"`
		ReadyCmd     string `json:"ready_cmd"`
		Dockerfile   string `json:"dockerfile"`
		TemplateName string `json:"template_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad body")
		return
	}
	startCmd := body.StartCmd
	if startCmd == "" {
		startCmd = body.StartCmdE2B
	}
	auth := BuildAuth{
		PullToken:        r.Header.Get(PullTokenHeader),
		RegistryUsername: body.FromImageRegistry.Username,
		RegistryPassword: body.FromImageRegistry.Password,
	}
	err := a.core.TriggerBuild(r.Context(), apiKeyFrom(r.Context()),
		r.PathValue("tid"), r.PathValue("bid"), body.FromImage, startCmd, auth)
	if err != nil {
		a.fail(w, err)
		return
	}
	w.WriteHeader(202)
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
	resp := map[string]any{
		"templateID": tid,
		"buildID":    b.BuildID,
		"status":     b.Status.SDKStatus(),
		"logs":       []string{}, // paginated by logsOffset
		"logEntries": []any{},
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
	var body struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
		writeErr(w, 400, "token required")
		return
	}
	id, err := a.core.ImportSandbox(r.Context(), apiKeyFrom(r.Context()), body.Token)
	if err != nil {
		a.failMigrate(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"sandboxID": id})
}

// failMigrate surfaces export/import errors: these are operator/tenant tools, so the
// concrete message (e.g. "pause X first", "runtime mismatch", "tenant key not on
// this node") is returned rather than collapsed to a generic 500.
func (a *API) failMigrate(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeErr(w, 404, err.Error())
	case errors.Is(err, ErrNotAllowed):
		writeErr(w, 403, err.Error())
	default:
		a.log.Warn("migrate error", "err", err)
		writeErr(w, 400, err.Error())
	}
}

// --- response shaping ---

func (a *API) envdVersion(sb *types.Sandbox) string {
	if sb.Profile() == types.ProfileE2B {
		return "0.6.1"
	}
	return "0.1.0" // bare stub: >=0.1.0 so the SDK does not self-destruct
}

func (a *API) sandboxResp(sb *types.Sandbox) map[string]any {
	return map[string]any{
		"sandboxID":          sb.ID,
		"templateID":         sb.TemplateID,
		"clientID":           "orchestrator",
		"domain":             a.domain,
		"envdVersion":        a.envdVersion(sb),
		"envdAccessToken":    sb.EnvdAccessToken,
		"trafficAccessToken": sb.TrafficAccessToken,
		"alias":              "",
	}
}

func (a *API) sandboxDetail(sb *types.Sandbox) map[string]any {
	d := a.sandboxResp(sb)
	d["state"] = string(sb.State)
	d["startedAt"] = sb.CreatedUnix
	d["endAt"] = sb.DeadlineUnix
	d["metadata"] = sb.Metadata
	return d
}

// listed renders one item of GET /v2/sandboxes. The e2b SDK's ListedSandbox model
// requires clientID/cpuCount/diskSizeMB/memoryMB/sandboxID/templateID/envdVersion/
// state plus startedAt/endAt as ISO-8601 (it isoparse()s them) — a Unix int or a
// missing field crashes next_items(). State is always running/paused here (dead rows
// are deleted on kill), matching the SDK's SandboxState enum.
func (a *API) listed(sb *types.Sandbox) map[string]any {
	end := sb.DeadlineUnix
	if end == 0 { // no deadline: report start so endAt is still a valid timestamp
		end = sb.CreatedUnix
	}
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
		"endAt":       isoUnix(end),
		"metadata":    sb.Metadata,
	}
}

// isoUnix formats a Unix-seconds timestamp as RFC3339 (ISO-8601) for the SDK.
func isoUnix(sec int64) string { return time.Unix(sec, 0).UTC().Format(time.RFC3339) }

func (a *API) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeErr(w, 404, "not found")
	case errors.Is(err, ErrNotAllowed):
		writeErr(w, 403, "manifest key not allowed")
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

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
	Connect(ctx context.Context, id, apiKey string, timeoutSec int) (*types.Sandbox, error)
	Pause(ctx context.Context, id, apiKey string) error // ErrAlreadyPaused / ErrNotFound
	SetTimeout(ctx context.Context, id, apiKey string, timeoutSec int) (bool, error)

	// Template builds. v2 build system (POST /v3/templates + POST /v2/.../builds);
	// v1 build system (deprecated but still the CLI 2.10.3 default): POST /templates
	// (config at create) + no-body POST /templates/{tid}/builds/{bid} (start).
	RegisterBuild(ctx context.Context, apiKey, name string, tags []string) (*types.Build, error)
	TriggerBuild(ctx context.Context, apiKey, templateID, buildID, fromImage, startCmd string) error
	CreateBuildV1(ctx context.Context, apiKey, alias, startCmd string) (*types.Build, error)
	StartBuild(ctx context.Context, apiKey, templateID, buildID string) error
	BuildStatus(ctx context.Context, apiKey, templateID, buildID string) (*types.Build, error)
	ListTemplates(ctx context.Context, apiKey string) ([]*types.Build, error)
}

type API struct {
	core   Core
	domain string
	log    *slog.Logger
}

func New(core Core, domain string, log *slog.Logger) *API {
	return &API{core: core, domain: domain, log: log}
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
	// v1 build system (deprecated; CLI 2.10.3 default): create carries the config,
	// the no-body start kicks the (already image-pushed) build off.
	mux.HandleFunc("POST /templates", a.auth(a.createTemplateV1))
	mux.HandleFunc("POST /templates/{tid}/builds/{bid}", a.auth(a.startBuildV1))
	mux.HandleFunc("GET /templates/{tid}/builds/{bid}/status", a.auth(a.buildStatus))
	mux.HandleFunc("GET /templates", a.auth(a.listTemplates))
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
	if req.TimeoutSec <= 0 {
		req.TimeoutSec = 15 // SDK default for create
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
	var body struct {
		Timeout int `json:"timeout"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	sb, err := a.core.Connect(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), body.Timeout)
	if err != nil {
		a.fail(w, err)
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

// createTemplateV1 handles the deprecated v1 POST /templates: the build config
// arrives here (alias + start command); the dockerfile/cpu/memory are advisory
// (the client docker-builds + pushes the image itself). Returns the ids the
// client then pushes under and starts.
func (a *API) createTemplateV1(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Alias    string `json:"alias"`
		Name     string `json:"name"`
		StartCmd string `json:"start_cmd"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	alias := body.Alias
	if alias == "" {
		alias = body.Name
	}
	b, err := a.core.CreateBuildV1(r.Context(), apiKeyFrom(r.Context()), alias, body.StartCmd)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, 202, map[string]any{
		"templateID": b.TemplateID,
		"buildID":    b.BuildID,
		"public":     false,
		"aliases":    b.Aliases,
		"logsOffset": 0,
	})
}

// startBuildV1 handles the v1 no-body POST /templates/{tid}/builds/{bid}.
func (a *API) startBuildV1(w http.ResponseWriter, r *http.Request) {
	err := a.core.StartBuild(r.Context(), apiKeyFrom(r.Context()), r.PathValue("tid"), r.PathValue("bid"))
	if err != nil {
		a.fail(w, err)
		return
	}
	w.WriteHeader(202)
}

func (a *API) triggerBuild(w http.ResponseWriter, r *http.Request) {
	// Accept both the simple shape ({fromImage,startCmd}) and the real e2b CLI
	// body ({dockerfile,template_name,start_cmd,ready_cmd,cpu_count,memory_mb,
	// team_id}). The CLI client-side `docker build`s the dockerfile and pushes the
	// result to <mask>/{templateID}:{buildID}; it sends no image ref, so when
	// fromImage is empty the orchestrator derives it from builder_image_uri_mask.
	var body struct {
		FromImage    string `json:"fromImage"`
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
	err := a.core.TriggerBuild(r.Context(), apiKeyFrom(r.Context()),
		r.PathValue("tid"), r.PathValue("bid"), body.FromImage, startCmd)
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
	resp := map[string]any{
		"templateID": b.TemplateID, // stays transient; the persist id rides in names+aliases
		"buildID":    b.BuildID,
		"status":     b.Status.SDKStatus(),
		"reason":     b.Reason,
		"logs":       []string{}, // e2b CLI streams build logs from here (paginated by logsOffset)
	}
	if b.PersistID != "" { // surface the self-describing persist id once ready
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

func (a *API) listed(sb *types.Sandbox) map[string]any {
	return map[string]any{
		"sandboxID":   sb.ID,
		"templateID":  sb.TemplateID,
		"state":       string(sb.State),
		"startedAt":   sb.CreatedUnix,
		"endAt":       sb.DeadlineUnix,
		"metadata":    sb.Metadata,
		"envdVersion": a.envdVersion(sb),
	}
}

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

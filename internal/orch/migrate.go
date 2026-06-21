package orch

// Sandbox export/import: turn a paused sandbox's remote (portable) snapshot into
// either a reusable template (--to-template; fork/fan-out, new sid) or a one-line
// migration token that recreates the SAME sandbox on another node (move, same
// sid). Both ride existing sandbox-ctl primitives (snapshot --upload /
// upload-snapshot / run --restore) + the e2b CLI (create / resume); nothing about
// the e2b API/CLI changes. The token carries the sandbox row minus system
// secrets: the tenant manifest_key appears only as a fingerprint — the target
// resolves the real key from its own whitelist (= the create/build precondition).

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
)

const sandboxTokenVersion = 1

// SandboxToken is the portable, base64-encoded migration handle from export-sandbox.
// It is "sandbox-sensitive" (carries the sandbox's own env/metadata/data-plane
// tokens) but contains NO system key — manifest_key is a fingerprint only.
type SandboxToken struct {
	V             int               `json:"v"`
	ID            string            `json:"id"`
	TemplateID    string            `json:"template_id"`
	SnapshotRef   string            `json:"snapshot_ref"` // manifest://<key> (always remote)
	Profile       string            `json:"profile"`
	Env           map[string]string `json:"env,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	DeadlineUnix  int64             `json:"deadline_unix,omitempty"`
	CreatedUnix   int64             `json:"created_unix,omitempty"`
	EnvdAccessTok string            `json:"envd_access_token,omitempty"` // sandbox data-plane token (not a system key)
	TrafAccessTok string            `json:"traffic_access_token,omitempty"`
	MKFingerprint string            `json:"mk_fingerprint"` // hex SHA256(manifest_key)[:12]; NOT the key
	RuntimeDigest string            `json:"runtime_digest"` // sha256 of the profile's guest runtime erofs
}

// ExportSandbox authorizes apiKey against the paused sandbox, ensures its snapshot
// is remote (promoting a local checkpoint if needed), then either returns the
// derived persist template id (toTemplate; fork) or a one-line base64 migration
// token (default; same-sid move). A move relinquishes the source row unless
// keepSource (copy) — the remote snapshot persists either way.
func (o *Orchestrator) ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error) {
	if apiKey == "" {
		return "", fmt.Errorf("export-sandbox: E2B_API_KEY is required")
	}
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return "", err
	}
	if !ownsSandbox(sb, apiKey) {
		return "", fmt.Errorf("export-sandbox: api key does not own sandbox %s", sid)
	}
	if sb.State != types.StatePaused || sb.SnapshotRef == "" {
		return "", fmt.Errorf("export-sandbox: pause %s first (e2b sandbox pause %s)", sid, sid)
	}
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return "", err
	}
	// Ensure the snapshot is remote (portable). A local checkpoint is a bundle
	// path; promote it to a manifest and repoint the row (local files redundant).
	ref := sb.SnapshotRef
	if !strings.HasPrefix(ref, "manifest://") {
		mref, err := o.promote(ctx, sb, ref)
		if err != nil {
			return "", err
		}
		_ = o.st.SetSnapshotRef(ctx, sid, mref)
		_ = os.RemoveAll(filepath.Dir(ref)) // drop the now-redundant local bundle dir
		sb.SnapshotRef, ref = mref, mref
	}
	key := strings.TrimPrefix(ref, "manifest://")

	if toTemplate {
		// A remote snapshot manifest IS a template: assemble the self-describing
		// persist id. No builds row — usable directly via `e2b sandbox create`.
		return types.TemplateID{Profile: tmpl.Profile, Kind: types.KindSnp, Key: key}.String(), nil
	}

	tok, err := o.mintSandboxToken(sb, ref)
	if err != nil {
		return "", err
	}
	if !keepSource {
		_ = o.st.Delete(ctx, sid) // move: relinquish the source (the remote snapshot persists)
	}
	return tok, nil
}

// mintSandboxToken marshals a sandbox + its remote snapshot ref into a base64
// migration token — the handle export-sandbox and the cluster SAVED promote both
// hand a target node. No api-key gating; callers authorize upstream.
func (o *Orchestrator) mintSandboxToken(sb *types.Sandbox, ref string) (string, error) {
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return "", err
	}
	dig, err := sha256File(o.runtimeFileFor(tmpl.Profile))
	if err != nil {
		return "", fmt.Errorf("mint token: hash runtime: %w", err)
	}
	rawMK, _ := hex.DecodeString(sb.ManifestKey)
	b, err := json.Marshal(SandboxToken{
		V: sandboxTokenVersion, ID: sb.ID, TemplateID: sb.TemplateID, SnapshotRef: ref,
		Profile: string(tmpl.Profile), Env: sb.Env, Metadata: sb.Metadata,
		DeadlineUnix: sb.DeadlineUnix, CreatedUnix: sb.CreatedUnix,
		EnvdAccessTok: sb.EnvdAccessToken, TrafAccessTok: sb.TrafficAccessToken,
		MKFingerprint: hex.EncodeToString(apikey.Fingerprint(rawMK)), RuntimeDigest: dig,
	})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// ImportSandbox decodes a migration token, requires the tenant manifest_key to be
// whitelisted here (= the create precondition), checks the token fingerprint + that
// this node's guest runtime matches the snapshot's, then inserts the paused row.
// `e2b sandbox resume <id>` then restores it on this node.
func (o *Orchestrator) ImportSandbox(ctx context.Context, apiKey, token string) (string, error) {
	if apiKey == "" {
		return "", fmt.Errorf("import-sandbox: E2B_API_KEY is required")
	}
	// The tenant key must be on this node (manifest-key add) — same precondition as
	// create — and the api key must resolve to it.
	mk, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return "", err
	}
	if mk == "" {
		return "", fmt.Errorf("import-sandbox: tenant key not on this node — add it first: node-ctl manifest-key add <key>")
	}
	return o.importSandboxWithKey(ctx, mk, token)
}

// importSandboxWithKey decodes a migration token, checks its fingerprint against
// the resolved tenant key mk + that this node's guest runtime matches the
// snapshot's, then inserts the paused row (the caller resumes). The cluster create
// path supplies mk from the predistributed key; the SDK path from the api key.
func (o *Orchestrator) importSandboxWithKey(ctx context.Context, mk, token string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return "", fmt.Errorf("import-sandbox: bad token: %w", err)
	}
	var tok SandboxToken
	if err := json.Unmarshal(raw, &tok); err != nil || tok.ID == "" || tok.SnapshotRef == "" {
		return "", fmt.Errorf("import-sandbox: bad token (id/snapshot_ref missing)")
	}
	rawMK, _ := hex.DecodeString(mk)
	if hex.EncodeToString(apikey.Fingerprint(rawMK)) != tok.MKFingerprint {
		return "", fmt.Errorf("import-sandbox: token is for a different tenant")
	}
	// The snapshot is bound to the guest runtime it was captured under; a different
	// runtime here would fail restore — reject early with a clear message.
	if tok.RuntimeDigest != "" {
		dig, err := sha256File(o.runtimeFileFor(types.Profile(tok.Profile)))
		if err != nil {
			return "", fmt.Errorf("import-sandbox: hash runtime: %w", err)
		}
		if dig != tok.RuntimeDigest {
			return "", fmt.Errorf("import-sandbox: runtime mismatch — this node's %s runtime is %s…, snapshot needs %s…",
				tok.Profile, short(dig), short(tok.RuntimeDigest))
		}
	}
	if existing, _ := o.st.Get(ctx, tok.ID); existing != nil {
		return "", fmt.Errorf("import-sandbox: sandbox %s already exists on this node", tok.ID)
	}
	created := tok.CreatedUnix
	if created == 0 {
		created = time.Now().Unix()
	}
	sb := &types.Sandbox{
		ID: tok.ID, TemplateID: tok.TemplateID, State: types.StatePaused,
		DeadlineUnix: tok.DeadlineUnix, CreatedUnix: created,
		RunDir: o.cfg.Paths.RunRoot + "/" + tok.ID, BaseDir: o.cfg.Paths.BaseRoot + "/" + tok.ID,
		ManifestKey: mk, SnapshotRef: tok.SnapshotRef,
		EnvdAccessToken: tok.EnvdAccessTok, TrafficAccessToken: tok.TrafAccessTok,
		Metadata: tok.Metadata, Env: tok.Env,
	}
	if types.Profile(tok.Profile) == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	if err := o.st.Put(ctx, sb); err != nil {
		return "", err
	}
	return sb.ID, nil
}

// runtimeFileFor returns the guest runtime erofs path for a profile.
func (o *Orchestrator) runtimeFileFor(p types.Profile) string {
	if p == types.ProfileBare {
		return o.cfg.Sandbox.Boot.RuntimeBase
	}
	return o.cfg.Sandbox.Boot.RuntimeE2B
}

func sha256File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func short(s string) string {
	if len(s) > 12 {
		return s[:12]
	}
	return s
}

package orch

// Sandbox export/import: turn a paused sandbox's remote (portable) snapshot into
// either a reusable template (--to-template; fork/fan-out) or a one-line
// migration token that restores the snapshot under a fresh sandbox id on another
// node. Both ride existing sandbox-ctl primitives (snapshot --upload /
// upload-snapshot / run --restore) + the e2b CLI (create / resume); nothing about
// the e2b API/CLI changes. The token carries the sandbox row minus system
// secrets: tenant APISecret and ManifestKey appear only as complete fingerprints;
// the target resolves the real pair from its own allowlist (= the create/build
// precondition).

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

	"github.com/google/uuid"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const sandboxTokenVersion = 1

// SandboxToken is the portable, base64-encoded migration handle from export-sandbox.
// It carries portable snapshot state but no sandbox identity or data-plane
// credentials. Tenant roots are represented by complete fingerprints only.
type SandboxToken struct {
	V                      int               `json:"v"`
	TemplateID             string            `json:"template_id"`
	SnapshotRef            string            `json:"snapshot_ref"` // manifest://<key> (always remote)
	Profile                string            `json:"profile"`
	Env                    map[string]string `json:"env,omitempty"`
	Metadata               map[string]string `json:"metadata,omitempty"`
	DeadlineUnix           int64             `json:"deadline_unix,omitempty"`
	APISecretFingerprint   string            `json:"api_secret_fingerprint"`
	ManifestKeyFingerprint string            `json:"manifest_key_fingerprint"`
	RuntimeDigest          string            `json:"runtime_digest"` // sha256 of the profile's guest runtime erofs
}

// ExportSandbox authorizes apiKey against the paused sandbox, ensures its snapshot
// is remote (promoting a local checkpoint if needed), then either returns the
// derived persist template id (toTemplate; fork) or a one-line base64 migration
// token (default). A move relinquishes the source row unless
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
	if sb.Profile != tmpl.Profile {
		return "", fmt.Errorf("export-sandbox: sandbox profile %q does not match template profile %q", sb.Profile, tmpl.Profile)
	}
	// Ensure the snapshot is remote (portable). A local checkpoint is a bundle
	// path; promote it to a manifest and repoint the row (local files redundant).
	ref := sb.SnapshotRef
	if !strings.HasPrefix(ref, "manifest://") {
		localRef := ref
		mref, err := o.promote(ctx, sb, localRef)
		if err != nil {
			return "", err
		}
		if err := o.st.SetSnapshotRef(ctx, sid, mref); err != nil {
			return "", fmt.Errorf("export-sandbox: persist promoted snapshot ref for %s: %w", sid, err)
		}
		sb.SnapshotRef, ref = mref, mref
		o.cache(sb)
		o.publishUpsert(sb)
		if err := os.RemoveAll(filepath.Dir(localRef)); err != nil {
			o.log.Warn("export-sandbox: remove redundant local snapshot", "sid", sid, "path", filepath.Dir(localRef), "err", err)
		}
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
		if err := o.st.Delete(ctx, sid); err != nil { // move: remote snapshot persists
			return "", fmt.Errorf("export-sandbox: delete source %s: %w", sid, err)
		}
		o.uncache(sid)
		o.publishDelete(sid)
	}
	return tok, nil
}

// mintSandboxToken marshals a sandbox + its remote snapshot ref into a base64
// migration token. No api-key gating; callers authorize upstream.
func (o *Orchestrator) mintSandboxToken(sb *types.Sandbox, ref string) (string, error) {
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return "", err
	}
	if sb.Profile != tmpl.Profile {
		return "", fmt.Errorf("mint token: sandbox profile %q does not match template profile %q", sb.Profile, tmpl.Profile)
	}
	dig, err := sha256File(o.runtimeFileFor(tmpl.Profile))
	if err != nil {
		return "", fmt.Errorf("mint token: hash runtime: %w", err)
	}
	apiFingerprint, err := store.APISecretHash(sb.APISecret)
	if err != nil {
		return "", err
	}
	manifestFingerprint, err := store.ManifestKeyHash(sb.ManifestKey)
	if err != nil {
		return "", err
	}
	b, err := json.Marshal(SandboxToken{
		V: sandboxTokenVersion, TemplateID: sb.TemplateID, SnapshotRef: ref,
		Profile: string(tmpl.Profile), Env: sb.Env, Metadata: sb.Metadata,
		DeadlineUnix:           sb.DeadlineUnix,
		APISecretFingerprint:   apiFingerprint,
		ManifestKeyFingerprint: manifestFingerprint,
		RuntimeDigest:          dig,
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
	pair, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return "", err
	}
	if pair.APISecret == "" {
		return "", fmt.Errorf("import-sandbox: tenant key not on this node — add it first: node-ctl manifest-key add <key>")
	}
	return o.importSandboxWithKey(ctx, pair, token)
}

// importSandboxWithKey decodes a migration token, checks its fingerprint against
// the resolved tenant key mk + that this node's guest runtime matches the
// snapshot's, then inserts the paused row (the caller resumes). The cluster create
// path supplies mk from the predistributed key; the SDK path from the api key.
func (o *Orchestrator) importSandboxWithKey(ctx context.Context, pair store.KeyPair, token string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return "", fmt.Errorf("import-sandbox: bad token: %w", err)
	}
	var tok SandboxToken
	if err := json.Unmarshal(raw, &tok); err != nil || tok.TemplateID == "" || tok.SnapshotRef == "" || tok.Profile == "" {
		return "", fmt.Errorf("import-sandbox: bad token (template_id/snapshot_ref/profile missing)")
	}
	profile, err := types.ParseProfile(tok.Profile)
	if err != nil {
		return "", fmt.Errorf("import-sandbox: bad token profile: %w", err)
	}
	template, err := types.ParseTemplateID(tok.TemplateID)
	if err != nil {
		return "", fmt.Errorf("import-sandbox: bad token template: %w", err)
	}
	if template.Profile != profile {
		return "", fmt.Errorf("import-sandbox: token profile %q does not match template profile %q", profile, template.Profile)
	}
	apiFingerprint, err := store.APISecretHash(pair.APISecret)
	if err != nil {
		return "", err
	}
	manifestFingerprint, err := store.ManifestKeyHash(pair.ManifestKey)
	if err != nil {
		return "", err
	}
	if apiFingerprint != tok.APISecretFingerprint || manifestFingerprint != tok.ManifestKeyFingerprint {
		return "", fmt.Errorf("import-sandbox: token is for a different tenant")
	}
	// The snapshot is bound to the guest runtime it was captured under; a different
	// runtime here would fail restore — reject early with a clear message.
	if tok.RuntimeDigest != "" {
		dig, err := sha256File(o.runtimeFileFor(profile))
		if err != nil {
			return "", fmt.Errorf("import-sandbox: hash runtime: %w", err)
		}
		if dig != tok.RuntimeDigest {
			return "", fmt.Errorf("import-sandbox: runtime mismatch — this node's %s runtime is %s…, snapshot needs %s…",
				tok.Profile, short(dig), short(tok.RuntimeDigest))
		}
	}
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("import-sandbox: new id: %w", err)
	}
	sid := id.String()
	envdToken, err := keys.MintToken()
	if err != nil {
		return "", fmt.Errorf("import-sandbox: mint envd token: %w", err)
	}
	trafficToken, err := keys.MintToken()
	if err != nil {
		return "", fmt.Errorf("import-sandbox: mint traffic token: %w", err)
	}
	sb := &types.Sandbox{
		ID: sid, Profile: profile, TemplateID: tok.TemplateID, State: types.StatePaused,
		DeadlineUnix: tok.DeadlineUnix, CreatedUnix: time.Now().Unix(),
		RunDir: o.cfg.Paths.RunRoot + "/" + sid, BaseDir: o.cfg.Paths.BaseRoot + "/" + sid,
		APISecret: pair.APISecret, ManifestKey: pair.ManifestKey, SnapshotRef: tok.SnapshotRef,
		EnvdAccessToken: envdToken, TrafficAccessToken: trafficToken,
		Metadata: tok.Metadata, Env: tok.Env,
	}
	if profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	if err := o.st.Put(ctx, sb); err != nil {
		return "", err
	}
	return sb.ID, nil
}

// runtimeFileFor returns the guest runtime erofs path.
func (o *Orchestrator) runtimeFileFor(p types.Profile) string {
	return o.cfg.Sandbox.Boot.Runtime
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

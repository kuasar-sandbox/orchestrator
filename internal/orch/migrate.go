package orch

// Sandbox export/import turns a paused sandbox's portable snapshot into either a
// reusable template or an opaque kmt1 migration token. The token preserves the
// logical sandbox state and service credentials while carrying only fingerprints
// of tenant API and manifest roots. The target obtains those roots from its own
// trusted key store before it can authenticate and decrypt the token.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// ExportSandbox authorizes apiKey against the paused sandbox, ensures its snapshot
// is remote (promoting a local checkpoint if needed), then either returns the
// derived persist template id (toTemplate; fork) or a one-line opaque kmt1
// token (default). A move relinquishes the source row unless
// keepSource (copy) — the remote snapshot persists either way.
func (o *Orchestrator) ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (string, error) {
	unlock := o.lifecycle.Lock(sid)
	defer unlock()

	if apiKey == "" {
		return "", fmt.Errorf("export-sandbox: API key is required: %w", api.ErrNotAllowed)
	}
	sb, err := o.st.Get(ctx, sid)
	if err != nil {
		return "", err
	}
	if !ownsSandbox(sb, apiKey) {
		return "", api.ErrNotFound
	}
	o.cancelResumeRequests(sid)
	if sb.State != types.StatePaused || sb.SnapshotRef == "" {
		return "", fmt.Errorf("export-sandbox: pause %s first (e2b sandbox pause %s): %w", sid, sid, api.ErrBadRequest)
	}
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return "", err
	}
	if sb.Profile != tmpl.Profile {
		return "", fmt.Errorf("export-sandbox: sandbox profile %q does not match template profile %q", sb.Profile, tmpl.Profile)
	}
	// Ensure the snapshot is portable. A local checkpoint is published through
	// the configured manifest or named-location publisher and then repointed.
	ref := sb.SnapshotRef
	if !types.IsPortableRef(ref) {
		localRef := ref
		portableRef, err := o.promote(ctx, sb, localRef)
		if err != nil {
			return "", err
		}
		if err := o.st.SetSnapshotRef(ctx, sid, portableRef); err != nil {
			return "", fmt.Errorf("export-sandbox: persist promoted snapshot ref for %s: %w", sid, err)
		}
		sb.SnapshotRef, ref = portableRef, portableRef
		o.cache(sb)
		o.publishUpsert(sb)
		if err := os.RemoveAll(filepath.Dir(localRef)); err != nil {
			o.log.Warn("export-sandbox: remove redundant local snapshot", "sid", sid, "path", filepath.Dir(localRef), "err", err)
		}
	}
	if toTemplate {
		id := types.TemplateID{Profile: tmpl.Profile, Kind: types.KindSnp, Ref: ref}.String()
		if _, err := types.ParseTemplateID(id); err != nil {
			return "", fmt.Errorf("export-sandbox: snapshot template: %w", err)
		}
		return id, nil
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
		o.clearDeadlineIntent(sid)
		o.publishDelete(sid)
	}
	return tok, nil
}

// mintSandboxToken seals a complete portable sandbox record. There is no API-key
// gate here; ExportSandbox performs object authorization before calling it.
func (o *Orchestrator) mintSandboxToken(sb *types.Sandbox, ref string) (string, error) {
	if sb == nil {
		return "", fmt.Errorf("mint migration token: sandbox is required")
	}
	tmpl, err := types.ParseTemplateID(sb.TemplateID)
	if err != nil {
		return "", fmt.Errorf("mint migration token: template: %w", err)
	}
	if sb.Profile != tmpl.Profile {
		return "", fmt.Errorf("mint migration token: sandbox profile %q does not match template profile %q", sb.Profile, tmpl.Profile)
	}
	dig, err := sha256File(o.runtimeFileFor(tmpl.Profile))
	if err != nil {
		return "", fmt.Errorf("mint migration token: hash runtime: %w", err)
	}
	apiFingerprint, err := store.APISecretHash(sb.APISecret)
	if err != nil {
		return "", fmt.Errorf("mint migration token: API secret fingerprint: %w", err)
	}
	manifestFingerprint, err := store.ManifestKeyHash(sb.ManifestKey)
	if err != nil {
		return "", fmt.Errorf("mint migration token: manifest key fingerprint: %w", err)
	}
	return migrationtoken.Seal(
		migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey},
		migrationtoken.MigrationTokenPayloadV1{
			Version:                1,
			NodeSandboxID:          sb.ID,
			AuthSandboxID:          sb.AuthSandboxID(),
			APISecretFingerprint:   apiFingerprint,
			ManifestKeyFingerprint: manifestFingerprint,
			TemplateID:             sb.TemplateID,
			Profile:                string(sb.Profile),
			RuntimeDigest:          dig,
			SnapshotRef:            ref,
			Env:                    sb.Env,
			Metadata:               sb.Metadata,
			CreatedUnix:            sb.CreatedUnix,
			DeadlineUnix:           sb.DeadlineUnix,
			ServiceSecret:          sb.ServiceSecret,
			EnvdAccessToken:        sb.EnvdAccessToken,
			TrafficAccessToken:     sb.TrafficAccessToken,
			ForwardAccessToken:     sb.ForwardAccessToken,
		},
	)
}

// ImportSandbox authenticates the caller's paired tenant roots and inserts the
// token as a paused standalone sandbox. An empty targetID reuses the source
// NodeSandboxID; a non-empty targetID replaces only the node-local identity.
func (o *Orchestrator) ImportSandbox(ctx context.Context, apiKey, token, targetID string) (string, error) {
	if apiKey == "" {
		return "", fmt.Errorf("import-sandbox: API key is required: %w", api.ErrNotAllowed)
	}
	// The tenant key must be on this node (manifest-key add) — same precondition as
	// create — and the api key must resolve to it.
	pair, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return "", err
	}
	if pair.APISecret == "" {
		return "", fmt.Errorf("import-sandbox: credential pair is not installed: %w", api.ErrNotAllowed)
	}
	sb, err := o.importSandboxWithKey(ctx, pair, token, targetID, migrationtoken.Expectations{}, nil, 0)
	if err != nil {
		return "", err
	}
	return sb.ID, nil
}

// importSandboxWithKey is the shared synchronous KMT import core. expected and
// cluster are trusted caller inputs: standalone import passes zero values, while
// cluster commands can constrain the token subject/profile/runtime and attach
// system-owned Group/RouteKey state. A positive deadlineOverride is written as
// part of the insert; zero preserves the token deadline. The token never
// supplies trusted cluster context. There is no MMDS redeclaration on
// connect/exec-session: the token's own carried kuasar-sandbox.mmds (if any)
// is the only value in play for an import, in both branches below.
func (o *Orchestrator) importSandboxWithKey(
	ctx context.Context,
	pair store.KeyPair,
	token, targetID string,
	expected migrationtoken.Expectations,
	cluster *types.ClusterSandboxContext,
	deadlineOverride int64,
) (*types.Sandbox, error) {
	if targetID != "" && !types.ValidLocalSandboxID(targetID) {
		return nil, fmt.Errorf("import-sandbox: invalid target sandbox ID: %w", api.ErrBadRequest)
	}
	payload, err := migrationtoken.Open(
		migrationtoken.KeyMaterial{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey},
		token,
	)
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: open migration token: %w", err)
	}
	if err := migrationtoken.ValidateExpectations(payload, expected); err != nil {
		return nil, fmt.Errorf("import-sandbox: validate target: %w", err)
	}
	profile, err := types.ParseProfile(payload.Profile)
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: profile: %w", err)
	}
	runtimeDigest, err := sha256File(o.runtimeFileFor(profile))
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: hash runtime: %w", err)
	}
	if runtimeDigest != payload.RuntimeDigest {
		return nil, fmt.Errorf("import-sandbox: %w: runtime", migrationtoken.ErrIncompatible)
	}
	if targetID == "" {
		targetID = payload.NodeSandboxID
	}
	if !types.ValidLocalSandboxID(targetID) {
		return nil, fmt.Errorf("import-sandbox: invalid target sandbox ID: %w", api.ErrBadRequest)
	}

	var trustedCluster *types.ClusterSandboxContext
	metadata := migrationSandboxMetadata(payload.Metadata)
	if cluster != nil {
		trustedCluster = &types.ClusterSandboxContext{Group: cluster.Group, RouteKey: cluster.RouteKey}
		metadata = clusterSandboxMetadata(metadata)
	}
	if cluster != nil {
		// The token's own kuasar-sandbox.mmds (which may originate from a
		// different cluster, a standalone node, or a pre-MMDS-feature row)
		// is kept as the default rather than discarded, now checked against
		// this node's own mmds.routes policy -- the same policy standalone
		// uses, and the only policy cluster mode has (see admittedMMDSMetadata's
		// doc comment: cluster has no registry-owned policy, so every node in
		// the cluster is expected to run the same mmds.routes configuration).
		// A value that fails this check is simply dropped, not treated as a
		// hard failure -- it's carried-forward legacy state, not the tenant's
		// fresh input for this specific request, so a malformed or
		// policy-incompatible value must never turn an otherwise-recoverable
		// migration into a 400.
		if raw, mmdsPresent := metadata[sandboxcfg.NsMMDS]; mmdsPresent {
			if _, canonical, extractErr := sandboxcfg.ExtractMMDS(map[string]string{sandboxcfg.NsMMDS: raw}, o.mmdsPolicy()); extractErr == nil {
				metadata[sandboxcfg.NsMMDS] = canonical[sandboxcfg.NsMMDS]
			} else {
				o.log.Warn("import-sandbox: dropping the migration token's own MMDS metadata that fails this node's policy", "sid", targetID, "err", extractErr)
				delete(metadata, sandboxcfg.NsMMDS)
			}
		}
	} else {
		_, mmdsPresent := metadata[sandboxcfg.NsMMDS]
		_, metadata, err = sandboxcfg.ExtractMMDS(metadata, o.mmdsPolicy())
		// Unlike cluster import (registry can retry another node on
		// ErrTargetIncompatible), a standalone import has nowhere else to go, so
		// an unavailable runtime is a hard client-facing error, not a retryable
		// one -- same distinction Create makes for the same condition.
		if err == nil && mmdsPresent && !o.MMDSRuntimeAvailable() {
			return nil, fmt.Errorf("import-sandbox: %w: MMDS is unavailable on this node", api.ErrBadRequest)
		}
	}
	if err != nil {
		var validationErr *sandboxcfg.MMDSValidationError
		if errors.As(err, &validationErr) {
			o.log.Warn("MMDS metadata rejected", "operation", "import", "err", validationErr.Diagnostic())
		}
		return nil, fmt.Errorf("%w: import-sandbox: %v", api.ErrBadRequest, err)
	}
	sb := &types.Sandbox{
		ID:                 targetID,
		Profile:            profile,
		Cluster:            trustedCluster,
		AuthSandboxIDValue: payload.AuthSandboxID,
		TemplateID:         payload.TemplateID,
		State:              types.StatePaused,
		DeadlineUnix:       payload.DeadlineUnix,
		CreatedUnix:        payload.CreatedUnix,
		RunDir:             filepath.Join(o.cfg.Paths.RunRoot, targetID),
		BaseDir:            filepath.Join(o.cfg.Paths.BaseRoot, targetID),
		APISecret:          pair.APISecret,
		ManifestKey:        pair.ManifestKey,
		SnapshotRef:        payload.SnapshotRef,
		ServiceSecret:      payload.ServiceSecret,
		EnvdAccessToken:    payload.EnvdAccessToken,
		TrafficAccessToken: payload.TrafficAccessToken,
		ForwardAccessToken: payload.ForwardAccessToken,
		Metadata:           metadata,
		Env:                payload.Env,
	}
	if deadlineOverride > 0 {
		sb.DeadlineUnix = deadlineOverride
	}
	if profile == types.ProfileE2B {
		sb.EnvdUDS = sb.RunDir + "/envd.sock"
		sb.CiUDS = sb.RunDir + "/ci.sock"
	}
	// Defense-in-depth: reaching the canonical size that overflows RouteEntry
	// once double-escaped (see validateMMDSRouteEntryTransport) requires
	// getting within a few dozen bytes of maxMMDSCanonicalBytes (512 KiB),
	// which is also exactly migrationtoken.MaxWireSize -- no real migration
	// token can carry a specification that large plus the rest of the
	// sandbox record plus encryption/framing overhead, so this cannot fire
	// through a real token today. It stays wired because those two constants
	// live in unrelated packages with no enforced relationship; either one
	// changing alone later would silently reopen this without it.
	if raw, mmdsPresent := sb.Metadata[sandboxcfg.NsMMDS]; mmdsPresent {
		// routeEntryWithMMDS (not routeEntry): for the cluster branch, this
		// dormant carried-forward value is deliberately not gated on
		// MMDSRuntimeAvailable() above, so using it directly predicts the size
		// that will actually publish once capability returns, not the ""
		// admittedMMDSMetadata reports right now. For the standalone branch,
		// MMDSRuntimeAvailable() was already confirmed true above, so it's the
		// same value admittedMMDSMetadata would derive anyway -- just without
		// paying a second ExtractMMDS pass for it.
		entry := o.routeEntryWithMMDS(sb, raw)
		if err := validateMMDSRouteEntryTransport(entry); err != nil {
			if cluster != nil {
				// Same treatment as a malformed or policy-incompatible token
				// value above: carried-forward legacy state, not the tenant's
				// fresh input for this request, so this must never turn an
				// otherwise-recoverable migration into a hard failure.
				o.log.Warn("import-sandbox: dropping the migration token's own MMDS metadata that would overflow the route-sync frame once published", "sid", targetID, "err", err)
				delete(sb.Metadata, sandboxcfg.NsMMDS)
			} else {
				return nil, fmt.Errorf("import-sandbox: %w: %v", api.ErrBadRequest, err)
			}
		}
	}
	if err := o.st.InsertSandbox(ctx, sb); err != nil {
		if errors.Is(err, store.ErrSandboxExists) {
			return nil, fmt.Errorf("import-sandbox: %w", api.ErrAlreadyExists)
		}
		return nil, fmt.Errorf("import-sandbox: insert target %s: %w", targetID, err)
	}
	return sb, nil
}

// migrationSandboxMetadata preserves ordinary portable metadata while ensuring
// the request-only credentials carrier can never re-enter metadata_json. The
// service credentials restored from KMT1 come only from their typed payload
// fields and are validated separately.
func migrationSandboxMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	cleaned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if key != sandboxcfg.NsCredentials {
			cleaned[key] = value
		}
	}
	return cleaned
}

// runtimeFileFor returns the guest runtime erofs path.
func (o *Orchestrator) runtimeFileFor(_ types.Profile) string {
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

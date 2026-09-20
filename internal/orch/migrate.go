package orch

// Sandbox export/import turns a paused sandbox's portable resume artifact into either a
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
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	artifactresult "github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// ExportSandbox publishes a paused sandbox artifact, then returns either a
// reusable template id or an opaque kmt1 migration token. Publish does not hold
// the source lifecycle lock: an accepted resume may preempt a KMT export or
// detach a template export before the short source finalizer begins.
func (o *Orchestrator) ExportSandbox(ctx context.Context, apiKey, sid string, toTemplate, keepSource bool) (types.ExportResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return types.ExportResult{}, err
	}
	if apiKey == "" {
		return types.ExportResult{}, fmt.Errorf("export-sandbox: API key is required: %w", api.ErrNotAllowed)
	}
	lifecycleCtx := o.launchContext()
	finishOperation, err := o.acceptedOps.Begin(lifecycleCtx)
	if err != nil {
		return types.ExportResult{}, fmt.Errorf("orch: lifecycle is stopping: %w", err)
	}
	opCtx, cancelUpload := context.WithCancelCause(ctx)
	stopLifecycle := context.AfterFunc(lifecycleCtx, func() {
		cancelUpload(context.Cause(lifecycleCtx))
	})

	var (
		source  *types.Sandbox
		tmpl    types.TemplateID
		attempt *exportAttempt
	)
	defer func() {
		stopLifecycle()
		cancelUpload(nil)
		o.exports.Finish(attempt)
		finishOperation()
	}()
	for {
		unlock, err := o.lockLifecycleMutation(opCtx, sid)
		if err != nil {
			return types.ExportResult{}, context.Cause(opCtx)
		}
		sb, err := o.st.Get(opCtx, sid)
		if err != nil {
			unlock()
			return types.ExportResult{}, err
		}
		if !ownsSandbox(sb, apiKey) {
			unlock()
			return types.ExportResult{}, api.ErrNotFound
		}
		if sb.State != types.StatePaused || !sb.ResumeSource.Valid() {
			unlock()
			return types.ExportResult{}, fmt.Errorf("export-sandbox: pause %s first (e2b sandbox pause %s): %w", sid, sid, api.ErrBadRequest)
		}
		tmpl, err = types.ParseTemplateID(sb.TemplateID)
		if err != nil {
			unlock()
			return types.ExportResult{}, err
		}
		if sb.Profile != tmpl.Profile {
			unlock()
			return types.ExportResult{}, fmt.Errorf("export-sandbox: sandbox profile %q does not match template profile %q", sb.Profile, tmpl.Profile)
		}

		source = cloneSandbox(sb)
		var started bool
		attempt, started = o.exports.Begin(sid, source.ResumeSource, toTemplate, cancelUpload)
		unlock()
		if started {
			break
		}
	}

	// Phase one only publishes and prepares the immutable result. It never
	// mutates the source row, cache, route, or local checkpoint.
	artifact := source.ResumeSource
	report := artifactresult.PublishReport{SandboxRef: artifact.Ref, RemovedRefs: []string{}}
	if artifact.Kind == types.ResumeSourceSnapshot {
		report.SnapshotRef, report.SandboxRef = artifact.Ref, artifact.SandboxRef
	}
	localArtifactDir := ""
	if !types.IsPortableRef(artifact.Ref) {
		localArtifactDir, err = o.ownedLocalArtifactDir(source, artifact)
		if err != nil {
			return types.ExportResult{}, err
		}
		publish := o.promote
		if o.artifactPublisher != nil {
			publish = o.artifactPublisher
		}
		published, err := publish(opCtx, source, artifact)
		if err != nil {
			if o.exports.State(attempt) == exportPreempted {
				return types.ExportResult{}, exportPreemptedError(sid)
			}
			return types.ExportResult{}, err
		}
		if err := published.Validate(artifactRole(artifact.Kind)); err != nil {
			return types.ExportResult{}, fmt.Errorf("export-sandbox: publisher changed or returned an invalid %s source", artifact.Kind)
		}
		report = published
		artifact.Ref = report.SandboxRef
		if artifact.Kind == types.ResumeSourceSnapshot {
			artifact.Ref, artifact.SandboxRef = report.SnapshotRef, report.SandboxRef
		}
	}

	if err := report.Validate(artifactRole(artifact.Kind)); err != nil {
		return types.ExportResult{}, err
	}

	if o.exports.State(attempt) == exportPreempted {
		return types.ExportResult{}, exportPreemptedError(sid)
	}
	if err := opCtx.Err(); err != nil {
		return types.ExportResult{}, context.Cause(opCtx)
	}
	var result string
	if toTemplate {
		kind := types.KindSnp
		if artifact.Kind == types.ResumeSourceSandbox {
			kind = types.KindSbx
		}
		result = types.TemplateID{Profile: tmpl.Profile, Kind: kind, Ref: artifact.Ref}.String()
		if _, err := types.ParseTemplateID(result); err != nil {
			return types.ExportResult{}, fmt.Errorf("export-sandbox: artifact template: %w", err)
		}
	} else {
		var err error
		result, err = o.mintSandboxToken(source, artifact)
		if err != nil {
			if o.exports.State(attempt) == exportPreempted {
				return types.ExportResult{}, exportPreemptedError(sid)
			}
			return types.ExportResult{}, err
		}
	}

	response := types.ExportResult{Result: result, PublishReport: report}
	if err := response.Validate(); err != nil {
		return types.ExportResult{}, err
	}

	// A detached template result no longer owns source finalization. Returning it
	// directly also keeps accepted resume launch commits independent of upload.
	switch o.exports.State(attempt) {
	case exportPreempted:
		return types.ExportResult{}, exportPreemptedError(sid)
	}
	if err := lifecycleCtx.Err(); err != nil {
		return types.ExportResult{}, fmt.Errorf("orch: lifecycle is stopping: %w", err)
	}
	if err := opCtx.Err(); err != nil {
		return types.ExportResult{}, context.Cause(opCtx)
	}
	if o.exports.State(attempt) == exportDetached {
		return response, nil
	}

	// Phase two has one short linearization point against Resume. Once this lock
	// is held and BeginFinalize succeeds, Resume waits for the durable source
	// mutation and local cleanup attempt to finish.
	unlock := o.lifecycle.Lock(sid)
	defer unlock()
	if o.exports.State(attempt) == exportPreempted {
		return types.ExportResult{}, exportPreemptedError(sid)
	}
	if err := lifecycleCtx.Err(); err != nil {
		return types.ExportResult{}, fmt.Errorf("orch: lifecycle is stopping: %w", err)
	}
	if err := opCtx.Err(); err != nil {
		return types.ExportResult{}, context.Cause(opCtx)
	}
	switch o.exports.BeginFinalize(attempt) {
	case exportPreempted:
		return types.ExportResult{}, exportPreemptedError(sid)
	case exportDetached:
		return response, nil
	case exportFinalizing:
	default:
		return types.ExportResult{}, fmt.Errorf("export-sandbox: invalid export attempt state")
	}

	// Request/lifecycle cancellation may abandon export before BeginFinalize,
	// but it cannot split an already-won durable source commit. Preserve context
	// values while finishing this bounded local critical section.
	finalizeCtx := context.WithoutCancel(opCtx)
	current, err := o.st.Get(finalizeCtx, sid)
	if err != nil {
		return types.ExportResult{}, err
	}
	if current == nil || current.State != types.StatePaused || current.ResumeSource != attempt.source ||
		current.CreatedUnix != source.CreatedUnix || current.TemplateID != source.TemplateID ||
		current.StableID() != source.StableID() {
		return types.ExportResult{}, exportPreemptedError(sid)
	}
	exportKind := "kmt"
	if toTemplate {
		exportKind = "template"
	}
	o.log.Info("export won source finalization", "sid", sid, "export_kind", exportKind, "keep_source", keepSource)

	if keepSource {
		// Retain the source unchanged: keep-source produces an export result
		// (template id or KMT token) but does not modify the source sandbox's
		// ResumeSource, row, cache, or route, and does not remove its
		// checkpoint. The source resumes from exactly where it would have
		// resumed without the export (#336).
		return response, nil
	}
	cleanupCtx, cancelCleanup := cleanupContext()
	cleanupErr := o.teardownPersistedOwnership(cleanupCtx, current, false)
	cancelCleanup()
	if cleanupErr != nil {
		return types.ExportResult{}, fmt.Errorf("export-sandbox: teardown source %s: %w", sid, cleanupErr)
	}
	if err := o.st.Delete(finalizeCtx, sid); err != nil {
		o.cache(current)
		return types.ExportResult{}, fmt.Errorf("export-sandbox: delete source %s: %w", sid, err)
	}
	o.runs.forget(current.RunID)
	o.releaseDetachedPortFence(current.VswitchPort)
	o.uncache(sid)
	o.clearDeadlineIntent(sid)
	o.publishDelete(sid)
	o.observeSandboxDelete(current)
	if localArtifactDir != "" {
		if err := os.RemoveAll(localArtifactDir); err != nil {
			o.log.Warn("export-sandbox: remove finalized local artifact", "sid", sid, "path", localArtifactDir, "err", err)
		}
	}
	return response, nil
}

// ownedLocalArtifactDir returns the only directory ExportSandbox may publish
// and recursively remove for a node-local pause artifact. The exact path is a
// capture invariant, not a value trusted merely because it was persisted in a
// row: malformed state must fail closed before sandbox-ctl reads it or cleanup
// derives a broader deletion target from it.
func (o *Orchestrator) ownedLocalArtifactDir(sb *types.Sandbox, source types.ResumeSource) (string, error) {
	if sb == nil || sb.ID == "" || sb.BaseDir == "" {
		return "", fmt.Errorf("export-sandbox: local artifact owner is incomplete")
	}
	wantBaseDir := nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, sb.ID)
	if sb.BaseDir != wantBaseDir {
		return "", fmt.Errorf("export-sandbox: sandbox BaseDir %q does not match canonical path %q", sb.BaseDir, wantBaseDir)
	}
	dir := filepath.Join(wantBaseDir, "checkpoint")
	if !source.Valid() {
		return "", fmt.Errorf("export-sandbox: incomplete local source")
	}
	check := func(raw string, role types.ResumeSourceKind) error {
		if types.IsPortableRef(raw) {
			return nil
		}
		if !strings.HasPrefix(raw, "file://") {
			if filepath.Clean(raw) == filepath.Join(dir, sb.ID+"."+string(role)) {
				return nil
			}
			return fmt.Errorf("export-sandbox: local %s source is outside its owned capture path", role)
		}
		ref, err := manifest.ParseRef(raw)
		if err != nil || ref.Location != "" || ref.Digest == "" ||
			ref.Path != filepath.Base(ref.Path) || ref.Path == "." || ref.Path == ".." || strings.ContainsAny(ref.Path, `/\`) ||
			(!strings.HasSuffix(ref.Path, ".bundle") && !strings.HasSuffix(ref.Path, "."+string(role))) {
			return fmt.Errorf("export-sandbox: local %s source is outside its owned capture path", role)
		}
		return nil
	}
	if err := check(source.Ref, source.Kind); err != nil {
		return "", err
	}
	if source.Kind == types.ResumeSourceSnapshot {
		if err := check(source.SandboxRef, types.ResumeSourceSandbox); err != nil {
			return "", err
		}
	}
	return dir, nil
}

func exportPreemptedError(sid string) error {
	return fmt.Errorf("export-sandbox: resume accepted for %s: %w", sid, api.ErrExportPreempted)
}

// mintSandboxToken seals a complete portable sandbox record. There is no API-key
// gate here; ExportSandbox performs object authorization before calling it.
func (o *Orchestrator) mintSandboxToken(sb *types.Sandbox, source types.ResumeSource) (string, error) {
	if sb == nil {
		return "", fmt.Errorf("mint migration token: sandbox is required")
	}
	if !source.Valid() || !types.IsPortableRef(source.Ref) {
		return "", fmt.Errorf("mint migration token: portable resume source is required")
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
	metadata, err := sandboxcfg.NormalizeResourceMetadata(sb.Metadata)
	if err != nil {
		return "", fmt.Errorf("mint migration token: portable resource metadata: %w", err)
	}
	metadata, err = sandboxcfg.NormalizeTrafficMetadata(metadata)
	if err != nil {
		return "", fmt.Errorf("mint migration token: portable traffic metadata: %w", err)
	}
	trafficSpec, err := sandboxcfg.ParseSpec(metadata)
	if err != nil {
		return "", fmt.Errorf("mint migration token: portable traffic metadata: %w", err)
	}
	if err := sandboxcfg.ValidateTrafficForProfile(sb.Profile, trafficSpec.Traffic); err != nil {
		return "", fmt.Errorf("mint migration token: portable traffic metadata: %w", err)
	}
	return migrationtoken.Seal(
		migrationtoken.KeyMaterial{APISecret: sb.APISecret, ManifestKey: sb.ManifestKey},
		migrationtoken.MigrationTokenPayloadV1{
			Version:                1,
			NodeSandboxID:          sb.ID,
			StableID:               sb.StableID(),
			APISecretFingerprint:   apiFingerprint,
			ManifestKeyFingerprint: manifestFingerprint,
			TemplateID:             sb.TemplateID,
			Profile:                string(sb.Profile),
			RuntimeDigest:          dig,
			ResumeSourceKind:       source.Kind,
			ResumeSourceRef:        source.Ref,
			ResumeSandboxRef:       source.SandboxRef,
			Env:                    sb.Env,
			Metadata:               metadata,
			CreatedUnix:            sb.CreatedUnix,
			DeadlineUnix:           sb.DeadlineUnix,
			AutoPauseMemory:        sb.AutoPauseMemory,
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
// cluster commands can constrain the token StableID/profile/runtime and attach
// system-owned Group/RouteKey state. A positive deadlineOverride is written as
// part of the insert; zero preserves the token deadline. The token never
// supplies trusted cluster context.
func (o *Orchestrator) importSandboxWithKey(
	ctx context.Context,
	pair store.KeyPair,
	token, targetID string,
	expected migrationtoken.Expectations,
	cluster *types.ClusterSandboxContext,
	deadlineOverride int64,
) (*types.Sandbox, error) {
	return o.importSandboxWithKeyOptions(ctx, pair, token, targetID, expected, cluster, deadlineOverride, standaloneMMDSImport{})
}

type standaloneMMDSImport struct {
	routesPresent bool
	routesJSON    string
	routesDigest  string
	secretValues  store.MMDSRouteSecretValues
}

func (o *Orchestrator) importSandboxWithKeyOptions(
	ctx context.Context,
	pair store.KeyPair,
	token, targetID string,
	expected migrationtoken.Expectations,
	cluster *types.ClusterSandboxContext,
	deadlineOverride int64,
	mmdsImport standaloneMMDSImport,
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
	// The token StableID later keys the entity's publication location names
	// (reflocation.PublicationName), so admission enforces the same opaque-id
	// contract local IDs satisfy; a malformed identity would otherwise surface
	// only as an export-time Resolve failure.
	if !types.ValidLocalSandboxID(payload.StableID) {
		return nil, fmt.Errorf("import-sandbox: invalid stable ID: %w", api.ErrBadRequest)
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
	metadata, err := migrationSandboxMetadata(payload.Metadata)
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: token resource metadata: %w: %v", migrationtoken.ErrInvalidPayload, err)
	}
	trafficSpec, err := sandboxcfg.ParseSpec(metadata)
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: token traffic metadata: %w: %v", migrationtoken.ErrInvalidPayload, err)
	}
	if err := sandboxcfg.ValidateTrafficForProfile(profile, trafficSpec.Traffic); err != nil {
		return nil, fmt.Errorf("import-sandbox: token traffic metadata: %w: %v", migrationtoken.ErrInvalidPayload, err)
	}
	if raw, present := metadata[sandboxcfg.NsMMDS]; present {
		_, routesJSON, err := sandboxcfg.ValidatePersistedMMDSRoutes(raw, o.mmdsPolicy())
		if err != nil {
			return nil, fmt.Errorf("import-sandbox: token MMDS routes: %w: %v", migrationtoken.ErrInvalidPayload, err)
		}
		// Routes always come from the authenticated migration token. The
		// standalone CONNECT path may add initial values, but it cannot replace
		// this portable declaration or its digest.
		metadata[sandboxcfg.NsMMDS] = routesJSON
		mmdsImport.routesPresent = true
		mmdsImport.routesJSON = routesJSON
		mmdsImport.routesDigest = sandboxcfg.MMDSRoutesDigest(routesJSON)
	} else if mmdsImport.routesPresent || len(mmdsImport.secretValues) != 0 {
		return nil, fmt.Errorf("import-sandbox: request MMDS secrets without token routes: %w", migrationtoken.ErrInvalidPayload)
	}
	if cluster != nil {
		trustedCluster = &types.ClusterSandboxContext{Group: cluster.Group, RouteKey: cluster.RouteKey}
		metadata = clusterSandboxMetadata(metadata)
	}
	sb := &types.Sandbox{
		ID:            targetID,
		Profile:       profile,
		Cluster:       trustedCluster,
		StableIDValue: payload.StableID,
		TemplateID:    payload.TemplateID,
		State:         types.StatePaused,
		DeadlineUnix:  payload.DeadlineUnix,
		CreatedUnix:   payload.CreatedUnix,
		RunDir:        nodepath.SandboxRunDir(o.cfg.Paths.RunRoot, targetID),
		BaseDir:       nodepath.SandboxBaseDir(o.cfg.Paths.BaseRoot, targetID),
		APISecret:     pair.APISecret,
		ManifestKey:   pair.ManifestKey,
		ResumeSource: types.ResumeSource{
			Kind:       payload.ResumeSourceKind,
			Ref:        payload.ResumeSourceRef,
			SandboxRef: payload.ResumeSandboxRef,
		},
		AutoPauseMemory:    payload.AutoPauseMemory,
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
	if mmdsImport.routesPresent {
		initial := &mmdsInitialRouteSecretValues{
			routesDigest: mmdsImport.routesDigest,
			values:       mmdsImport.secretValues,
		}
		if err := validateInitialMMDSRouteEntry(sb, initial); err != nil {
			return nil, fmt.Errorf("import-sandbox: MMDS route projection: %w: %v", migrationtoken.ErrInvalidPayload, err)
		}
	}
	unlockEvent := o.lockExtensionSandboxEvent(sb.ID)
	defer unlockEventFence(unlockEvent)
	var insertErr error
	if mmdsImport.routesPresent {
		insertErr = o.st.InsertSandboxWithMMDSRouteSecretValues(ctx, sb, mmdsImport.routesDigest, mmdsImport.secretValues)
	} else {
		insertErr = o.st.InsertSandbox(ctx, sb)
	}
	if insertErr != nil {
		if errors.Is(insertErr, store.ErrSandboxExists) {
			return nil, fmt.Errorf("import-sandbox: %w", api.ErrAlreadyExists)
		}
		return nil, fmt.Errorf("import-sandbox: insert target %s: %w", targetID, insertErr)
	}
	o.observeSandboxUpsert(sb)
	return sb, nil
}

func (o *Orchestrator) prepareStandaloneTargetWithMMDS(ctx context.Context, id, apiKey, token string, requestMetadata map[string]string, header *string) (*types.Sandbox, error) {
	pair, err := o.resolveAllowed(ctx, apiKey)
	if err != nil {
		return nil, err
	}
	if pair.APISecret == "" {
		return nil, fmt.Errorf("import-sandbox: credential pair is not installed: %w", api.ErrNotAllowed)
	}
	payload, err := migrationtoken.Open(
		migrationtoken.KeyMaterial{APISecret: pair.APISecret, ManifestKey: pair.ManifestKey},
		token,
	)
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: open migration token: %w", err)
	}

	var tokenRoutes []sandboxcfg.MMDSRoute
	var routesJSON string
	routesPresent := false
	if raw, ok := payload.Metadata[sandboxcfg.NsMMDS]; ok {
		tokenRoutes, routesJSON, err = sandboxcfg.ValidatePersistedMMDSRoutes(raw, o.mmdsPolicy())
		if err != nil {
			return nil, fmt.Errorf("import-sandbox: token MMDS routes: %w: %v", migrationtoken.ErrInvalidPayload, err)
		}
		routesPresent = true
	}
	requestMMDS := map[string]string(nil)
	if raw, ok := requestMetadata[sandboxcfg.NsMMDS]; ok {
		requestMMDS = map[string]string{sandboxcfg.NsMMDS: raw}
	}
	values, err := sandboxcfg.ExtractMMDSImportSecrets(requestMMDS, header, tokenRoutes, o.mmdsPolicy())
	if err != nil {
		return nil, fmt.Errorf("import-sandbox: request MMDS secrets: %w: %v", api.ErrBadRequest, err)
	}

	imported, err := o.importSandboxWithKeyOptions(
		ctx, pair, token, id, migrationtoken.Expectations{}, nil, 0,
		standaloneMMDSImport{
			routesPresent: routesPresent,
			routesJSON:    routesJSON,
			routesDigest:  sandboxcfg.MMDSRoutesDigest(routesJSON),
			secretValues:  store.MMDSRouteSecretValues(values),
		},
	)
	if err != nil {
		if !errors.Is(err, api.ErrAlreadyExists) {
			return nil, err
		}
		// A concurrent import won. Its row and secret blob are authoritative;
		// ignore this request's token and values from this point onward.
		imported, err = o.st.Get(ctx, id)
		if err != nil {
			return nil, err
		}
	}
	if !ownsSandbox(imported, apiKey) {
		return nil, api.ErrNotFound
	}
	return imported, nil
}

// migrationSandboxMetadata preserves ordinary portable metadata while ensuring
// the request-only credentials carrier can never re-enter metadata_json. The
// service credentials restored from KMT1 come only from their typed payload
// fields and are validated separately.
func migrationSandboxMetadata(metadata map[string]string) (map[string]string, error) {
	if metadata == nil {
		return nil, nil
	}
	cleaned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		if key != sandboxcfg.NsCredentials {
			cleaned[key] = value
		}
	}
	cleaned, err := sandboxcfg.NormalizeResourceMetadata(cleaned)
	if err != nil {
		return nil, err
	}
	return sandboxcfg.NormalizeTrafficMetadata(cleaned)
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

func artifactRole(kind types.ResumeSourceKind) artifactresult.LogicalRole {
	if kind == types.ResumeSourceSnapshot {
		return artifactresult.RoleSnapshot
	}
	return artifactresult.RoleSandbox
}

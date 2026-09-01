package orch

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	rtutil "github.com/kuasar-sandbox/sandboxer/pkg/util"
)

const buildRuntimePrepareSchemaVersion = 1

// buildRuntimePreparation is the non-secret durable authority committed in the
// same SQLite UPDATE as the exact connector port. It freezes every node-policy
// result needed to rebuild an equivalent final BuildSpec after restart.
type buildRuntimePreparation struct {
	SchemaVersion   int                      `json:"schema_version"`
	PrepareDigest   string                   `json:"prepare_digest"`
	Network         sandboxcfg.NetworkSpec   `json:"network"`
	TemplateNetwork sandboxcfg.NetworkSpec   `json:"template_network"`
	Resources       rtconfig.ResourcesConfig `json:"resources"`
}

func encodeBuildRuntimePreparation(prep buildRuntimePreparation) (string, error) {
	if err := validateBuildRuntimePreparation(prep); err != nil {
		return "", err
	}
	body, err := json.Marshal(prep)
	if err != nil {
		return "", fmt.Errorf("build: encode runtime preparation: %w", err)
	}
	return string(body), nil
}

func decodeBuildRuntimePreparation(raw string) (buildRuntimePreparation, error) {
	if raw == "" {
		return buildRuntimePreparation{}, errors.New("build: durable runtime preparation is missing")
	}
	var prep buildRuntimePreparation
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&prep); err != nil {
		return buildRuntimePreparation{}, fmt.Errorf("build: decode durable runtime preparation: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return buildRuntimePreparation{}, errors.New("build: durable runtime preparation has trailing data")
	}
	if err := validateBuildRuntimePreparation(prep); err != nil {
		return buildRuntimePreparation{}, err
	}
	return prep, nil
}

func validateBuildRuntimePreparation(prep buildRuntimePreparation) error {
	if prep.SchemaVersion != buildRuntimePrepareSchemaVersion {
		return fmt.Errorf("build: unsupported durable runtime preparation schema %d", prep.SchemaVersion)
	}
	digest, err := hex.DecodeString(prep.PrepareDigest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != prep.PrepareDigest {
		return errors.New("build: durable prepare digest is not SHA-256")
	}
	if prep.Network.InnerIP == "" || prep.TemplateNetwork.InnerIP == "" {
		return errors.New("build: durable runtime preparation has unresolved network")
	}
	if prep.Resources.Capacity.CPU <= 0 {
		return errors.New("build: durable runtime preparation has invalid resources capacity CPU")
	}
	memory, err := rtutil.ParseSize(strings.TrimSpace(prep.Resources.Capacity.Memory))
	if err != nil || memory == 0 {
		return errors.New("build: durable runtime preparation has invalid resources capacity memory")
	}
	return nil
}

func fastBuildPrepareDigest(buildID string) string {
	digest := sha256.Sum256([]byte("build-fast-v1\x00" + buildID))
	return hex.EncodeToString(digest[:])
}

func validateBuildPrepareSummary(summary configsock.ArtifactPrepareSummary) (sandboxcfg.NetworkSpec, error) {
	if summary.SchemaVersion != configsock.ArtifactPrepareSchemaVersion {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: unsupported artifact prepare schema %d", summary.SchemaVersion)
	}
	if summary.RequiredRefCount < 1 || summary.RequiredRefCount > maxRequiredArtifactRefs {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: snapshot required ref count %d outside 1..%d", summary.RequiredRefCount, maxRequiredArtifactRefs)
	}
	if types.ResumeSourceKind(summary.PreparedSourceKind) != types.ResumeSourceSnapshot {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: prepared source kind %q is not Snapshot", summary.PreparedSourceKind)
	}
	digest, err := hex.DecodeString(summary.ResolutionDigest)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != summary.ResolutionDigest {
		return sandboxcfg.NetworkSpec{}, errors.New("build: snapshot resolution digest is not SHA-256")
	}
	memory := strings.TrimSpace(summary.Capacity.Memory)
	if summary.Capacity.CPU <= 0 || memory == "" {
		return sandboxcfg.NetworkSpec{}, errors.New("build: snapshot config has no usable resources.capacity")
	}
	bytes, err := rtutil.ParseSize(memory)
	if err != nil || bytes == 0 {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: snapshot config has invalid resources.capacity.memory %q", summary.Capacity.Memory)
	}
	inherited := artifactNetworkSpec(summary.Network)
	if err := sandboxcfg.ValidateNetworkSpec(inherited); err != nil {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: source-template network summary: %w", err)
	}
	if err := validateArtifactDiskTopology(summary.DiskTopology); err != nil {
		return sandboxcfg.NetworkSpec{}, fmt.Errorf("build: source-template disk summary: %w", err)
	}
	return inherited, nil
}

// buildTaskHandoff owns one exact run's immutable prepare input and final
// result. HTTP cancellation only stops that request's wait; it never retracts
// an accepted summary.
type buildTaskHandoff struct {
	mu sync.Mutex

	snapshot       bool
	expectedDigest string
	summary        *configsock.ArtifactPrepareSummary
	prepareReady   chan struct{}

	final      *configsock.BuildSpec
	finalErr   error
	finalReady chan struct{}

	conflict      error
	conflictReady chan struct{}
}

func newBuildTaskHandoff(snapshot bool, expectedDigest string) *buildTaskHandoff {
	return &buildTaskHandoff{
		snapshot: snapshot, expectedDigest: expectedDigest,
		prepareReady: make(chan struct{}), finalReady: make(chan struct{}), conflictReady: make(chan struct{}),
	}
}

func (h *buildTaskHandoff) Submit(summary configsock.ArtifactPrepareSummary) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.snapshot {
		return false, errors.New("build: snapshot preparation is not required")
	}
	if h.conflict != nil {
		return false, h.conflict
	}
	if h.expectedDigest != "" {
		if summary.ResolutionDigest == h.expectedDigest {
			return true, nil
		}
		return false, h.setConflictLocked(errors.New("build: artifact prepare conflicts with durable preparation"))
	}
	if h.summary == nil {
		copySummary := configsock.CloneArtifactPrepareSummary(summary)
		h.summary = &copySummary
		close(h.prepareReady)
		return false, nil
	}
	if configsock.EqualArtifactPrepareSummary(*h.summary, summary) {
		return true, nil
	}
	return false, h.setConflictLocked(errors.New("build: conflicting artifact prepare replay"))
}

func (h *buildTaskHandoff) setConflictLocked(err error) error {
	if h.conflict == nil {
		h.conflict = err
		close(h.conflictReady)
	}
	return h.conflict
}

func (h *buildTaskHandoff) WaitPrepare(ctx context.Context) (configsock.ArtifactPrepareSummary, error) {
	select {
	case <-h.prepareReady:
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.conflict != nil {
			return configsock.ArtifactPrepareSummary{}, h.conflict
		}
		if h.summary == nil {
			return configsock.ArtifactPrepareSummary{}, errors.New("build: prepare completed without summary")
		}
		return configsock.CloneArtifactPrepareSummary(*h.summary), nil
	case <-h.conflictReady:
		h.mu.Lock()
		defer h.mu.Unlock()
		return configsock.ArtifactPrepareSummary{}, h.conflict
	case <-ctx.Done():
		return configsock.ArtifactPrepareSummary{}, ctx.Err()
	}
}

func (h *buildTaskHandoff) PublishFinal(spec *configsock.BuildSpec, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	select {
	case <-h.finalReady:
		return
	default:
	}
	if h.conflict != nil {
		h.final, h.finalErr = nil, h.conflict
	} else {
		h.final, h.finalErr = spec, err
	}
	close(h.finalReady)
}

func (h *buildTaskHandoff) WaitFinal(ctx context.Context) (*configsock.BuildSpec, error) {
	select {
	case <-h.finalReady:
		h.mu.Lock()
		defer h.mu.Unlock()
		if h.conflict != nil {
			return nil, h.conflict
		}
		return h.final, h.finalErr
	case <-h.conflictReady:
		h.mu.Lock()
		defer h.mu.Unlock()
		return nil, h.conflict
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (h *buildTaskHandoff) Conflict() <-chan struct{} { return h.conflictReady }

func (h *buildTaskHandoff) ConflictErr() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conflict
}

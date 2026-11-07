package taskartifact

import (
	"context"
	"errors"
	"fmt"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
)

var ErrSnapshotPairMismatch = errors.New("Snapshot E differs from the authenticated pair")

// CheckSnapshotRuntime reads only the selected S and its paired E in a tenant
// process. It does not walk parent artifacts, inspect disks, or prepare a launch.
// MANIFEST_KEY must already be installed before calling it, just as for Prepare.
func CheckSnapshotRuntime(ctx context.Context, spec configsock.SnapshotRuntimeCheckRequest) error {
	if spec.Source.Kind != types.ResumeSourceSnapshot || !spec.Source.Valid() {
		return errors.New("snapshot runtime admission requires an exact S/E pair")
	}
	if err := validateRootResolution(spec.Source.Ref, spec.RelativeDir); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	manifestCfg, err := rtconfig.LoadManifestConfig(spec.ManifestConfig)
	if err != nil {
		if !errors.Is(err, manifest.ErrConfigNotProvided) {
			return fmt.Errorf("snapshot runtime admission: manifest config: %w", err)
		}
		manifestCfg = nil
	}
	locations := rtconfig.RefLocations{}
	for _, raw := range []string{spec.Source.Ref, spec.Source.SandboxRef} {
		name, err := refLocationName(raw)
		if err != nil {
			return fmt.Errorf("snapshot runtime admission: source ref: %w", err)
		}
		if name == "" {
			continue
		}
		location, err := reflocation.Resolve(spec.RefLocationParent, name)
		if err != nil {
			return fmt.Errorf("snapshot runtime admission: source location: %w", err)
		}
		locations[name] = location.Path
	}
	reader, err := restore.NewSnapshotCfgReader(manifestCfg)
	if err != nil {
		return fmt.Errorf("snapshot runtime admission: initialize reader: %w", err)
	}
	document, readErr := reader.Read(ctx, spec.Source.Ref, restore.SnapshotCfgReadOptions{
		RefLocations: locations, RelativeDir: spec.RelativeDir,
	})
	closeErr := reader.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		return fmt.Errorf("snapshot runtime admission: read S/E: %w", err)
	}
	paired, err := pairedSandboxIdentity(spec.Source.Ref, document.SandboxRef, spec.RelativeDir, locations)
	if err != nil {
		return err
	}
	if paired != spec.Source.SandboxRef {
		return ErrSnapshotPairMismatch
	}
	if _, err := restore.ResolveRuntimeBinding(ctx, spec.Runtime, document.Config.Boot.RuntimeRef); err != nil {
		return fmt.Errorf("snapshot runtime admission: %w", err)
	}
	return nil
}

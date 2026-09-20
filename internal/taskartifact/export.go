package taskartifact

import (
	"context"
	"errors"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// SnapshotSandboxRef performs the root-metadata-only read needed when export
// starts from an already portable S. It shares task preparation's carrier and
// trusted location resolution without preparing or scanning the launch graph.
func SnapshotSandboxRef(ctx context.Context, source types.ResumeSource, configPath, customerKey, locationParent string) (string, error) {
	cfg, err := rtconfig.LoadManifestConfig(configPath)
	if err != nil && !errors.Is(err, manifest.ErrConfigNotProvided) {
		return "", err
	}
	key, err := manifest.ParseHexKey(customerKey)
	if err != nil {
		return "", err
	}
	defer clear(key[:])
	storage, err := artifact.NewProcessStorageWithCustomerKey(cfg, func() ([32]byte, error) { return [32]byte(key), nil })
	if err != nil {
		return "", err
	}
	locations := rtconfig.RefLocations{}
	addLocation := func(raw string) error {
		name, err := refLocationName(raw)
		if err != nil {
			return err
		}
		if name == "" {
			return nil
		}
		location, err := reflocation.Resolve(locationParent, name)
		if err != nil {
			return err
		}
		locations[name] = location.Path
		return nil
	}
	if err := addLocation(source.Ref); err != nil {
		return "", errors.Join(err, storage.Close())
	}
	refs, _, err := rootCarrier(source.Ref, "", locations)
	if err != nil {
		return "", errors.Join(err, storage.Close())
	}
	for _, raw := range refs {
		if err := addLocation(raw); err != nil {
			return "", errors.Join(err, storage.Close())
		}
	}
	ref, readErr := storage.SnapshotSandboxRef(ctx, source.Ref, locations)
	if err := errors.Join(readErr, storage.Close()); err != nil {
		return "", err
	}
	return ref, nil
}

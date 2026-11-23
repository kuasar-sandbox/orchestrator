package taskartifact

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	rtsandbox "github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// ImageEnvironment stays in the tenant task. Its digest is diagnostic identity;
// Env never returns through the conductor protocol or logs.
type ImageEnvironment struct {
	ImageRef     string
	ConfigSHA256 string
	Env          map[string]string
}

func LoadImageEnvironment(ctx context.Context, spec configsock.ImageLaunchProjection) (*ImageEnvironment, error) {
	if _, err := types.ParsePortableRef(spec.ImageRef); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cfg, err := rtconfig.LoadManifestConfig(spec.ManifestConfig)
	if err != nil && !errors.Is(err, manifest.ErrConfigNotProvided) {
		return nil, err
	}
	storage, err := artifact.NewProcessStorage(cfg)
	if err != nil {
		return nil, err
	}
	defer storage.Close()
	locations := rtconfig.RefLocations{}
	names := make([]string, 0, len(spec.RefLocations))
	for name := range spec.RefLocations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := locations.Set(name + "=" + spec.RefLocations[name]); err != nil {
			return nil, err
		}
	}
	image, err := rtsandbox.OpenFlattenedImage(ctx, spec.ImageRef, storage, locations)
	if err != nil {
		return nil, err
	}
	defer image.Close()
	env, err := ParseImageEnvironment(image.ImageConfig)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &ImageEnvironment{ImageRef: spec.ImageRef, ConfigSHA256: fmt.Sprintf("%x", sha256.Sum256(image.ImageConfig)), Env: env}, nil
}

// ParseImageEnvironment follows the Host's image-default parsing: the last
// occurrence of a named key wins, empty values are retained, malformed entries
// are ignored. It reads the strict flattened image's existing runtime config.
func ParseImageEnvironment(raw []byte) (map[string]string, error) {
	image, err := rtsandbox.LoadImageConfigBytes(raw)
	if err != nil {
		return nil, err
	}
	env := make(map[string]string, len(image.Env))
	for _, entry := range image.Env {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			env[key] = value
		}
	}
	return env, nil
}

package workspacedisk

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/diffstore"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
	"github.com/kuasar-sandbox/sandboxer/pkg/snapshot"
)

// FreezeSeed fixes the cold disk's capacity and turns its optional provisioning
// template into an immutable upper layer next to the new instance E. A sparse
// template can override selected blocks and still inherit data from lower layers.
func FreezeSeed(ctx context.Context, disk rtconfig.DiskV1, storage *artifact.ProcessStorage, locations rtconfig.RefLocations, directory string) (_ rtconfig.DiskV1, retErr error) {
	var options []diffstore.DiskOption
	if storage.LocalCodec() != nil {
		if storage.CustomerKeyFunc() == nil {
			return rtconfig.DiskV1{}, errors.New("workspace disk requires its tenant key")
		}
		key, err := storage.CustomerKeyFunc()()
		if err != nil {
			return rtconfig.DiskV1{}, err
		}
		options = append(options, diffstore.WithDiffEncryption(key, storage.LocalRequired()))
		clear(key[:])
	}
	base, size, seed, err := sandbox.OpenDiskV1Source(ctx, disk, storage, locations, options...)
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	if base != nil {
		defer func() { retErr = errors.Join(retErr, base.Close()) }()
	}
	disk.Size = &size
	if seed == "" {
		return disk, nil
	}
	if err = os.MkdirAll(directory, 0700); err != nil {
		return rtconfig.DiskV1{}, err
	}
	temp, err := os.MkdirTemp(directory, ".seed-*")
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	defer func() { retErr = errors.Join(retErr, os.RemoveAll(temp)) }()
	path := filepath.Join(temp, "writer")
	init, err := sandbox.PrepareDiff(path, seed, size)
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	writer, err := diffstore.OpenDisk(path, base, init, options...)
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	defer func() { retErr = errors.Join(retErr, writer.Close()) }()
	view, err := writer.OpenActiveView(ctx)
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	// At the native layer limit collapse the oldest two immutable layers
	// first, preserving the template's own sparse upper and all logical bytes.
	if len(disk.BaseFromRefs) == rtconfig.MaxPortableLayerRefs {
		n := len(disk.BaseFromRefs)
		older, err := collapseParents(ctx, disk.BaseFromRefs[n-2:], storage, locations, directory)
		if err != nil {
			return rtconfig.DiskV1{}, errors.Join(err, view.Close())
		}
		disk.BaseFromRefs = append(append([]string(nil), disk.BaseFromRefs[:n-2]...), older)
	}
	defer func() { retErr = errors.Join(retErr, view.Close()) }()
	sink := snapshot.NewFileSink(directory, "seed", storage.LocalCodec(), storage.LocalRequired(), nil)
	defer func() { retErr = errors.Join(retErr, sink.Close()) }()
	raw, name, err := sink.AbsorbOverlaySource(ctx, view)
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return rtconfig.DiskV1{}, err
	}
	ref.Path = name
	if disk.Base != "" {
		disk.BaseFromRefs = append([]string{disk.Base}, disk.BaseFromRefs...)
	}
	disk.Base, disk.DiffTemplate = ref.String(), ""
	return disk, disk.Validate()
}

// Join two immutable layers only when adding the seed would exceed the native graph limit.
func collapseParents(ctx context.Context, refs []string, storage *artifact.ProcessStorage, loc rtconfig.RefLocations, directory string) (_ string, retErr error) {
	var layers []fetch.Stream
	for _, raw := range refs {
		ref, err := manifest.ParseRef(raw)
		if err != nil {
			return "", err
		}
		layer, err := OpenLayer(ctx, storage, ref, "", loc)
		if err != nil {
			return "", err
		}
		defer func() { retErr = errors.Join(retErr, layer.Close()) }()
		layers = append(layers, layer)
	}
	sink := snapshot.NewFileSink(directory, "parent", storage.LocalCodec(), storage.LocalRequired(), nil)
	defer func() { retErr = errors.Join(retErr, sink.Close()) }()
	raw, path, err := sink.AbsorbOverlaySource(ctx, fetch.NewLayered(layers...))
	if err != nil {
		return "", err
	}
	ref, err := manifest.ParseRef(raw)
	if err != nil {
		return "", err
	}
	ref.Path = path
	return ref.String(), nil
}

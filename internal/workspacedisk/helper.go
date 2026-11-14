package workspacedisk

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"slices"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// Request is a private, bounded conductor-to-child request. No tenant key is
// serialized; only this short-lived process receives the selected manifest key.
type Request struct {
	Operation                            string
	Source, SourceDir, Name, Publication string
	ManifestConfig, RefLocationParent    string
	Disk                                 *rtconfig.DiskV1      `json:",omitempty"`
	Locations                            rtconfig.RefLocations `json:",omitempty"`
}
type Result struct {
	Error   string            `json:",omitempty"`
	Entries []workspace.Entry `json:",omitempty"`
	States  []ctl.DiskState   `json:",omitempty"`
	Disk    *rtconfig.DiskV1  `json:",omitempty"`
}

func Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(in, rtconfig.MaxPortableConfigBytes+1))
	if err != nil {
		return err
	}
	var req Request
	if len(raw) > rtconfig.MaxPortableConfigBytes || strictjson.Decode(raw, &req) != nil {
		return errors.New("invalid workspace operation")
	}
	result, err := execute(ctx, req, os.Getenv(manifest.CustomerKeyEnv))
	if err != nil {
		result = Result{Error: "storage"}
		switch {
		case errors.Is(err, ErrUnauthorized):
			result.Error = "permission"
		case errors.Is(err, artifact.ErrDiskNotFound):
			result.Error = "not_found"
		case errors.Is(err, artifact.ErrDiskUnbound), errors.Is(err, artifact.ErrDiskLayout):
			result.Error = "conflict"
		}
	}
	return json.NewEncoder(out).Encode(result)
}

func execute(ctx context.Context, req Request, key string) (out Result, retErr error) {
	if req.Operation == "authorize" {
		if req.Disk == nil {
			return out, ErrSelection
		}
		return out, AuthorizeLayers(ctx, *req.Disk, req.Locations, Options{ManifestConfig: req.ManifestConfig, ManifestKey: key})
	}
	if req.Operation != "list" && req.Operation != "export" {
		return out, ErrSelection
	}
	a, err := openArchive(ctx, req, key)
	if err != nil {
		return out, err
	}
	defer func() { retErr = errors.Join(retErr, a.close()) }()
	entries, err := workspace.Captured(a.config)
	if err != nil {
		return out, err
	}
	if req.Operation == "list" {
		out.Entries = entries
		out.States, err = a.inventory(ctx, entries)
		return out, err
	}
	if !slices.ContainsFunc(entries, func(e workspace.Entry) bool { return e.Name == req.Name }) {
		return out, artifact.ErrDiskNotFound
	}
	out.Disk, err = a.publish(ctx, req, req.Name)
	return out, err
}

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
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// MaxExportResultBytes bounds the private export response, including repeated
// carrier names in removedRefs. A saved disk has at most 65 explicit old layers;
// each chosen Bundle carrier is bounded by its existing 1 MiB refs metadata.
// A selector adds fewer than 128 bytes, and JSON escaping expands at most 6x.
// The final graph is present in both Disk and Report and already obeys the
// Workspace portable-ref size limit. The parent allocates only bytes received.
// List and authorize keep their original PortableConfig-sized response budget.
const MaxExportResultBytes = (rtconfig.MaxPortableLayerRefs+1)*(6*(rtconfig.MaxPortableConfigBytes+128)+3) +
	2*(rtconfig.MaxPortableLayerRefs+1)*(6*types.MaxPortableRefBytes+3) + 4<<10

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
	Error   string               `json:",omitempty"`
	Entries []workspace.Entry    `json:",omitempty"`
	States  []ctl.DiskState      `json:",omitempty"`
	Disk    *rtconfig.DiskV1     `json:",omitempty"`
	Report  *artifact.DiskReport `json:",omitempty"`
}

func (r *Result) UnmarshalJSON(raw []byte) error {
	var wire struct {
		Error   string
		Entries []workspace.Entry
		States  []ctl.DiskState
		Disk    *rtconfig.DiskV1
		Report  json.RawMessage
	}
	if err := strictjson.Decode(raw, &wire); err != nil {
		return err
	}
	out := Result{Error: wire.Error, Entries: wire.Entries, States: wire.States, Disk: wire.Disk}
	if wire.Report != nil {
		report, err := artifact.DecodeDiskReport(wire.Report)
		if err != nil {
			return err
		}
		out.Report = &report
	}
	*r = out
	return nil
}

// ValidateExport checks the private child's report against the very DiskV1
// that will be authenticated by the node. No artifact is reopened here.
func (r Result) ValidateExport() error {
	if r.Disk == nil || r.Report == nil || !r.Report.Refs {
		return errors.New("workspace export: complete disk and reference report required")
	}
	if err := r.Report.Validate(); err != nil {
		return err
	}
	refs := append([]string{r.Disk.Base}, r.Disk.BaseFromRefs...)
	slices.Sort(refs)
	refs = slices.Compact(refs)
	if !slices.Equal(refs, r.Report.DiskRefs) {
		return errors.New("workspace export: report does not match disk")
	}
	return nil
}

func Serve(ctx context.Context, in io.Reader, out io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(in, rtconfig.MaxPortableConfigBytes+1))
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var req Request
	if len(raw) > rtconfig.MaxPortableConfigBytes || strictjson.Decode(raw, &req) != nil {
		return errors.New("invalid workspace operation")
	}
	result, err := execute(ctx, req, os.Getenv(manifest.CustomerKeyEnv))
	if ctx.Err() != nil {
		// execute has unwound its owned resources. Keep cancellation and any
		// cleanup diagnostic visible to the parent, rather than report success.
		// Cancellation takes precedence even if commit just completed; committed
		// content-addressed artifacts remain available and are not rolled back.
		return errors.Join(ctx.Err(), err)
	}
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
	entries := workspace.Captured(a.config)
	if req.Operation == "list" {
		out.Entries = entries
		out.States, err = a.inventory(ctx, entries)
		return out, err
	}
	if !slices.ContainsFunc(entries, func(e workspace.Entry) bool { return e.Name == req.Name }) {
		return out, artifact.ErrDiskNotFound
	}
	published, err := a.publish(ctx, req, req.Name)
	if err != nil {
		return out, err
	}
	report, err := published.Report()
	if err != nil {
		return out, err
	}
	out.Disk, out.Report = &published.Disk, &report
	return out, out.ValidateExport()
}

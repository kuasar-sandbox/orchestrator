package registry

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

const (
	RouteLinkExportPath = "/route-link/export"
	RouteLinkImportPath = "/route-link/import"

	SnapshotKindRoute = "route"
	SnapshotKindBuild = "build"
)

// SnapshotRecord is one JSONL row for registry operator import/export. It is a
// management snapshot, not a replication log: route imports get fresh target-side
// revisions/ballots and node liveness must still be re-confirmed by node-link.
type SnapshotRecord struct {
	Type  string         `json:"type"`
	Route *SandboxRecord `json:"route,omitempty"`
	Build *BuildRecord   `json:"build,omitempty"`
}

type SnapshotOptions struct {
	Kind  string
	Group string
}

type SnapshotSummary struct {
	Routes int `json:"routes"`
	Builds int `json:"builds"`
}

func (o SnapshotOptions) normalized() (SnapshotOptions, error) {
	if o.Kind == "" {
		o.Kind = "route_link"
	}
	switch o.Kind {
	case "route_link":
	default:
		return SnapshotOptions{}, fmt.Errorf("registry snapshot: invalid kind %q", o.Kind)
	}
	if o.Group == "" {
		return SnapshotOptions{}, fmt.Errorf("registry snapshot: group is required")
	}
	return o, nil
}

func (r *Registry) ExportSnapshot(ctx context.Context, w io.Writer, opts SnapshotOptions) (SnapshotSummary, error) {
	opts, err := opts.normalized()
	if err != nil {
		return SnapshotSummary{}, err
	}
	enc := json.NewEncoder(w)
	var sum SnapshotSummary
	if err := r.stores.RangeSandboxes(ctx, opts.Group, func(s *SandboxRecord) error {
		out := *s
		if err := enc.Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: &out}); err != nil {
			return err
		}
		sum.Routes++
		return nil
	}); err != nil {
		return SnapshotSummary{}, err
	}
	if err := r.stores.RangeBuildsInGroup(ctx, opts.Group, func(b *BuildRecord) error {
		out := *b
		if err := enc.Encode(SnapshotRecord{Type: SnapshotKindBuild, Build: &out}); err != nil {
			return err
		}
		sum.Builds++
		return nil
	}); err != nil {
		return SnapshotSummary{}, err
	}
	return sum, nil
}

func (r *Registry) ImportSnapshot(ctx context.Context, rd io.Reader) (SnapshotSummary, error) {
	sc := bufio.NewScanner(rd)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var sum SnapshotSummary
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var rec SnapshotRecord
		if err := json.Unmarshal([]byte(raw), &rec); err != nil {
			return sum, fmt.Errorf("registry snapshot line %d: %w", line, err)
		}
		switch rec.Type {
		case SnapshotKindRoute:
			if rec.Route == nil || rec.Route.Group == "" || rec.Route.RouteKey == "" {
				return sum, fmt.Errorf("registry snapshot line %d: route record missing group or route_key", line)
			}
			route := *rec.Route
			if _, err := r.stores.PutSandbox(ctx, &route); err != nil {
				return sum, fmt.Errorf("registry snapshot line %d: put route %q/%q: %w", line, route.Group, route.RouteKey, err)
			}
			if route.SID != "" && route.NodeID != "" {
				if err := r.stores.AddNodeSandboxRef(ctx, route.NodeID, clusterstate.NodeSandboxRef{
					SandboxID: route.SID, Group: route.Group, RouteKey: route.RouteKey,
				}); err != nil {
					_ = r.stores.DeleteSandbox(ctx, route.Group, route.RouteKey)
					return sum, fmt.Errorf("registry snapshot line %d: put node sandbox %q: %w", line, route.SID, err)
				}
			}
			sum.Routes++
		case SnapshotKindBuild:
			if rec.Build == nil || rec.Build.Group == "" || rec.Build.BuildID == "" {
				return sum, fmt.Errorf("registry snapshot line %d: build record missing group or build_id", line)
			}
			build := *rec.Build
			if err := r.stores.PutBuild(ctx, &build); err != nil {
				return sum, fmt.Errorf("registry snapshot line %d: put build %q/%q: %w", line, build.Group, build.BuildID, err)
			}
			if build.NodeID != "" {
				if err := r.stores.AddNodeBuildRef(ctx, build.NodeID, clusterstate.NodeBuildRef{BuildID: build.BuildID, Group: build.Group}); err != nil {
					_ = r.stores.DeleteBuild(ctx, build.Group, build.BuildID)
					return sum, fmt.Errorf("registry snapshot line %d: put node build %q: %w", line, build.BuildID, err)
				}
			}
			sum.Builds++
		default:
			return sum, fmt.Errorf("registry snapshot line %d: unknown type %q", line, rec.Type)
		}
	}
	if err := sc.Err(); err != nil {
		return sum, err
	}
	return sum, nil
}

func (r *Registry) serveExport(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	w.Header().Set("Content-Type", "application/x-ndjson")
	if _, err := r.ExportSnapshot(req.Context(), w, SnapshotOptions{
		Kind:  q.Get("kind"),
		Group: q.Get("group"),
	}); err != nil {
		http.Error(w, err.Error(), routeLinkStatus(err))
		return
	}
}

func (r *Registry) serveImport(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sum, err := r.ImportSnapshot(req.Context(), req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, sum)
}

package registry

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	RouteLinkExportPath = "/route-link/export"
	RouteLinkImportPath = "/route-link/import"

	SnapshotKindRoute = "route"
)

// SnapshotRecord is one JSONL row for registry operator import/export. It is a
// management snapshot, not a replication log: route imports get fresh target-side
// revisions/ballots and node liveness must still be re-confirmed by node-link.
type SnapshotRecord struct {
	Type  string         `json:"type"`
	Route *SandboxRecord `json:"route,omitempty"`
}

type SnapshotOptions struct {
	Kind  string
	Group string
}

type SnapshotSummary struct {
	Routes int `json:"routes"`
}

func (o SnapshotOptions) normalized() (SnapshotOptions, error) {
	if o.Kind == "" {
		o.Kind = "routes"
	}
	switch o.Kind {
	case "routes":
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
		out := sandboxRecordFromBuild(b)
		if err := enc.Encode(SnapshotRecord{Type: SnapshotKindRoute, Route: out}); err != nil {
			return err
		}
		sum.Routes++
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
			if route.SID != "" {
				r.indexSID(route.SID, route.Group, route.RouteKey)
			}
			sum.Routes++
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
		http.Error(w, err.Error(), http.StatusBadRequest)
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

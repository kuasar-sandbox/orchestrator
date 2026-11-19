// Package workspace associates data disks with the guest paths through which
// an application consumes them.
package workspace

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

const Namespace = "kuasar-sandbox.workspace"
const Header = "X-Kuasar-Sandbox-Workspace"

type Mount struct {
	Path    string `json:"path"`
	Subpath string `json:"subpath,omitempty"`
}
type Entry struct {
	Name   string                   `json:"name"`
	Mounts []Mount                  `json:"mounts,omitempty"`
	Slot   *rtconfig.DiskSlotConfig `json:"slot,omitempty"`
	Disk   json.RawMessage          `json:"disk,omitempty"`
}

// Prepared holds the fixed private create choice after token resolution.
// An entry with no choice inherits a captured device or reserves a new slot.
type Prepared struct {
	Name   string                   `json:"name"`
	Mounts []Mount                  `json:"mounts,omitempty"`
	Slot   *rtconfig.DiskSlotConfig `json:"slot,omitempty"`
	Disk   *rtconfig.DiskV1         `json:"disk,omitempty"`
	// DefaultFromSlot preserves an explicit disk:{} request admitted without a
	// node template. Resolve it only against the selected, unbound fixed slot.
	DefaultFromSlot bool                  `json:"defaultFromSlot,omitempty"`
	AuthorizeLayers bool                  `json:"authorizeLayers,omitempty"`
	RefLocations    rtconfig.RefLocations `json:"refLocations,omitempty"`
}

// ResolveDefault fixes a pending default after the target's authenticated
// geometry is available. A nonpositive capacity is a dynamic/unknown slot,
// never a size from which to invent a filesystem.
func (p Prepared) ResolveDefault(capacity int64) (Prepared, error) {
	if !p.DefaultFromSlot {
		return p, nil
	}
	if p.Disk != nil || p.Slot != nil || capacity <= 0 {
		return p, fmt.Errorf("workspace %q: default disk requires a template or a fixed target slot capacity", p.Name)
	}
	disk, err := (rtconfig.DiskV1{Kind: "disk", Version: 1, Size: &capacity}).NormalizeCreate()
	if err != nil {
		return p, fmt.Errorf("workspace %q: %w", p.Name, err)
	}
	p.Disk, p.DefaultFromSlot = &disk, false
	return p, nil
}

func Parse(raw string) ([]Entry, error) {
	var entries []Entry
	if err := strictjson.Decode([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("workspace: %w", err)
	}
	return validateEntries(entries, false)
}

// Saved views may contain "." components accepted by older versions.
func validateEntries(entries []Entry, savedView bool) ([]Entry, error) {
	if len(entries) > rtconfig.MaxDataDisks {
		return nil, fmt.Errorf("workspace: maximum %d disks", rtconfig.MaxDataDisks)
	}
	names := map[string]bool{}
	var targets, subpaths []string
	for i := range entries {
		e := &entries[i]
		if strings.TrimSpace(e.Name) == "" || strings.IndexByte(e.Name, 0) >= 0 || names[e.Name] {
			return nil, fmt.Errorf("workspace: empty or duplicate name at index %d", i)
		}
		names[e.Name] = true
		if e.Slot != nil && len(e.Disk) > 0 {
			return nil, errors.New("workspace: slot and disk are mutually exclusive")
		}
		if e.Slot != nil {
			if _, err := e.Slot.Capacity(); err != nil {
				return nil, err
			}
		}
		if e.Disk != nil {
			if err := ValidateDisk(e.Disk); err != nil {
				return nil, err
			}
		}
		if e.Mounts != nil && len(e.Mounts) == 0 {
			return nil, errors.New("workspace: mounts must be nonempty")
		}
		subpaths = subpaths[:0]
		for j := range e.Mounts {
			m := &e.Mounts[j]
			if !path.IsAbs(m.Path) || strings.IndexByte(m.Path, 0) >= 0 || invalidPathComponent(m.Path, savedView) || path.Clean(m.Path) == "/" || path.IsAbs(m.Subpath) || strings.IndexByte(m.Subpath, 0) >= 0 || invalidPathComponent(m.Subpath, savedView) {
				return nil, fmt.Errorf("workspace %q: invalid mount path or subpath", e.Name)
			}
			m.Path = path.Clean(m.Path)
			m.Subpath = path.Clean(m.Subpath)
			if m.Subpath == "." {
				m.Subpath = ""
			}
			if overlapping, ok := overlappingPath(targets, m.Path); ok {
				return nil, fmt.Errorf("workspace %q: target %q overlaps %q", e.Name, m.Path, overlapping)
			}
			if overlapping, ok := overlappingPath(subpaths, m.Subpath); ok {
				return nil, fmt.Errorf("workspace %q: subpath %q overlaps %q", e.Name, m.Subpath, overlapping)
			}
			targets = append(targets, m.Path)
			subpaths = append(subpaths, m.Subpath)
		}
	}
	if len(targets) > rtconfig.MaxPortableMounts {
		return nil, fmt.Errorf("workspace: at most %d mount targets", rtconfig.MaxPortableMounts)
	}
	if len(TopologyJSON(entries)) > rtconfig.MaxPortableScalarBytes {
		return nil, fmt.Errorf("workspace topology exceeds %d bytes", rtconfig.MaxPortableScalarBytes)
	}
	return entries, nil
}

func invalidPathComponent(name string, savedView bool) bool {
	for part := range strings.SplitSeq(name, "/") {
		if part == ".." || part == "." && !savedView {
			return true
		}
	}
	return false
}

// Both targets and subpaths have already been cleaned. The empty subpath
// denotes the disk root and overlaps every directory on that disk.
func overlappingPath(previous []string, name string) (string, bool) {
	for _, p := range previous {
		if name == p || name == "" || p == "" || strings.HasPrefix(p, name+"/") || strings.HasPrefix(name, p+"/") {
			return p, true
		}
	}
	return "", false
}

func ValidateDisk(raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := strictjson.Decode(raw, &fields); err != nil {
		return fmt.Errorf("workspace disk: %w", err)
	}
	if len(fields) == 0 {
		return nil
	}
	if token, ok := fields["migrationToken"]; ok {
		var s string
		if len(fields) != 1 || json.Unmarshal(token, &s) != nil || s == "" {
			return errors.New("workspace disk: migrationToken must be a nonempty string and the sole field")
		}
		return nil
	}
	var d rtconfig.DiskV1
	return json.Unmarshal(raw, &d)
}

func TopologyJSON(entries []Entry) string {
	public := make([]Entry, len(entries))
	for i, e := range entries {
		public[i] = Entry{Name: e.Name, Mounts: e.Mounts}
	}
	b, _ := json.Marshal(public)
	return string(b)
}

func Extract(metadata map[string]string) (map[string]string, []Entry, error) {
	raw, ok := metadata[Namespace]
	if !ok {
		return metadata, nil, nil
	}
	entries, err := Parse(raw)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[string]string, len(metadata))
	for k, v := range metadata {
		out[k] = v
	}
	out[Namespace] = TopologyJSON(entries)
	return out, entries, nil
}

func projection(name string, m Mount) rtconfig.MountConfig {
	return rtconfig.MountConfig{Type: "disk", Target: m.Path, Source: name, Subpath: m.Subpath}
}

func Topology(plan []Prepared) string {
	entries := make([]Entry, len(plan))
	for i, p := range plan {
		entries[i] = Entry{Name: p.Name, Mounts: p.Mounts}
	}
	return TopologyJSON(entries)
}

// ApplyImage adds devices in input order and validates against ordinary mounts.
func ApplyImage(c *rtconfig.SandboxConfig, plan []Prepared) error {
	if len(plan) == 0 {
		return nil
	}
	if len(plan)+len(c.Boot.Disks) > rtconfig.MaxDataDisks {
		return errors.New("workspace: data disk limit exceeded")
	}
	used := map[string]bool{}
	for _, d := range c.Boot.Disks {
		used[d.Name] = true
	}
	for _, m := range c.Mounts {
		if m.Type == "disk" {
			used[m.Source] = true
		}
	}
	for _, p := range plan {
		if p.DefaultFromSlot {
			return fmt.Errorf("workspace %q: default disk requires a template or a fixed target slot capacity", p.Name)
		}
		if used[p.Name] || len(p.Mounts) == 0 {
			return fmt.Errorf("workspace %q: duplicate disk name or missing mounts", p.Name)
		}
		used[p.Name] = true
		d := rtconfig.DiskConfig{Name: p.Name, Slot: p.Slot, Disk: p.Disk}
		if d.Slot == nil && d.Disk == nil {
			d.Slot = &rtconfig.DiskSlotConfig{}
		}
		c.Boot.Disks = append(c.Boot.Disks, d)
		for _, m := range p.Mounts {
			c.Mounts = append(c.Mounts, projection(p.Name, m))
		}
	}
	if len(c.Mounts) > rtconfig.MaxPortableMounts {
		return fmt.Errorf("workspace: at most %d mount targets", rtconfig.MaxPortableMounts)
	}
	if err := c.ValidateMountTopology(c.Mounts); err != nil {
		return err
	}
	if c.Metadata == nil {
		c.Metadata = map[string]string{}
	}
	c.Metadata[Namespace] = Topology(plan)
	return nil
}

// MergeLocations converts sandboxer directories into the CLI's URI map.
func MergeLocations(dest map[string]string, plan []Prepared) error {
	for _, p := range plan {
		for name, dir := range p.RefLocations {
			uri := (&url.URL{Scheme: "file", Path: dir}).String()
			if prev, ok := dest[name]; ok && prev != uri {
				return fmt.Errorf("workspace %q: conflicting named location %q", p.Name, name)
			}
			dest[name] = uri
		}
	}
	return nil
}

// Captured returns presentations whose names and complete mount sets match
// native devices in the authenticated E. Stale metadata cannot block boot.
func Captured(cfg *rtconfig.PortableSandboxConfig) []Entry {
	if cfg == nil || cfg.Version < 2 {
		return nil
	}
	raw, ok := cfg.Metadata[Namespace]
	if !ok {
		return nil
	}
	disks := make(map[string][]Mount, len(cfg.Boot.Disks))
	for _, d := range cfg.Boot.Disks {
		if d.PureBlock || d.Slot != nil {
			disks[d.Name] = nil
		}
	}
	for _, m := range cfg.Mounts {
		if _, ok := disks[m.Source]; !ok || m.Type != "disk" {
			continue
		}
		sub := path.Clean(m.Subpath)
		if sub == "." {
			sub = ""
		}
		disks[m.Source] = append(disks[m.Source], Mount{Path: path.Clean(m.Target), Subpath: sub})
	}
	return savedViews(raw, disks)
}

// A nil disk table projects only public fields from older records. When a
// table is available, require a matching native disk and its projections.
func savedViews(raw string, disks map[string][]Mount) []Entry {
	var rows []json.RawMessage
	if json.Unmarshal([]byte(raw), &rows) != nil || rows == nil {
		return nil
	}
	seen := make(map[string]bool)
	ambiguous := make(map[string]bool)
	views := make([]Entry, 0, len(rows))
	for _, row := range rows {
		// Older records may carry private selectors. Decode only the name and
		// owned projections, without opening or replaying any disk source.
		var view struct {
			Name   string          `json:"name"`
			Mounts json.RawMessage `json:"mounts"`
		}
		if strictjson.DecodeAllowUnknown(row, &view) != nil || view.Name == "" {
			continue
		}
		if seen[view.Name] {
			ambiguous[view.Name] = true
			continue
		}
		seen[view.Name] = true
		entry := Entry{Name: view.Name}
		if view.Mounts != nil && strictjson.Decode(view.Mounts, &entry.Mounts) != nil {
			continue
		}
		entries, err := validateEntries([]Entry{entry}, true)
		if err != nil {
			continue
		}
		e := entries[0]
		if disks != nil && (len(e.Mounts) == 0 || !slices.Equal(disks[e.Name], e.Mounts)) {
			continue
		}
		views = append(views, e)
	}
	views = slices.DeleteFunc(views, func(e Entry) bool { return ambiguous[e.Name] })
	if len(views) > rtconfig.MaxDataDisks {
		return nil
	}
	// Resolve target conflicts after checking native matches. An orphaned
	// row cannot conceal the real workspace mounted at its former target.
	for i, a := range views {
		targets := make([]string, 0, len(a.Mounts))
		for _, m := range a.Mounts {
			targets = append(targets, m.Path)
		}
		for _, b := range views[i+1:] {
			for _, m := range b.Mounts {
				if _, overlaps := overlappingPath(targets, m.Path); overlaps {
					ambiguous[a.Name], ambiguous[b.Name] = true, true
					break
				}
			}
		}
	}
	views = slices.DeleteFunc(views, func(e Entry) bool { return ambiguous[e.Name] })
	if _, err := validateEntries(views, true); err != nil {
		return nil
	}
	return views
}

// SetTopology replaces only this namespace, keeping all other metadata intact.
func SetTopology(meta map[string]string, plan []Prepared) map[string]string {
	if _, ok := meta[Namespace]; !ok && len(plan) == 0 {
		return meta
	}
	out := make(map[string]string, len(meta)+1)
	for k, v := range meta {
		if k != Namespace {
			out[k] = v
		}
	}
	if len(plan) > 0 {
		out[Namespace] = Topology(plan)
	}
	return out
}

// AdditionalDevices returns new devices in declaration order. Existing names are
// resolved only through the captured presentation; ordinary disks cannot be claimed.
func AdditionalDevices(disks []string, captured []Entry, choices []Prepared) ([]Prepared, error) {
	names := make(map[string]bool, len(disks))
	for _, name := range disks {
		names[name] = true
	}
	presented := make(map[string]bool, len(captured))
	for _, e := range captured {
		presented[e.Name] = true
	}
	var added []Prepared
	for _, p := range choices {
		if presented[p.Name] {
			continue
		}
		if p.DefaultFromSlot {
			return nil, fmt.Errorf("workspace %q: default disk requires a template or a fixed target slot capacity", p.Name)
		}
		if names[p.Name] || len(p.Mounts) == 0 {
			return nil, fmt.Errorf("workspace %q: disk name exists or new mounts are missing", p.Name)
		}
		names[p.Name] = true
		added = append(added, p)
		captured = append(captured, Entry{Name: p.Name, Mounts: p.Mounts})
	}
	if len(names) > rtconfig.MaxDataDisks {
		return nil, errors.New("workspace: data disk limit exceeded")
	}
	if len(added) > 0 {
		if _, err := Parse(TopologyJSON(captured)); err != nil {
			return nil, err
		}
	}
	return added, nil
}

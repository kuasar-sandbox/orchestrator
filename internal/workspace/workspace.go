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
	Name            string                   `json:"name"`
	Mounts          []Mount                  `json:"mounts,omitempty"`
	Slot            *rtconfig.DiskSlotConfig `json:"slot,omitempty"`
	Disk            *rtconfig.DiskV1         `json:"disk,omitempty"`
	AuthorizeLayers bool                     `json:"authorizeLayers,omitempty"`
	RefLocations    rtconfig.RefLocations    `json:"refLocations,omitempty"`
}

func Parse(raw string) ([]Entry, error) {
	var entries []Entry
	if err := strictjson.Decode([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("workspace: %w", err)
	}
	if len(entries) > rtconfig.MaxDataDisks {
		return nil, fmt.Errorf("workspace: maximum %d disks", rtconfig.MaxDataDisks)
	}
	names := map[string]bool{}
	var mounts []rtconfig.MountConfig
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
		for j := range e.Mounts {
			m := &e.Mounts[j]
			if !path.IsAbs(m.Path) || strings.IndexByte(m.Path, 0) >= 0 || slices.Contains(strings.Split(m.Path, "/"), "..") || path.Clean(m.Path) == "/" || path.IsAbs(m.Subpath) || strings.IndexByte(m.Subpath, 0) >= 0 || slices.Contains(strings.Split(m.Subpath, "/"), "..") {
				return nil, fmt.Errorf("workspace %q: invalid mount path or subpath", e.Name)
			}
			m.Path = path.Clean(m.Path)
			m.Subpath = path.Clean(m.Subpath)
			if m.Subpath == "." {
				m.Subpath = ""
			}
			mounts = append(mounts, projection(e.Name, *m))
		}
	}
	if len(mounts) > rtconfig.MaxPortableMounts {
		return nil, fmt.Errorf("workspace: at most %d mount targets", rtconfig.MaxPortableMounts)
	}
	if err := rtconfig.ValidateMountProjections(mounts); err != nil {
		return nil, err
	}
	if len(TopologyJSON(entries)) > rtconfig.MaxPortableScalarBytes {
		return nil, fmt.Errorf("workspace topology exceeds %d bytes", rtconfig.MaxPortableScalarBytes)
	}
	return entries, nil
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
	if err := rtconfig.ValidateMountProjections(c.Mounts); err != nil {
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

// Captured projects workspace topology from the authenticated E. The metadata
// alone is insufficient: every declared device and projection must exist.
func Captured(cfg *rtconfig.PortableSandboxConfig) ([]Entry, error) {
	if cfg == nil || cfg.Version < 2 {
		return nil, nil
	}
	raw, ok := cfg.Metadata[Namespace]
	if !ok {
		return nil, nil
	}
	entries, err := Parse(raw)
	if err != nil {
		return nil, err
	}
	disks := map[string]rtconfig.PortableDiskConfig{}
	for _, d := range cfg.Boot.Disks {
		disks[d.Name] = d
	}
	for _, e := range entries {
		disk, ok := disks[e.Name]
		if !ok || (!disk.PureBlock && disk.Slot == nil) || e.Slot != nil || e.Disk != nil || len(e.Mounts) == 0 {
			return nil, fmt.Errorf("workspace %q: invalid captured disk declaration", e.Name)
		}
		var actual []Mount
		for _, m := range cfg.Mounts {
			if m.Type == "disk" && m.Source == e.Name {
				sub := path.Clean(m.Subpath)
				if sub == "." {
					sub = ""
				}
				actual = append(actual, Mount{Path: path.Clean(m.Target), Subpath: sub})
			}
		}
		if !slices.Equal(actual, e.Mounts) {
			return nil, fmt.Errorf("workspace %q: captured mounts disagree with disk table", e.Name)
		}
	}
	return entries, nil
}

// SetTopology replaces only this namespace, keeping all other metadata intact.
func SetTopology(meta map[string]string, plan []Prepared) map[string]string {
	if len(plan) == 0 && meta[Namespace] == "" {
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

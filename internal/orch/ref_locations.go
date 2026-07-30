package orch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const maxSnapshotGraphRefs = 1024

// templateRefLocations expands every logical location needed to consume a
// template. Snapshot templates are walked through from_refs; image templates
// need only their own ref.
func (o *Orchestrator) templateRefLocations(ctx context.Context, manifestKey string, tmpl types.TemplateID) (map[string]string, error) {
	if tmpl.Kind == types.KindSnp {
		return o.snapshotRefLocations(ctx, manifestKey, tmpl.Ref)
	}
	locations := map[string]string{}
	if err := o.addRefLocation(locations, tmpl.Ref); err != nil {
		return nil, err
	}
	return locations, nil
}

func (o *Orchestrator) sandboxRefLocations(ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID) (map[string]string, error) {
	if restoreRef := sandboxcfg.RestoreRefFor(sb, tmpl); restoreRef != "" {
		return o.snapshotRefLocations(ctx, sb.ManifestKey, restoreRef)
	}
	return o.templateRefLocations(ctx, sb.ManifestKey, tmpl)
}

// snapshotRefLocations walks only snapshot parent refs. All disk/runtime refs
// are scanned for logical locations but are not mistaken for snapshot bundles.
func (o *Orchestrator) snapshotRefLocations(ctx context.Context, manifestKey, root string) (map[string]string, error) {
	if o.cfg.Checkpoint.Remote.RefLocationParent == "" {
		if ref, err := manifest.ParseRef(root); err == nil && ref.Location != "" {
			return nil, fmt.Errorf("resolve ref location %q: checkpoint.remote.ref_location_parent is not configured", ref.Location)
		}
		return nil, nil
	}
	locations := map[string]string{}
	pending := []string{root}
	seen := map[string]bool{}
	for len(pending) > 0 {
		ref := pending[0]
		pending = pending[1:]
		if seen[ref] {
			continue
		}
		if len(seen) >= maxSnapshotGraphRefs {
			return nil, fmt.Errorf("snapshot ref graph exceeds %d entries", maxSnapshotGraphRefs)
		}
		seen[ref] = true
		if err := o.addRefLocation(locations, ref); err != nil {
			return nil, err
		}
		cfg, err := o.readSnapshotRefConfig(ctx, manifestKey, ref, locations)
		if err != nil {
			return nil, err
		}
		pending = append(pending, cfg.FromRefs...)
		for _, artifactRef := range cfg.artifactRefs() {
			if err := o.addRefLocation(locations, artifactRef); err != nil {
				return nil, err
			}
		}
	}
	return locations, nil
}

func (o *Orchestrator) addRefLocation(locations map[string]string, raw string) error {
	ref, err := manifest.ParseRef(raw)
	if err != nil || ref.Location == "" {
		return nil // local paths and manifest refs do not need a location mapping
	}
	uri, err := o.cfg.Checkpoint.RefLocationURI(ref.Location)
	if err != nil {
		return fmt.Errorf("resolve ref location %q: %w", ref.Location, err)
	}
	locations[ref.Location] = uri
	return nil
}

type snapshotRefConfig struct {
	FromRefs []string `json:"FromRefs"`
	Boot     struct {
		RuntimeRef string             `json:"RuntimeRef"`
		Root       snapshotDiskNode   `json:"Root"`
		Disks      []snapshotDiskNode `json:"Disks"`
	} `json:"Boot"`
}

type snapshotDiskNode struct {
	BaseRef      string   `json:"BaseRef"`
	Base         string   `json:"Base"`
	BaseFromRefs []string `json:"BaseFromRefs"`
	Overlay      *struct {
		Base         string   `json:"Base"`
		BaseFromRefs []string `json:"BaseFromRefs"`
	} `json:"Overlay"`
}

func (c snapshotRefConfig) artifactRefs() []string {
	refs := []string{c.Boot.RuntimeRef}
	add := func(node snapshotDiskNode) {
		refs = append(refs, node.BaseRef, node.Base)
		refs = append(refs, node.BaseFromRefs...)
		if node.Overlay != nil {
			refs = append(refs, node.Overlay.Base)
			refs = append(refs, node.Overlay.BaseFromRefs...)
		}
	}
	add(c.Boot.Root)
	for _, disk := range c.Boot.Disks {
		add(disk)
	}
	return refs
}

func (o *Orchestrator) readSnapshotRefConfig(ctx context.Context, manifestKey, ref string, locations map[string]string) (snapshotRefConfig, error) {
	args := []string{"info", "--json", "--manifest-config", o.cfg.ManifestConfig}
	args = appendRefLocationArgs(args, locations)
	args = append(args, ref)
	cmd := exec.CommandContext(ctx, o.cfg.SandboxCtl(), args...)
	cmd.Env = append(os.Environ(), "MANIFEST_KEY="+manifestKey)
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return snapshotRefConfig{}, fmt.Errorf("read snapshot ref %q: %w: %s", ref, err, errb.String())
	}
	var cfg snapshotRefConfig
	if err := json.Unmarshal(out.Bytes(), &cfg); err != nil {
		return snapshotRefConfig{}, fmt.Errorf("parse snapshot ref %q: %w", ref, err)
	}
	return cfg, nil
}

func appendRefLocationArgs(args []string, locations map[string]string) []string {
	names := make([]string, 0, len(locations))
	for name := range locations {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		args = append(args, "--ref-location", name+"="+locations[name])
	}
	return args
}

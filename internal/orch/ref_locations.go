package orch

import (
	"fmt"
	"sort"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
)

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

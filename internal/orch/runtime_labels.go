package orch

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
)

// RuntimeLabels rebuilds reserved facts once at Node startup. The configured
// runtime's lexical parent is the inventory boundary, even if it is a symlink
// into a release directory. No watcher or download path is introduced.
func RuntimeLabels(configured map[string]string, primary string, diagnose func(string, error)) map[string]string {
	labels := make(map[string]string, len(configured))
	for key, value := range configured {
		if !strings.HasPrefix(key, placement.RuntimeLabelPrefix) {
			labels[key] = value
		}
	}
	primary = strings.TrimPrefix(primary, "file://")
	parent := filepath.Dir(primary)
	entries, err := os.ReadDir(parent)
	if err != nil {
		if diagnose != nil {
			diagnose(parent, err)
		}
		return labels
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".bundle") {
			continue
		}
		path := filepath.Join(parent, entry.Name())
		info, err := artifact.InspectRuntimeBundle(path)
		if err == nil {
			identity := manifest.RuntimeIdentity{Basename: entry.Name(), Digest: strings.TrimPrefix(info.Digest, "digest:")}
			err = identity.Validate()
		}
		if err != nil {
			if diagnose != nil {
				diagnose(path, err)
			}
			continue
		}
		labels[placement.RuntimeLabelPrefix+entry.Name()] = info.Digest
	}
	return labels
}

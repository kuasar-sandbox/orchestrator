package builder

import (
	"os"
	"sort"
	"strings"
)

// authoritativeProcessEnv overlays each key exactly once. run-builder already
// installed its task bootstrap environment, but explicit child overrides must
// not append a second MANIFEST_KEY entry inherited from os.Environ.
func authoritativeProcessEnv(overrides map[string]string) []string {
	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	env := make([]string, 0, len(os.Environ())+len(keys))
	for _, entry := range os.Environ() {
		key, _, found := strings.Cut(entry, "=")
		if found {
			if _, replace := blocked[key]; replace {
				continue
			}
		}
		env = append(env, entry)
	}
	for _, key := range keys {
		env = append(env, key+"="+overrides[key])
	}
	return env
}

package orch

import "github.com/kuasar-sandbox/orchestrator/internal/configsock"

// buildRuntimeDir derives a fixed-length node-local directory from the complete
// opaque BuildID. BuildID remains the durable identity; only this reconstructible
// runtime path is compacted so nested sandbox Unix sockets stay below sun_path.
func buildRuntimeDir(runRoot, buildID string) string {
	return configsock.BuildRuntimeDir(runRoot, buildID)
}

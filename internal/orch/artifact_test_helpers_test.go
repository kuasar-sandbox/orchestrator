package orch

import "github.com/kuasar-sandbox/orchestrator/internal/types"

func validArtifactDiskTopology() types.ArtifactDiskTopology {
	return types.ArtifactDiskTopology{
		Root: types.ArtifactDiskShape{
			Mode:          types.ArtifactDiskOverlay,
			HasActiveBase: true,
		},
	}
}

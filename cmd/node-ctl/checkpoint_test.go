package main

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
)

func TestWarnDeprecatedCheckpointMode(t *testing.T) {
	tests := []struct {
		name              string
		checkpoint        config.CheckpointConfig
		wantWarnings      int
		wantCompatibility bool
	}{
		{name: "local", checkpoint: config.CheckpointConfig{Mode: config.CheckpointLocal}},
		{name: "remote", checkpoint: config.CheckpointConfig{Mode: config.CheckpointRemote}, wantWarnings: 1},
		{
			name: "remote with ref location parent",
			checkpoint: config.CheckpointConfig{
				Mode: config.CheckpointRemote,
				Remote: config.CheckpointRemoteConfig{
					RefLocationParent: "file:///mnt/checkpoints",
				},
			},
			wantWarnings: 1, wantCompatibility: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&output, nil))
			warnDeprecatedCheckpointMode(&config.Config{Checkpoint: tc.checkpoint}, logger)
			const warning = "checkpoint.mode=remote is deprecated; use checkpoint.mode=local and publish separately"
			if got := strings.Count(output.String(), warning); got != tc.wantWarnings {
				t.Fatalf("warning count = %d, want %d: %s", got, tc.wantWarnings, output.String())
			}
			if got := strings.Contains(output.String(), "Pause currently uses local capture"); got != tc.wantCompatibility {
				t.Fatalf("compatibility detail present=%t, want %t: %s", got, tc.wantCompatibility, output.String())
			}
		})
	}
}

package controlplane

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type recoveryConfigSender struct{}

func (recoveryConfigSender) SendRecoveryCommand(
	context.Context,
	session.ServeIdentity,
	string,
	uint64,
	string,
	*routesync.Command,
) (routesync.CmdAck, bool, error) {
	return routesync.CmdAck{}, false, nil
}

func TestRecoveryCoordinatorBoundsPerNodeWorkers(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		want    int
	}{
		{name: "explicit bounded value", workers: 2, want: 2},
		{name: "zero uses safe default", workers: 0, want: 1},
		{name: "cannot exceed cluster workers", workers: 5, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coordinator, err := NewRecoveryCoordinator(
				&RaftStore{},
				&RecoveryMesh{},
				session.NewDirectory(allowTestDirectoryEntries{}),
				recoveryConfigSender{},
				RecoveryCoordinatorConfig{Workers: 4, PerNodeWorkers: test.workers},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if coordinator.config.PerNodeWorkers != test.want {
				t.Fatalf("PerNodeWorkers=%d, want %d", coordinator.config.PerNodeWorkers, test.want)
			}
		})
	}
}

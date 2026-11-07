package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/signal"
	"syscall"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
)

// checkSnapshotRuntimeCmd is an internal tenant-process admission helper. Inputs
// contain no key; the parent installs this tenant's MANIFEST_KEY in the child env.
func checkSnapshotRuntimeCmd(in io.Reader, out io.Writer) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return checkSnapshotRuntimeWith(ctx, in, out, taskartifact.CheckSnapshotRuntime)
}

func checkSnapshotRuntimeWith(ctx context.Context, in io.Reader, out io.Writer,
	check func(context.Context, configsock.SnapshotRuntimeCheckRequest) error) error {
	const maxRequestBytes = 64 << 10
	raw, err := io.ReadAll(io.LimitReader(in, maxRequestBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxRequestBytes {
		return errors.New("snapshot runtime request too large")
	}
	var request configsock.SnapshotRuntimeCheckRequest
	if err := strictjson.Decode(raw, &request); err != nil {
		return fmt.Errorf("decode snapshot runtime request: %w", err)
	}
	result := configsock.SnapshotRuntimeCheckResult{Status: configsock.SnapshotRuntimeCompatible}
	if err := check(ctx, request); err != nil {
		result.Error = sanitizeSandboxResultError(err.Error())
		switch {
		case errors.Is(err, restore.ErrRuntimeIncompatible):
			result.Status = configsock.SnapshotRuntimeIncompatible
		case errors.Is(err, taskartifact.ErrSnapshotPairMismatch):
			result.Status = configsock.SnapshotRuntimeSourceMismatch
		default:
			result.Status = configsock.SnapshotRuntimeCheckError
		}
	}
	return json.NewEncoder(out).Encode(result)
}

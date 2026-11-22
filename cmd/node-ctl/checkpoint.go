package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/orch"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

const checkpointCleanupDeferredExitCode = orch.CheckpointMemoryReadersBusyExitCode

var errCheckpointCleanupDeferred = errors.New("checkpoint cleanup deferred while memory readers are active")

// This tenant-key-bound tool collects a committed paused source. Active memory
// readers produce an explicit deferred result; Sandboxer owns interpretation
// and deletion, and this adapter supplies the existing node ref-location policy.
func checkpointCleanupCmd(args []string) error {
	fs := flag.NewFlagSet("checkpoint-cleanup", flag.ContinueOnError)
	sid := fs.String("sandbox-id", "", "capture producer identity")
	pathID := fs.String("path-id", "", "owned base directory leaf (defaults to sandbox-id)")
	base := fs.String("base-root", "", "exclusive sandbox base root")
	e := fs.String("from", "", "committed Sandbox E ref")
	s := fs.String("restore", "", "committed Snapshot S ref, if present")
	retain := fs.String("retain", "", "additional immutable S/E pairs to retain (JSON)")
	manifestPath := fs.String("manifest-config", "", "storage and crypto config")
	parent := fs.String("ref-location-parent", "", "trusted named-location parent URI")
	list := fs.Bool("list-versions", false, "report protected DIFF versions as JSON without cleanup")
	if err := fs.Parse(args); err != nil {
		return err
	}
	leaf, err := sandbox.ResolvePathID(*sid, *pathID)
	if err != nil || sandbox.ValidatePathID(*sid) != nil || *base == "" || fs.NArg() != 0 {
		return fmt.Errorf("checkpoint-cleanup requires --sandbox-id, --base-root and explicit --retain [--from --restore]")
	}
	var retained []artifact.CheckpointSource
	if err := json.Unmarshal([]byte(*retain), &retained); err != nil {
		return fmt.Errorf("invalid retained checkpoint roots: %w", err)
	}
	if retained == nil {
		return errors.New("checkpoint-cleanup requires an explicit complete retention set, including []")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.LoadManifestConfig(*manifestPath)
	if errors.Is(err, manifest.ErrConfigNotProvided) {
		cfg, err = nil, nil
	}
	if err != nil {
		return err
	}
	storage, err := artifact.NewProcessStorage(cfg)
	if err != nil {
		return err
	}
	checkpoint := artifact.Checkpoint{
		Directory: filepath.Join(sandbox.DefaultBaseDir(*base, leaf), "checkpoint"), SandboxID: *sid,
		SandboxRef: *e, SnapshotRef: *s, Retain: retained,
		ResolveLocation: func(name string) (string, error) {
			loc, err := reflocation.Resolve(*parent, name)
			return loc.Path, err
		},
	}
	if *list {
		var refs []artifact.DiffCheckpointRef
		refs, err = storage.CheckpointVersions(ctx, checkpoint)
		if err == nil {
			err = json.NewEncoder(os.Stdout).Encode(refs)
		}
	} else {
		err = storage.CleanupCheckpoint(ctx, checkpoint)
	}
	return checkpointCleanupResult(err, storage.Close(), *list)
}

func checkpointCleanupResult(cleanupErr, closeErr error, inspect bool) error {
	// Preserve every genuine I/O/close error. The SDK exposes this sentinel
	// only when physical collection is deferred; logical revocation may have completed.
	if !inspect && closeErr == nil && errors.Is(cleanupErr, artifact.ErrCheckpointMemoryReadersBusy) {
		return fmt.Errorf("%w: %v", errCheckpointCleanupDeferred, cleanupErr)
	}
	return errors.Join(cleanupErr, closeErr)
}

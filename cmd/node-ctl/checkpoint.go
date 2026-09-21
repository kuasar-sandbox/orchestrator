package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest"
	"github.com/kuasar-sandbox/orchestrator/internal/reflocation"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

// This short-lived, tenant-key-bound tool is invoked only after conductor has
// committed S/E and fenced its users. Sandboxer owns interpretation/deletion;
// this adapter supplies the existing node ref-location policy.
func checkpointCleanupCmd(args []string) error {
	fs := flag.NewFlagSet("checkpoint-cleanup", flag.ContinueOnError)
	sid := fs.String("sandbox-id", "", "capture producer identity")
	pathID := fs.String("path-id", "", "owned base directory leaf (defaults to sandbox-id)")
	base := fs.String("base-root", "", "exclusive sandbox base root")
	e := fs.String("from", "", "committed Sandbox E ref")
	s := fs.String("restore", "", "committed Snapshot S ref, if present")
	manifestPath := fs.String("manifest-config", "", "storage and crypto config")
	parent := fs.String("ref-location-parent", "", "trusted named-location parent URI")
	if err := fs.Parse(args); err != nil {
		return err
	}
	leaf, err := sandbox.ResolvePathID(*sid, *pathID)
	if err != nil || sandbox.ValidatePathID(*sid) != nil || *base == "" || *e == "" || fs.NArg() != 0 {
		return fmt.Errorf("checkpoint-cleanup requires --sandbox-id, --base-root and committed --from [--restore]")
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
	err = storage.CleanupCheckpoint(ctx, artifact.Checkpoint{
		Directory: filepath.Join(sandbox.DefaultBaseDir(*base, leaf), "checkpoint"), SandboxID: *sid,
		SandboxRef: *e, SnapshotRef: *s,
		ResolveLocation: func(name string) (string, error) {
			loc, err := reflocation.Resolve(*parent, name)
			return loc.Path, err
		},
	})
	return errors.Join(err, storage.Close())
}

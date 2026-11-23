package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestLaunchTaskProjectsPreparedSourceBeforeSpawnWithoutReread(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "supported-primary", true: "unknown-layout"}[invalid], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "sandbox.yaml")
			if err := os.WriteFile(path, []byte("boot:\n  kernel: file:///kernel\n  runtime: file:///runtime\nlaunch:\n  systemd: provided\n  env:\n    INSTANCE: yes\n"), 0600); err != nil {
				t.Fatal(err)
			}
			source := &rtconfig.PortableSandboxConfig{
				Version:   rtconfig.PortableSandboxConfigVersion,
				Resources: rtconfig.PortableResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"}, Allocatable: rtconfig.AllocatableConfig{CPU: 1, Memory: "256MiB"}},
				Network:   rtconfig.PortableNetworkConfig{},
				Boot:      rtconfig.PortableBootConfig{Kernel: "file://kernel@digest:" + strings.Repeat("a", 64), Runtime: "file://runtime@digest:" + strings.Repeat("b", 64), Root: rtconfig.PortableRootConfig{Base: "manifest://" + strings.Repeat("c", 64), Overlay: &rtconfig.PortableOverlayConfig{Base: "self"}}},
				Launch:    rtconfig.PortableLaunchConfig{Exec: "/opt/sandbox-runtime/bin/envd", Args: []string{"-port", "49983"}, Env: map[string]string{"SOURCE": "task-local"}, User: "0:0", Restart: "always", PIDNamespace: "shared", CgroupControl: true},
			}
			if invalid {
				source.Launch.Exec = "/unknown/product"
			}
			vmm, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer vmm.Close()
			reads, starts := 0, 0
			spawned := errors.New("spawn intercepted")
			err = launchTaskWith(context.Background(), nil, "/config.sock", "sid", "run", nil, vmm, taskLaunchOps{
				fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
					return &configsock.SandboxTaskSpec{SandboxID: "sid", RunID: "run", Env: map[string]string{"MANIFEST_KEY": "key"}, Prepare: &configsock.ArtifactPrepareSpec{RunID: "run"}}, nil
				},
				setenv: func(string, string) error { return nil },
				prepareArtifact: func(context.Context, configsock.ArtifactPrepareSpec) (*taskartifact.Result, error) {
					reads++
					return &taskartifact.Result{PreparedSource: types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://source"}, SourceSandboxConfig: source}, nil
				},
				completePrepare: func(context.Context, string, string, string, configsock.ArtifactPrepareSummary) (*configsock.LaunchSpec, error) {
					return &configsock.LaunchSpec{Exec: "/sandbox-ctl", Args: []string{"run", "--config", path}, SourceLaunch: &configsock.SourceLaunchProjection{Profile: types.ProfileE2B, ConfigPath: path}}, nil
				},
				startChild: func(_ string, argv, _ []string, _, _ *os.File, provenance []byte) error {
					starts++
					host, presence, err := rtconfig.LoadMergedWithPresence([]string{path})
					if err != nil {
						t.Fatal(err)
					}
					raw, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := rtconfig.ApplyConfigProvenance(raw, host, provenance); err != nil {
						t.Fatal(err)
					}
					if !presence.Has("launch.systemd") || host.Launch.Systemd != "provided" || host.Launch.Exec != source.Launch.Exec || host.Launch.Placeholder || len(host.Launch.Plugin) != 0 || host.Launch.Env["SOURCE"] != "task-local" || host.Launch.Env["INSTANCE"] != "yes" {
						t.Fatalf("spawn saw incomplete source projection: %+v", host.Launch)
					}
					args := host.Launch.Args
					if args[len(args)-1] != "/run/sandbox-init/cgroup" {
						t.Fatalf("envd root: %v", args)
					}
					if !strings.Contains(strings.Join(argv, " "), "--from manifest://source") {
						t.Fatalf("lost selected source: %v", argv)
					}
					return spawned
				},
			})
			if reads != 1 {
				t.Fatalf("source reads=%d", reads)
			}
			if invalid {
				if err == nil || starts != 0 || !strings.Contains(err.Error(), "new cold definition") {
					t.Fatalf("unknown layout spawned: starts=%d err=%v", starts, err)
				}
			} else if !errors.Is(err, spawned) || starts != 1 {
				t.Fatalf("spawn=%d err=%v", starts, err)
			}
		})
	}
}

package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestImageFastPathProjectsProductEnvironmentBeforeSpawn(t *testing.T) {
	for _, mismatch := range []bool{false, true} {
		t.Run(fmt.Sprint(mismatch), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "sandbox.yaml")
			ref := "manifest://" + strings.Repeat("a", 64)
			raw := "boot:\n  root:\n    base: " + ref + "\nlaunch:\n  systemd: provided\n  placeholder: true\n  env:\n    WIN: request\n  plugin:\n    - exec: /opt/sandbox-runtime/bin/envd\n      env:\n        WIN: request\n"
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			vmm, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer vmm.Close()
			deadline := time.Now().Add(time.Minute)
			reads, starts, keyInstalled := 0, 0, false
			spawned := errors.New("spawn intercepted")
			err = launchTaskWith(context.Background(), nil, "/socket", "sid", "run", nil, vmm, taskLaunchOps{
				fetchBootstrap: func(context.Context, string, string, string) (*configsock.SandboxTaskSpec, error) {
					return &configsock.SandboxTaskSpec{SandboxID: "sid", RunID: "run", AbsoluteDeadlineUnixNano: deadline.UnixNano(), Env: map[string]string{"MANIFEST_KEY": "key"}, Final: &configsock.LaunchSpec{Exec: "/sandbox-ctl", Args: []string{"run", "--config", path}, ImageLaunch: &configsock.ImageLaunchProjection{ConfigPath: path, ImageRef: ref}}}, nil
				},
				setenv: func(k, v string) error {
					if k == "MANIFEST_KEY" && v == "key" {
						keyInstalled = true
					}
					return nil
				},
				imageEnvironment: func(ctx context.Context, projection configsock.ImageLaunchProjection) (*taskartifact.ImageEnvironment, error) {
					reads++
					if d, ok := ctx.Deadline(); !ok || !d.Equal(deadline) || !keyInstalled {
						t.Fatal("image read escaped task key/deadline")
					}
					identity := projection.ImageRef
					if mismatch {
						identity = "manifest://different"
					}
					return &taskartifact.ImageEnvironment{ImageRef: identity, Env: map[string]string{"PATH": "/image/custom/bin:/usr/bin", "IMAGE": "yes", "WIN": "image"}}, nil
				},
				startChild: func(_ string, _, _ []string, _, _ *os.File, provenance []byte) error {
					starts++
					raw, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					host, err := rtconfig.LoadConfigBytes(raw)
					if err != nil {
						t.Fatal(err)
					}
					if err := rtconfig.ApplyConfigProvenance(raw, host, provenance); err != nil {
						t.Fatal(err)
					}
					env := host.Launch.Plugin[0].Env
					if env["IMAGE"] != "yes" || env["WIN"] != "request" || env["PATH"] != "/run/sandbox-runtime/systemd/bin:/image/custom/bin:/usr/bin" {
						t.Fatalf("wrong env: %#v", env)
					}
					if !host.Launch.PersistentSystemdPathDefault() || !host.Launch.Plugin[0].PersistentSystemdPathDefault() {
						t.Fatal("fresh image lost trusted PATH provenance at child handoff")
					}
					return spawned
				},
			})
			if reads != 1 {
				t.Fatalf("reads=%d", reads)
			}
			if mismatch {
				if err == nil || starts != 0 {
					t.Fatalf("mismatched image spawned: %d, %v", starts, err)
				}
			} else if !errors.Is(err, spawned) || starts != 1 {
				t.Fatalf("starts=%d err=%v", starts, err)
			}
		})
	}
}

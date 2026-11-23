package main

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxproc"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"golang.org/x/sys/unix"
)

func TestConfigProvenanceDescriptorSurvivesSpawnAndRejectsChangedConfig(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(strconv.FormatBool(changed), func(t *testing.T) {
			raw := []byte("launch:\n  systemd: runtime\n  env:\n    PATH: /image/bin:/usr/bin\n  plugin:\n    - exec: /envd\n      env:\n        PATH: /run/sandbox-runtime/systemd/bin:/image/bin:/usr/bin\n")
			cfg, err := rtconfig.LoadConfigBytes(raw)
			if err != nil {
				t.Fatal(err)
			}
			cfg.Launch.SetSystemdPathDefault(true)
			cfg.Launch.Plugin[0].SetSystemdPathDefault(true)
			envelope, err := rtconfig.MarshalConfigProvenance(raw, cfg.Launch)
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			configPath := filepath.Join(dir, "sandbox.yaml")
			if changed {
				raw = append(raw, '\n')
			}
			if err := os.WriteFile(configPath, raw, 0600); err != nil {
				t.Fatal(err)
			}
			vmm, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer vmm.Close()
			readyR, readyW, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer readyR.Close()
			defer readyW.Close()
			extra, err := os.Open(os.DevNull)
			if err != nil {
				t.Fatal(err)
			}
			defer extra.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestConfigProvenanceChild$", "--")
			cmd.Env = append(os.Environ(), "ORCH_CONFIG_PROVENANCE_CHILD=1", "ORCH_CONFIG_PROVENANCE_PATH="+configPath, rtconfig.ConfigProvenanceFDEnv+"=999")
			cmd.ExtraFiles = []*os.File{extra}
			file, err := sandboxproc.AttachConfigProvenance(cmd, envelope)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			st, err := file.Stat()
			if err != nil || !st.Mode().IsRegular() {
				t.Fatalf("not a regular handoff: %v, %v", st, err)
			}
			if _, err := file.WriteAt([]byte("x"), 0); err == nil {
				t.Fatal("provenance remained mutable")
			}
			seals, err := unix.FcntlInt(file.Fd(), unix.F_GET_SEALS, 0)
			if err != nil || seals&unix.F_SEAL_WRITE == 0 {
				t.Fatalf("missing write seal: %d, %v", seals, err)
			}
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := sandboxproc.Start(cmd, vmm, readyW); err != nil {
				t.Fatal(err)
			}
			_ = file.Close() // child must own its independent inherited reference
			err = cmd.Wait()
			if changed && err == nil {
				t.Fatal("child accepted changed final config")
			}
			if !changed && err != nil {
				t.Fatalf("child could not consume provenance: %v\n%s", err, output.Bytes())
			}
			if changed && !strings.Contains(output.String(), "config provenance") {
				t.Fatalf("unexpected child failure: %s", output.Bytes())
			}
		})
	}
}

func TestConfigProvenanceChild(t *testing.T) {
	if os.Getenv("ORCH_CONFIG_PROVENANCE_CHILD") != "1" {
		return
	}
	fd, err := strconv.Atoi(os.Getenv(rtconfig.ConfigProvenanceFDEnv))
	if err != nil || fd != 4 {
		t.Fatalf("wrong inherited provenance FD: %d, %v", fd, err)
	}
	file := os.NewFile(uintptr(fd), "provenance")
	defer file.Close()
	envelope, err := io.ReadAll(io.LimitReader(file, rtconfig.MaxConfigProvenanceBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(os.Getenv("ORCH_CONFIG_PROVENANCE_PATH"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := rtconfig.LoadConfigBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Launch.PersistentSystemdPathDefault() || cfg.Launch.Plugin[0].PersistentSystemdPathDefault() {
		t.Fatal("ordinary config accepted trusted provenance")
	}
	if err := rtconfig.ApplyConfigProvenance(raw, cfg, envelope); err != nil {
		t.Fatalf("config provenance: %v", err)
	}
	if !cfg.Launch.PersistentSystemdPathDefault() || !cfg.Launch.Plugin[0].PersistentSystemdPathDefault() {
		t.Fatal("trusted handoff lost provenance")
	}
}

func TestTaskEnvironmentCannotSupplyConfigProvenance(t *testing.T) {
	t.Setenv(rtconfig.ConfigProvenanceFDEnv, "123")
	for _, env := range taskEnv(map[string]string{rtconfig.ConfigProvenanceFDEnv: "456", "MANIFEST_KEY": "key"}) {
		if strings.HasPrefix(env, rtconfig.ConfigProvenanceFDEnv+"=") {
			t.Fatalf("forwarded foreign descriptor: %s", env)
		}
	}
	cmd := exec.Command("/unused")
	cmd.Env = []string{rtconfig.ConfigProvenanceFDEnv + "=789", "OK=yes"}
	file, err := sandboxproc.AttachConfigProvenance(cmd, nil)
	if err != nil || file != nil || len(cmd.ExtraFiles) != 0 || len(cmd.Env) != 1 || cmd.Env[0] != "OK=yes" {
		t.Fatalf("unprojected launch inherited provenance: %+v, %v", cmd, err)
	}
}

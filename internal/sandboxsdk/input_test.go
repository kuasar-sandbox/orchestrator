package sandboxsdk

import (
	"io"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/sandbox"
)

func generatedSpec(t *testing.T) configsock.LaunchSpec {
	t.Helper()
	dir := t.TempDir()
	return configsock.LaunchSpec{Exec: "/opt/bin/sandbox-ctl", Workdir: dir, Args: []string{"run", "--sandbox-id", "sb-1", "--path-id", "sb-1", "--config", dir + "/sandbox.yaml", "--manifest-config", "", "--run-root", dir + "/run", "--base-root", dir + "/base", "--log-to", "journald=sandbox-ctl,SID=sb-1", "--stdout-to", "journald=sandbox-runner,SID=sb-1", "--stderr-to", "journald=sandbox-runner,SID=sb-1", "--console", "journald=sandbox-console,SID=sb-1"}}
}
func TestGeneratedInput(t *testing.T) {
	spec := generatedSpec(t)
	spec.Args = append(spec.Args, "--connect", "/tmp/exec.sock:127.0.0.1:22")
	env := map[string]string{"MANIFEST_KEY": "", "SANDBOX_CH_PATH": "/opt/bin/cloud-hypervisor"}
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file:///prepared.sandbox"}
	in, err := Parse(spec, source, map[string]string{"local": "file:///tmp"}, env, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	env["MANIFEST_KEY"] = "changed"
	if in.Env["MANIFEST_KEY"] != "" || in.Source != source || len(in.Forwards) != 1 || in.PathID != "sb-1" || in.RunRoot != spec.Workdir+"/run" {
		t.Fatalf("input bindings lost: %+v", in)
	}
}
func TestRejectUnexpectedInput(t *testing.T) {
	for _, arg := range []string{"--from", "--restore", "--cgroup-path", "--cgroup-adopt", "--ready-fd", "--tty", "--stats-json", "--unknown", "--config=other", "positional"} {
		t.Run(arg, func(t *testing.T) {
			spec := generatedSpec(t)
			spec.Args = append(spec.Args, arg, "x")
			if _, err := Parse(spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": ""}, "sb-1"); err == nil {
				t.Fatal("accepted unexpected flag")
			}
		})
	}
	for _, mutate := range []func(*configsock.LaunchSpec){func(s *configsock.LaunchSpec) { s.Exec = "/bin/sh" }, func(s *configsock.LaunchSpec) { s.Args = append(s.Args, "--config", "other") }, func(s *configsock.LaunchSpec) { s.Args = s.Args[:len(s.Args)-1] }, func(s *configsock.LaunchSpec) { s.Workdir = "relative" }} {
		spec := generatedSpec(t)
		mutate(&spec)
		if _, err := Parse(spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": ""}, "sb-1"); err == nil {
			t.Fatal("accepted malformed spec")
		}
	}
}
func TestInputIdentityAndSource(t *testing.T) {
	spec := generatedSpec(t)
	for _, source := range []types.ResumeSource{{Kind: types.ResumeSourceSandbox}, {Ref: "file:///x"}, {Kind: "other", Ref: "file:///x"}} {
		if _, err := Parse(spec, source, nil, map[string]string{"MANIFEST_KEY": ""}, "sb-1"); err == nil {
			t.Fatal("accepted invalid source")
		}
	}
	if _, err := Parse(spec, types.ResumeSource{}, nil, nil, "sb-1"); err == nil {
		t.Fatal("accepted missing authoritative env")
	}
	if _, err := Parse(spec, types.ResumeSource{}, nil, map[string]string{"MANIFEST_KEY": ""}, "other"); err == nil {
		t.Fatal("accepted identity mismatch")
	}
}
func TestReadinessProtocol(t *testing.T) {
	for _, tc := range []struct {
		name   string
		events []sandbox.ReadinessEvent
		want   string
	}{
		{"success", []sandbox.ReadinessEvent{sandbox.ReadinessControlReady, sandbox.ReadinessControlReady, sandbox.ReadinessRuntimeReady, sandbox.ReadinessReady, sandbox.ReadinessReady}, "control_ready\nready\n"},
		{"out of order", []sandbox.ReadinessEvent{sandbox.ReadinessReady, sandbox.ReadinessControlReady}, ""},
		{"failure", []sandbox.ReadinessEvent{sandbox.ReadinessControlReady}, "control_ready\n"},
		{"unknown", []sandbox.ReadinessEvent{"bad", sandbox.ReadinessControlReady}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := &captureCloser{}
			r := NewReadiness(w)
			for _, e := range tc.events {
				r.Notify(e)
			}
			if tc.name == "success" && w.closes != 1 {
				t.Fatal("ready did not close immediately")
			}
			_ = r.Close()
			_ = r.Close()
			if w.String() != tc.want || w.closes != 1 {
				t.Fatalf("got %q closes=%d", w.String(), w.closes)
			}
		})
	}
}

type captureCloser struct {
	strings.Builder
	closes int
}

func (w *captureCloser) Close() error { w.closes++; return nil }
func TestReadinessDisconnected(t *testing.T) {
	r, w := io.Pipe()
	_ = r.Close()
	ready := NewReadiness(w)
	ready.Notify(sandbox.ReadinessControlReady)
	ready.Notify(sandbox.ReadinessReady)
	if err := ready.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRelativeHostPathsUseTaskWorkdir(t *testing.T) {
	spec := generatedSpec(t)
	for i, arg := range spec.Args {
		switch arg {
		case "--stdout-to":
			spec.Args[i+1] = "stdout.log"
		case "--stderr-to":
			spec.Args[i+1] = "stderr.log"
		case "--console":
			spec.Args[i+1] = "file=console.log"
		}
	}
	spec.Args = append(spec.Args, "--connect", "exec.sock:127.0.0.1:22")
	in, err := Parse(spec, types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://source.sandbox"}, nil, map[string]string{"MANIFEST_KEY": ""}, "sb-1")
	if err != nil {
		t.Fatal(err)
	}
	if in.Stdio.Stdout.Path != spec.Workdir+"/stdout.log" || in.Stdio.Stderr.Path != spec.Workdir+"/stderr.log" || in.Stdio.Console.Path != spec.Workdir+"/console.log" || in.Forwards[0].UDSPath != spec.Workdir+"/exec.sock" {
		t.Fatal("relative output path lost task workdir")
	}
	ref, err := in.sourceRef()
	if err != nil || ref != "file://"+spec.Workdir+"/source.sandbox" {
		t.Fatalf("relative source=%q %v", ref, err)
	}
}

type partialWriter struct{ captureCloser }

func (w *partialWriter) Write(p []byte) (int, error) {
	if len(p) > 2 {
		p = p[:2]
	}
	return w.Builder.Write(p)
}
func (w *partialWriter) WriteString(p string) (int, error) { return w.Write([]byte(p)) }
func TestReadinessCompletesShortWrites(t *testing.T) {
	w := &partialWriter{}
	r := NewReadiness(w)
	r.Notify(sandbox.ReadinessControlReady)
	r.Notify(sandbox.ReadinessReady)
	if w.String() != "control_ready\nready\n" || w.closes != 1 {
		t.Fatalf("wire=%q closes=%d", w.String(), w.closes)
	}
}

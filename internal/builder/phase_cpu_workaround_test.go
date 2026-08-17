package builder

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func TestParsePhaseCPUMax(t *testing.T) {
	tests := []struct {
		name      string
		raw       string
		quota     uint64
		period    uint64
		unlimited bool
		wantErr   bool
	}{
		{name: "numeric", raw: "200000 100000\n", quota: 200000, period: 100000},
		{name: "unlimited", raw: "max 100000\n", period: 100000, unlimited: true},
		{name: "missing period", raw: "200000", wantErr: true},
		{name: "extra field", raw: "200000 100000 extra", wantErr: true},
		{name: "zero quota", raw: "0 100000", wantErr: true},
		{name: "zero period", raw: "200000 0", wantErr: true},
		{name: "invalid quota", raw: "nope 100000", wantErr: true},
		{name: "invalid period", raw: "200000 nope", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quota, period, unlimited, err := parsePhaseCPUMax([]byte(tt.raw))
			if (err != nil) != tt.wantErr || quota != tt.quota || period != tt.period || unlimited != tt.unlimited {
				t.Fatalf("parsePhaseCPUMax(%q) = %d, %d, %t, %v", tt.raw, quota, period, unlimited, err)
			}
		})
	}
}

func TestPhaseCPUMaxVCPUKickWorkaroundRelaxesAndRestores(t *testing.T) {
	sb, cpuMaxPath := phaseCPUFixture(t, 2, "200000 100000\n")
	if err := sb.relaxCPUMaxForVCPUKick(); err != nil {
		t.Fatal(err)
	}
	assertFileText(t, cpuMaxPath, "max 100000")
	if err := sb.relaxCPUMaxForVCPUKick(); err != nil {
		t.Fatalf("idempotent relax: %v", err)
	}
	if err := sb.restoreCPUMaxAfterVCPUKick(); err != nil {
		t.Fatal(err)
	}
	assertFileText(t, cpuMaxPath, "200000 100000")
	if err := sb.restoreCPUMaxAfterVCPUKick(); err != nil {
		t.Fatalf("idempotent restore: %v", err)
	}
}

func TestPhaseCPUMaxVCPUKickWorkaroundRejectsUnexpectedLimit(t *testing.T) {
	tests := []struct {
		name    string
		cpu     int
		initial string
		want    string
	}{
		{name: "already unlimited", cpu: 2, initial: "max 100000\n", want: "already unlimited"},
		{name: "quota mismatch", cpu: 2, initial: "100000 100000\n", want: "does not match capacity.cpu"},
		{name: "malformed", cpu: 2, initial: "broken\n", want: "expected two fields"},
		{name: "capacity missing", cpu: 0, initial: "200000 100000\n", want: "capacity.cpu is unavailable"},
		{name: "overflow", cpu: int(^uint(0) >> 1), initial: "200000 100000\n", want: "overflows"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sb, cpuMaxPath := phaseCPUFixture(t, tt.cpu, tt.initial)
			err := sb.relaxCPUMaxForVCPUKick()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("relax error = %v, want %q", err, tt.want)
			}
			assertFileText(t, cpuMaxPath, strings.TrimSpace(tt.initial))
		})
	}
}

func TestPhaseCPUMaxRestoreFailureRemainsFailClosed(t *testing.T) {
	sb, cpuMaxPath := phaseCPUFixture(t, 2, "200000 100000\n")
	if err := sb.relaxCPUMaxForVCPUKick(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(cpuMaxPath); err != nil {
		t.Fatal(err)
	}
	if err := sb.restoreCPUMaxAfterVCPUKick(); err == nil || !strings.Contains(err.Error(), "restore numeric limit") {
		t.Fatalf("restore error = %v", err)
	}
	if sb.cpuMax.original == "" {
		t.Fatal("failed restore discarded the original numeric limit")
	}
}

func TestPhaseSandboxTeardownRelaxesBeforeSignalAndRestoresAfterExit(t *testing.T) {
	sb, cpuMaxPath := phaseCPUFixture(t, 2, "200000 100000\n")
	dir := filepath.Dir(cpuMaxPath)
	observed := filepath.Join(dir, "observed")
	ready := filepath.Join(dir, "ready")
	cmd := exec.Command("bash", "-c", `
trap 'cat "$CPU_MAX_PATH" > "$OBSERVED_PATH"; exit 0' TERM
: > "$READY_PATH"
while :; do sleep 0.05; done
`)
	cmd.Env = append(os.Environ(),
		"CPU_MAX_PATH="+cpuMaxPath,
		"OBSERVED_PATH="+observed,
		"READY_PATH="+ready,
	)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	sb.cmd = cmd
	go func() {
		err := cmd.Wait()
		sb.waitMu.Lock()
		sb.waitErr = err
		sb.waitMu.Unlock()
		close(sb.done)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fake phase process did not become ready")
		}
		time.Sleep(time.Millisecond)
	}
	if err := sb.teardown(); err != nil {
		t.Fatal(err)
	}
	assertFileText(t, observed, "max 100000")
	assertFileText(t, cpuMaxPath, "200000 100000")
}

func phaseCPUFixture(t *testing.T, cpu int, initial string) (*phaseSandbox, string) {
	t.Helper()
	dir := t.TempDir()
	cpuMaxPath := filepath.Join(dir, "cpu.max")
	if err := os.WriteFile(cpuMaxPath, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cgroup.events"), []byte("populated 0\nfrozen 0\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fd, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fd.Close() })
	p := &buildPipeline{
		spec: &configsock.BuildSpec{Resources: rtconfig.ResourcesConfig{Capacity: rtconfig.CapacityConfig{CPU: cpu}}},
		log:  testBuilderLogger(), vmmCgroup: fd,
	}
	return &phaseSandbox{p: p, sid: "phase-test", cmd: &exec.Cmd{}, done: make(chan struct{})}, cpuMaxPath
}

func assertFileText(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != want {
		t.Fatalf("%s = %q, want %q", filepath.Base(path), got, want)
	}
}

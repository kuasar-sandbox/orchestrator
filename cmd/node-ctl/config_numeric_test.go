package main

import (
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

func TestBuiltinConductorRejectsInvalidAdmissionBeforeOpeningStore(t *testing.T) {
	for _, field := range []string{"rate", "burst"} {
		body := strings.Replace(numericConductorConfig("  admission: {"+field+": -1}\n"), "test-key", strings.Repeat("a", 64), 1)
		path := writeConductorConfig(t, body)
		if err := runConductor([]string{"--config", path}, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil || !strings.Contains(err.Error(), "resource_listen.admission."+field) {
			t.Fatalf("built-in %s validation=%v", field, err)
		}
	}
}

func numericConductorConfig(extra string) string {
	return `api: {domain: config.test}
encryption_key: test-key
sandbox:
  boot: {kernel: /kernel, runtime: /runtime}
resource_listen:
  enabled: true
` + extra
}

func TestResolvedConductorRejectsInvalidResourceNumbers(t *testing.T) {
	for _, tc := range []struct{ body, path string }{
		{"  admission: {rate: -1}\n", "resource_listen.admission.rate"},
		{"  admission: {burst: -1}\n", "resource_listen.admission.burst"},
		{"  resources: {physical_cpu: '1.5'}\n", "resource_listen.resources.physical_cpu"},
		{"  resources: {physical_cpu: '8junk'}\n", "resource_listen.resources.physical_cpu"},
		{"  resources: {physical_memory: 32GiB, host_reserved: {cpu: -.inf}}\n", "resource_listen.resources.host_reserved.cpu"},
		{"  resources: {physical_memory: 32GiB}\n  rate_limits: {memory_grant_per_sec_factor: 0.000000000001}\n", "resource_listen.rate_limits.memory_grant_per_sec_factor"},
	} {
		t.Run(tc.path+tc.body, func(t *testing.T) {
			path := writeConductorConfig(t, numericConductorConfig(tc.body))
			if _, err := renderConductorConfig(false, true, path); err == nil || !strings.Contains(err.Error(), tc.path) {
				t.Fatalf("resolve error=%v", err)
			}
		})
	}
}

func TestResolvedConductorKeepsCPUValuesAndAdmissionDefaults(t *testing.T) {
	for _, physical := range []string{"auto", "8", "18446744073709551"} {
		for _, reserve := range []string{"0", "1.5", "0.0009"} {
			path := writeConductorConfig(t, numericConductorConfig("  resources: {physical_memory: 32GiB, physical_cpu: '"+physical+"', host_reserved: {memory: 1GiB, cpu: "+reserve+"}}\n  admission: {rate: 0, burst: 0}\n"))
			before, err := config.LoadConductor(path)
			if err != nil {
				t.Fatal(err)
			}
			want, err := nodectl.Resolve(before.ResourceListen)
			if err != nil {
				t.Fatal(err)
			}
			out, err := renderConductorConfig(false, true, path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, out, 0600); err != nil {
				t.Fatal(err)
			}
			after, err := config.LoadConductor(path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := nodectl.Resolve(after.ResourceListen)
			if err != nil {
				t.Fatal(err)
			}
			if after.ResourceListen.Resources.PhysicalCPU != strconv.FormatUint(want.PhysicalCPU/1000, 10) || got.PhysicalCPU != want.PhysicalCPU || got.HostReserved != want.HostReserved || got.Admission.Rate != 4 || got.Admission.Burst != 16 {
				t.Fatalf("resolved numeric configuration changed: before=%+v after=%+v", want, got)
			}
			state := nodectl.NewPoolState(got)
			if state.NodeBudget.CPUMilli != got.PhysicalCPU || state.HostReserved.CPUMilli != got.HostReserved.CPUMilli {
				t.Fatal("rendered CPU differs from resource state")
			}
		}
	}
}

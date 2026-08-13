package nodectl

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func runRecoveryControllerHelper(t *testing.T) {
	socket, root := os.Getenv("NODECTL_SOCKET"), os.Getenv("NODECTL_CGROUP_ROOT")
	state := NewState(530<<20, 4000, 0, 0, Watermarks{
		HighFactor: .85, LowFactor: .7, EmergencyFactor: .05, StartupFactor: .75,
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, StartupTTL: time.Minute, QueueTTL: time.Minute, QueueMaxDepth: 16,
	})
	srv := &Server{
		Path: socket, State: state, Admission: admission,
		Allocator: NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1 << 30, MinGrantStep: 1}),
		Inventory: &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{root}, Pool: state.AllocatablePool},
	}
	if phase := os.Getenv("NODECTL_CRASH_PHASE"); phase != "" {
		srv.phaseHook = func(got string) {
			if got != phase {
				return
			}
			_, _ = fmt.Fprintln(os.Stdout, "phase:"+got)
			select {}
		}
	}
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	admission.SetWiring(state, nil, func(string, ...any) {}, func(p *PendingAdmit) (*Message, error) {
		return srv.BuildAdmitOKFromQueue(p)
	})
	admission.Run()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	_, _ = fmt.Fprintln(os.Stdout, "ready")
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	cancel()
	<-done
	admission.Stop()
}

func TestRecoveryControllerProcessHelper(t *testing.T) {
	if os.Getenv("NODECTL_TEST_HELPER") != "recovery-controller" {
		return
	}
	runRecoveryControllerHelper(t)
}

type recoveryControllerProcess struct {
	cmd   *exec.Cmd
	stdin *bufio.Writer
	lines chan string
}

func startRecoveryController(t *testing.T, socket, root, phase string) *recoveryControllerProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRecoveryControllerProcessHelper$")
	cmd.Env = append(os.Environ(), "NODECTL_TEST_HELPER=recovery-controller",
		"NODECTL_SOCKET="+socket, "NODECTL_CGROUP_ROOT="+root, "NODECTL_CRASH_PHASE="+phase)
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	p := &recoveryControllerProcess{cmd: cmd, stdin: bufio.NewWriter(in), lines: make(chan string, 8)}
	go func() {
		scanner := bufio.NewScanner(out)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
	}()
	if line := p.next(t); line != "ready" {
		output := []string{line}
		for line := range p.lines {
			output = append(output, line)
		}
		_ = cmd.Wait()
		t.Fatalf("controller failed before ready:\n%s", strings.Join(output, "\n"))
	}
	return p
}

func (p *recoveryControllerProcess) next(t *testing.T) string {
	t.Helper()
	select {
	case line, ok := <-p.lines:
		if !ok {
			t.Fatal("controller output closed")
		}
		return line
	case <-time.After(5 * time.Second):
		t.Fatal("controller output timeout")
		return ""
	}
}

func (p *recoveryControllerProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Wait(); err == nil || !strings.Contains(err.Error(), "signal: killed") {
		t.Fatalf("controller SIGKILL wait = %v", err)
	}
}

func (p *recoveryControllerProcess) stop(t *testing.T) {
	t.Helper()
	_, _ = p.stdin.WriteString("stop\n")
	_ = p.stdin.Flush()
	if err := p.cmd.Wait(); err != nil {
		t.Fatalf("controller stop = %v", err)
	}
}

func recoverySandboxConfig(socket, cgroup string) *rtconfig.SandboxConfig {
	cfg := &rtconfig.SandboxConfig{}
	cfg.Resources.Capacity = rtconfig.CapacityConfig{CPU: 1, Memory: "512MiB"}
	cfg.Resources.Allocatable = rtconfig.AllocatableConfig{CPU: .5, Memory: "128MiB"}
	cfg.Resources.Startup = &rtconfig.StartupConfig{Memory: "256MiB"}
	cfg.Resources.Control.Controller = socket
	cfg.Resources.Control.CgroupPath = cgroup
	cfg.ApplyDefaults()
	return cfg
}

func waitRecoveryHooksConnected(t *testing.T, hooks *resctl.ControllerHooks) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !hooks.Connected() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !hooks.Connected() {
		t.Fatal("sandbox controller hooks did not reconnect")
	}
}

func assertRecoveredAllocation(t *testing.T, socket, sid string, want uint64) {
	t.Helper()
	client := &resource.Client{SocketPath: socket}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	reservations, err := client.AdminList()
	if err != nil {
		t.Fatal(err)
	}
	if len(reservations) != 1 || reservations[0].SandboxID != sid || reservations[0].Provisional ||
		!reservations[0].Connected || reservations[0].AllocatableMemory != want {
		t.Fatalf("recovered reservations = %+v, want one precise connected allocation %d", reservations, want)
	}
}

func TestControllerSIGKILLRecoveryAroundAdmitAndGrantResponses(t *testing.T) {
	for _, tc := range []struct {
		name, rpc, phase string
		want             uint64
	}{
		{name: "admit-before-response", rpc: resource.TypeAdmit, phase: "before_response:" + resource.TypeAdmit, want: 256 << 20},
		{name: "admit-after-response", rpc: resource.TypeAdmit, phase: "after_response:" + resource.TypeAdmit, want: 256 << 20},
		{name: "grant-before-response", rpc: resource.TypeRequestBudget, phase: "before_response:" + resource.TypeRequestBudget, want: 256 << 20},
		{name: "grant-after-response", rpc: resource.TypeRequestBudget, phase: "after_response:" + resource.TypeRequestBudget, want: 320 << 20},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, err := os.MkdirTemp("/tmp", "nodectl-173-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			socket, root, sid := filepath.Join(dir, "controller.sock"), filepath.Join(dir, "cgroups"), "live-sandbox"
			writeFakeCgroupRoot(t, root)
			cgroup := filepath.Join(root, "vmm")
			writeFakeCgroup(t, cgroup, "536870912", os.Getpid())
			if err := os.WriteFile(filepath.Join(cgroup, "memory.high"), []byte("max\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(cgroup, "memory.current"), []byte("268435456\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg := recoverySandboxConfig(socket, cgroup)
			hooks, err := resctl.NewControllerHooks(resctl.ControllerHookOptions{
				SocketPath: socket, CgroupPath: cgroup, SandboxID: sid,
				Context: context.Background(), Logf: t.Logf,
			}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			hooksReleased := false
			defer func() {
				if !hooksReleased {
					hooks.Release("test")
				}
			}()

			crashing := startRecoveryController(t, socket, root, tc.phase)
			if tc.rpc == resource.TypeAdmit {
				admitDone := make(chan error, 1)
				go func() {
					_, err := hooks.Admit(sid, 0)
					admitDone <- err
				}()
				if line := crashing.next(t); line != "phase:"+tc.phase {
					t.Fatalf("crash phase = %q", line)
				}
				if strings.HasPrefix(tc.phase, "after_response:") {
					if err := <-admitDone; err != nil {
						t.Fatal(err)
					}
				}
				crashing.kill(t)
				replacement := startRecoveryController(t, socket, root, "")
				if strings.HasPrefix(tc.phase, "before_response:") {
					select {
					case err := <-admitDone:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("Admit did not retry after controller replacement")
					}
				} else if err := hooks.Settled(); err != nil {
					t.Fatal(err)
				}
				waitRecoveryHooksConnected(t, hooks)
				assertRecoveredAllocation(t, socket, sid, tc.want)
				hooks.Release("test")
				hooksReleased = true
				replacement.stop(t)
				return
			}

			if _, err := hooks.Admit(sid, 0); err != nil {
				t.Fatal(err)
			}
			if err := hooks.Settled(); err != nil {
				t.Fatal(err)
			}
			grantDone := make(chan error, 1)
			go func() {
				_, _, _, err := hooks.RequestBudget(64<<20, resource.UrgencyNormal, "fault-window")
				grantDone <- err
			}()
			if line := crashing.next(t); line != "phase:"+tc.phase {
				t.Fatalf("crash phase = %q", line)
			}
			if strings.HasPrefix(tc.phase, "after_response:") {
				if err := <-grantDone; err != nil {
					t.Fatal(err)
				}
			}
			crashing.kill(t)
			if strings.HasPrefix(tc.phase, "before_response:") {
				if err := <-grantDone; err == nil || !resource.IsTransportError(err) {
					t.Fatalf("pre-response grant error = %v", err)
				}
			} else {
				if _, _, _, err := hooks.RequestBudget(1, resource.UrgencyNormal, "detect-drop"); err == nil || !resource.IsTransportError(err) {
					t.Fatalf("drop detection error = %v", err)
				}
			}
			replacement := startRecoveryController(t, socket, root, "")
			waitRecoveryHooksConnected(t, hooks)
			if hooks.AllocatableNowMem() != tc.want {
				t.Fatalf("sandbox applied allocation = %d, want %d", hooks.AllocatableNowMem(), tc.want)
			}
			assertRecoveredAllocation(t, socket, sid, tc.want)
			hooks.Release("test")
			hooksReleased = true
			replacement.stop(t)
		})
	}
}

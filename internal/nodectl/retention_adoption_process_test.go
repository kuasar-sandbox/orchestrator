package nodectl

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

func startHoldingRetentionProcess(t *testing.T, dir, socket string) *retentionClientProcess {
	t.Helper()
	source := startRetentionClient(t, "source", dir, socket)
	source.run(t, "prepare")
	source.run(t, "commit_capture")
	source.run(t, "close_runtime")
	source.run(t, "retire")
	waitRetainedCharge(t, socket, 256<<20, 0)
	return source
}

func waitRetentionStage(t *testing.T, socket, want string) *retainedReceipt {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		r, err := readRetainedReceipt(retentionPath(socket, "retained-sid"))
		if err == nil && r.Stage == want {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("retention did not reach %s: %v", want, err)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRetainedMemoryRPCAbandonedAdoptionWithoutRestart(t *testing.T) {
	for _, phase := range []string{"retention_intent", "retention_admitted", "retention_published"} {
		t.Run(phase, func(t *testing.T) {
			t.Setenv("NODECTL_TEST_PHYSICAL", "4294967296")
			t.Setenv("NODECTL_SWEEP_INTERVAL", "20ms")
			dir := shortTestDir(t)
			root, socket := filepath.Join(dir, "cgroups"), filepath.Join(dir, "ctl.sock")
			marker := filepath.Join(dir, "continue")
			t.Setenv("NODECTL_PHASE_CONTINUE", marker)
			writeFakeCgroupRoot(t, root)
			controller := startRecoveryController(t, socket, root, phase)
			source := startHoldingRetentionProcess(t, dir, socket)
			failed := startRetentionClient(t, "successor", dir, socket)
			failed.send(t, "admit")
			if got := controller.next(t); got != "phase:"+phase {
				t.Fatalf("adoption phase: %s", got)
			}
			before := waitRetentionStage(t, socket, retentionAdopting)
			if phase == "retention_published" {
				// Model publication succeeding but active receipt persistence
				// failing. Without this fault the server legitimately completes
				// active and normal execution cleanup owns the later exit.
				path := retentionPath(socket, "retained-sid")
				if err := os.Rename(path, path+".saved"); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			if err := failed.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			if err := failed.cmd.Wait(); err == nil {
				t.Fatal("failed candidate did not exit")
			}
			if err := os.WriteFile(marker, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if phase == "retention_published" {
				// EOF must detach the connection even though no token reached
				// the failed client. Only then restore writable inventory.
				client := &resource.Client{SocketPath: socket}
				if err := client.Connect(); err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				deadline := time.Now().Add(5 * time.Second)
				for {
					rows, err := client.AdminList()
					if err != nil {
						t.Fatal(err)
					}
					if len(rows) == 1 && !rows[0].Connected {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("publication error left a phantom live connection")
					}
					time.Sleep(time.Millisecond)
				}
				_ = client.Close()
				path := retentionPath(socket, "retained-sid")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(path+".saved", path); err != nil {
					t.Fatal(err)
				}
			}
			// Only the production periodic sweeper runs here: no new Admit and
			// no controller restart are needed to recover the failed attempt.
			waitRetentionStage(t, socket, retentionHolding)
			waitRetainedCharge(t, socket, 256<<20, 0)
			retry := startRetentionClient(t, "successor", dir, socket)
			retry.run(t, "admit")
			after := waitRetentionStage(t, socket, retentionActive)
			if after.Handoff.ID == before.Handoff.ID || after.OwnerLease.PID != retry.cmd.Process.Pid {
				t.Fatal("fresh process did not obtain a distinct handoff")
			}
			waitRetainedCharge(t, socket, 512<<20, 1)
			source.run(t, "ram_released")
			source.run(t, "release_retained")
			waitRetainedCharge(t, socket, 256<<20, 1)
			source.run(t, "close")
			retry.run(t, "close")
			waitRetainedCharge(t, socket, 0, 0)
			controller.stop(t)
		})
	}
}

func TestRetainedMemoryRPCSamePIDClosedCandidateRetries(t *testing.T) {
	t.Setenv("NODECTL_TEST_PHYSICAL", "4294967296")
	dir := shortTestDir(t)
	root, socket := filepath.Join(dir, "cgroups"), filepath.Join(dir, "ctl.sock")
	marker := filepath.Join(dir, "continue")
	t.Setenv("NODECTL_PHASE_CONTINUE", marker)
	writeFakeCgroupRoot(t, root)
	controller := startRecoveryController(t, socket, root, "retention_admitted")
	source := startHoldingRetentionProcess(t, dir, socket)
	capture := waitRetentionStage(t, socket, retentionHolding).Capture
	cgroup := filepath.Join(root, "embedded-sdk")
	writeFakeCgroup(t, cgroup, "536870912", os.Getpid())
	cfg := recoverySandboxConfig(socket, cgroup)
	newHooks := func() *resctl.ControllerHooks {
		h, err := resctl.NewControllerHooks(resctl.ControllerHookOptions{SocketPath: socket, SandboxID: "retained-sid",
			CgroupPath: cgroup, Context: context.Background(), Retained: &capture}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = h.CloseRetained() })
		return h
	}
	failed := newHooks()
	admitDone := make(chan error, 1)
	go func() { _, err := failed.Admit("retained-sid", 256<<20); admitDone <- err }()
	if got := controller.next(t); got != "phase:retention_admitted" {
		t.Fatalf("SDK adoption phase: %s", got)
	}
	before := waitRetentionStage(t, socket, retentionAdopting)
	if err := failed.CloseRetained(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-admitDone:
		if err == nil {
			t.Fatal("cancelled SDK admission succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("closing failed SDK did not interrupt its Admit")
	}
	if err := os.WriteFile(marker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// No periodic sweeper in this controller: the next Admit must recover
	// the ended candidate synchronously while this exact PID is still alive.
	retry := newHooks()
	if _, err := retry.Admit("retained-sid", 256<<20); err != nil {
		t.Fatal(err)
	}
	after := waitRetentionStage(t, socket, retentionActive)
	if after.Handoff.ID == before.Handoff.ID || after.OwnerLease.PID != os.Getpid() {
		t.Fatal("same process did not receive a fresh owner session")
	}
	waitRetainedCharge(t, socket, 512<<20, 1)
	source.run(t, "ram_released")
	source.run(t, "release_retained")
	source.run(t, "close")
	if err := retry.CloseRetained(); err != nil {
		t.Fatal(err)
	}
	waitRetainedCharge(t, socket, 0, 0)
	controller.stop(t)
}

func TestRetainedMemoryRPCPublishedACKLossWithoutRestart(t *testing.T) {
	t.Setenv("NODECTL_TEST_PHYSICAL", "4294967296")
	t.Setenv("NODECTL_DROP_ADOPT_ACK", "1")
	dir := shortTestDir(t)
	root, socket := filepath.Join(dir, "cgroups"), filepath.Join(dir, "ctl.sock")
	writeFakeCgroupRoot(t, root)
	controller := startRecoveryController(t, socket, root, "")
	source := startHoldingRetentionProcess(t, dir, socket)
	successor := startRetentionClient(t, "successor", dir, socket)
	successor.send(t, "admit")
	if got := controller.next(t); got != "dropped:adopt-ack" {
		t.Fatalf("ACK fault: %s", got)
	}
	before := waitRetentionStage(t, socket, retentionActive)
	if got := successor.next(t); got != "ok:admit" {
		t.Fatalf("ACK-loss replay: %s", got)
	}
	after := waitRetentionStage(t, socket, retentionActive)
	if after.Handoff.ID != before.Handoff.ID || after.OwnerFile != before.OwnerFile || after.OwnerToken != before.OwnerToken {
		t.Fatal("ACK-loss replay replaced a live published owner")
	}
	waitRetainedCharge(t, socket, 512<<20, 1)
	if pid, locked, err := resource.LeaseLockOwner(resource.LeasePath(socket, "retained-sid")); err != nil || !locked || pid != successor.cmd.Process.Pid {
		t.Fatalf("published successor lock: %d %v %v", pid, locked, err)
	}
	source.run(t, "ram_released")
	source.run(t, "release_retained")
	source.run(t, "close")
	successor.run(t, "close")
	waitRetainedCharge(t, socket, 0, 0)
	controller.stop(t)
}

package sandboxproc

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStartReadinessEOFBeforeChildExit(t *testing.T) {
	vmm, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer vmm.Close()
	readyR, readyW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyR.Close()
	releaseR, releaseW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer releaseR.Close()
	defer releaseW.Close()
	extra, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStartChild$", "--")
	cmd.Env = append(os.Environ(), "SANDBOXPROC_TEST_CHILD=1")
	cmd.ExtraFiles = []*os.File{extra}
	cmd.Stdin = releaseR
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := Start(cmd, vmm, readyW); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	joined := false
	defer func() {
		if !joined {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	if _, err := readyW.Write([]byte("parent")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("parent readiness remains open: %v", err)
	}
	if _, err := vmm.Stat(); err != nil {
		t.Fatalf("borrowed cgroup closed: %v", err)
	}
	if err := readyR.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	wire, err := io.ReadAll(readyR)
	if err != nil {
		t.Fatalf("readiness EOF while child lives: %v", err)
	}
	if string(wire) != "control_ready\nready\n" {
		t.Fatalf("wire %q", wire)
	}
	select {
	case err := <-done:
		joined = true
		t.Fatalf("child exited before release: %v", err)
	default:
	}
	if _, err := releaseW.Write([]byte("release\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		joined = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child did not exit after release")
	}
}

func TestStartFailureClosesReadiness(t *testing.T) {
	for _, name := range []string{"missing-command", "nil-command", "empty-command", "nil-vmm", "closed-vmm", "owned-option"} {
		t.Run(name, func(t *testing.T) {
			vmm, err := os.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer vmm.Close()
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			cmd := exec.Command("/nonexistent-sandboxproc-test")
			switch name {
			case "nil-command":
				cmd = nil
			case "empty-command":
				cmd = &exec.Cmd{}
			case "nil-vmm":
				vmm.Close()
				vmm = nil
			case "closed-vmm":
				vmm.Close()
			case "owned-option":
				cmd.Args = append(cmd.Args, "--ready-fd=99")
			}
			if err := Start(cmd, vmm, w); err == nil {
				t.Fatal("expected failure")
			}
			if cmd != nil && cmd.Process != nil {
				t.Fatal("unexpected child")
			}
			if err := r.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			wire, err := io.ReadAll(r)
			if err != nil || len(wire) != 0 {
				t.Fatalf("failure EOF %q: %v", wire, err)
			}
		})
	}
}

// Re-exec the real test binary: the child closes its readiness descriptor and
// remains alive on stdin, distinguishing readiness completion from child exit.
func TestStartChild(t *testing.T) {
	if os.Getenv("SANDBOXPROC_TEST_CHILD") != "1" {
		return
	}
	vmmFD, readyFD := -1, -1
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, "--cgroup-path=fd=") {
			vmmFD, _ = strconv.Atoi(strings.TrimPrefix(arg, "--cgroup-path=fd="))
		}
		if strings.HasPrefix(arg, "--ready-fd=") {
			readyFD, _ = strconv.Atoi(strings.TrimPrefix(arg, "--ready-fd="))
		}
	}
	if vmmFD != 4 || readyFD != 5 {
		t.Fatalf("child descriptors %d/%d", vmmFD, readyFD)
	}
	vmm := os.NewFile(uintptr(vmmFD), "vmm")
	if info, err := vmm.Stat(); err != nil || !info.IsDir() {
		t.Fatalf("child cgroup: %v", err)
	}
	ready := os.NewFile(uintptr(readyFD), "ready")
	if _, err := io.WriteString(ready, "control_ready\nready\n"); err != nil {
		t.Fatal(err)
	}
	if err := ready.Close(); err != nil {
		t.Fatal(err)
	}
	var release string
	if _, err := fmt.Fscan(os.Stdin, &release); err != nil || release != "release" {
		t.Fatalf("release %q: %v", release, err)
	}
}

func TestStartRequiresOpenReadiness(t *testing.T) {
	for _, name := range []string{"nil", "closed"} {
		t.Run(name, func(t *testing.T) {
			vmm, err := os.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer vmm.Close()
			var ready *os.File
			if name == "closed" {
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				r.Close()
				w.Close()
				ready = w
			}
			cmd := exec.Command(os.Args[0])
			if err := Start(cmd, vmm, ready); err == nil {
				t.Fatal("missing readiness accepted")
			}
			if cmd.Process != nil {
				t.Fatal("child unexpectedly started")
			}
			if _, err := vmm.Stat(); err != nil {
				t.Fatalf("borrowed descriptor closed: %v", err)
			}
		})
	}
}

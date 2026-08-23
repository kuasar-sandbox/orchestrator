//go:build linux

package nodectl

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const (
	hotPathHelperEnv = "NODECTL_TEST_HELPER_HOT_PATH"
	hotPathBegin     = "NODECTL-HOT-PATH-BEGIN"
	hotPathEnd       = "NODECTL-HOT-PATH-END"
)

func runHotPathTraceHelper(t *testing.T) {
	socket := os.Getenv("NODECTL_HOT_PATH_SOCKET")
	if socket == "" {
		t.Fatal("NODECTL_HOT_PATH_SOCKET is empty")
	}
	state := NewState(4<<30, 4000, 0, 0, Watermarks{
		HighFactor: .85, LowFactor: .70, EmergencyFactor: .05, StartupFactor: .50,
	})
	admission := NewAdmissionController(AdmissionPolicy{
		Rate: 100, Burst: 100, StartupTTL: time.Minute,
		QueueTTL: time.Minute, QueueMaxDepth: 16,
	})
	server := &Server{
		Path: socket, State: state, Admission: admission,
		Allocator: NewAllocator(AllocatorPolicy{
			MemoryGrantPerSecBytes: 1 << 30,
			MinGrantStep:           1,
			MaxGrantStep:           1 << 30,
		}),
	}
	if err := server.Listen(); err != nil {
		t.Fatal(err)
	}
	admission.SetWiring(state, nil, func(string, ...any) {}, server.BuildAdmitOKFromQueue)
	admission.Run()
	defer admission.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = server.Serve(ctx)
		close(done)
	}()
	_, _ = fmt.Fprintln(os.Stdout, hotPathBegin)
	_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
	_, _ = fmt.Fprintln(os.Stdout, hotPathEnd)
	cancel()
	<-done
}

func readTraceMarker(t *testing.T, scanner *bufio.Scanner, want string) {
	t.Helper()
	lines := make(chan string, 1)
	go func() {
		for scanner.Scan() {
			if scanner.Text() == want {
				lines <- want
				return
			}
		}
		lines <- ""
	}()
	select {
	case got := <-lines:
		if got != want {
			t.Fatalf("trace helper exited before %s", want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timeout waiting for trace marker %s", want)
	}
}

func traceHotPathRPCs(t *testing.T, socket string) {
	t.Helper()
	client := &Client{SocketPath: socket}
	if err := client.Connect(); err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	admitted, err := client.Admit(AdmitParams{
		SandboxID: "strace-hot-path", CapacityMemoryBytes: 512 << 20,
		FloorMemoryBytes: 64 << 20, StartupBudgetMemory: 128 << 20,
	})
	if err != nil || admitted.Status != StatusAdmitted {
		t.Fatalf("Admit = %+v, %v", admitted, err)
	}
	if err := client.Settled(64<<20, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Heartbeat(64<<20, 2, 0, 0); err != nil {
		t.Fatal(err)
	}
	granted, current, _, err := client.RequestBudget(
		admitted.GrantedInitialAlloc, 32<<20, UrgencyNormal, "strace",
	)
	if err != nil || granted == 0 {
		t.Fatalf("RequestBudget = granted %d current %d, %v", granted, current, err)
	}
	if err := client.OOMReport(1, 123, 64<<20); err != nil {
		t.Fatal(err)
	}
	if err := client.Release("strace-complete"); err != nil {
		t.Fatal(err)
	}
}

func hotPathTraceSegment(t *testing.T, trace string) string {
	t.Helper()
	begin := strings.Index(trace, hotPathBegin)
	if begin < 0 {
		t.Fatalf("strace output has no begin marker:\n%s", trace)
	}
	beginLineEnd := strings.IndexByte(trace[begin:], '\n')
	if beginLineEnd < 0 {
		t.Fatal("strace begin marker has no line ending")
	}
	begin += beginLineEnd + 1
	end := strings.Index(trace[begin:], hotPathEnd)
	if end < 0 {
		t.Fatalf("strace output has no end marker:\n%s", trace)
	}
	end += begin
	endLineStart := strings.LastIndexByte(trace[:end], '\n')
	if endLineStart < begin {
		t.Fatal("strace hot-path interval is empty")
	}
	return trace[begin:endLineStart]
}

func assertNoHotPathFileIO(t *testing.T, segment string) {
	t.Helper()
	for _, syscall := range []string{
		"open(", "openat(", "openat2(", "creat(",
		"rename(", "renameat(", "renameat2(",
		"fsync(", "fdatasync(", "unlink(", "unlinkat(",
	} {
		if strings.Contains(segment, syscall) {
			t.Fatalf("resource RPC hot path executed %s:\n%s", syscall, segment)
		}
	}
	for _, line := range strings.Split(segment, "\n") {
		if !strings.Contains(line, "write(") &&
			!strings.Contains(line, "writev(") &&
			!strings.Contains(line, "pwrite64(") {
			continue
		}
		// RPC replies use Unix sockets. The only other writes in the marked
		// interval are the helper's pipe markers. A regular-file descriptor
		// would be rendered as a pathname by strace -yy and fails this gate.
		if strings.Contains(line, "<UNIX") || strings.Contains(line, "<pipe:") ||
			strings.Contains(line, "<socket:[") ||
			strings.Contains(line, "<(null):[") ||
			strings.Contains(line, "<anon_inode:[eventfd]>") {
			continue
		}
		t.Fatalf("resource RPC hot path wrote a non-socket file descriptor:\n%s", line)
	}
}

func TestResourceRPCHotPathDoesNotPerformFileIO(t *testing.T) {
	if os.Getenv(hotPathHelperEnv) == "1" {
		runHotPathTraceHelper(t)
		return
	}
	strace, err := exec.LookPath("strace")
	if err != nil {
		t.Skip("strace is not installed")
	}
	dir := t.TempDir()
	socket := filepath.Join(dir, "controller.sock")
	tracePath := filepath.Join(dir, "strace.log")
	cmd := exec.Command(strace,
		"-f", "-qq", "-yy", "-s", "256", "-o", tracePath,
		"-e", "trace=open,openat,openat2,creat,rename,renameat,renameat2,fsync,fdatasync,write,pwrite64,writev,unlink,unlinkat",
		"--", os.Args[0], "-test.run=^TestResourceRPCHotPathDoesNotPerformFileIO$",
	)
	cmd.Env = append(os.Environ(), hotPathHelperEnv+"=1", "NODECTL_HOT_PATH_SOCKET="+socket)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if waited {
			return
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})
	scanner := bufio.NewScanner(stdout)
	readTraceMarker(t, scanner, hotPathBegin)
	traceHotPathRPCs(t, socket)
	if _, err := fmt.Fprintln(stdin, "done"); err != nil {
		t.Fatal(err)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	readTraceMarker(t, scanner, hotPathEnd)
	err = cmd.Wait()
	waited = true
	if err != nil {
		t.Fatalf("strace helper: %v: %s", err, stderr.String())
	}
	data, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	assertNoHotPathFileIO(t, hotPathTraceSegment(t, string(data)))
}

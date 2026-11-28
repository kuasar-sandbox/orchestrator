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
	"strconv"
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
	admission.SetWiring(state, nil, server.BuildAdmitOKFromQueue)
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

func isLegacyUnixSocketAnnotation(line, socket string) bool {
	const prefix = "<(null):["
	rest, ok := hotPathWriteArgs(line)
	if !ok {
		return false
	}
	fdStart := 0
	start := strings.Index(rest, prefix)
	if start < 0 {
		return false
	}
	if _, err := strconv.ParseUint(strings.TrimSpace(rest[fdStart:start]), 10, 64); err != nil {
		return false
	}
	annotation := rest[start+len(prefix):]
	end := strings.Index(annotation, "]>")
	if end < 0 {
		return false
	}
	if remainder := annotation[end+2:]; !strings.HasPrefix(remainder, ",") {
		return false
	}
	annotation = annotation[:end]
	pathSuffix := `,"` + socket + `"`
	if !strings.HasSuffix(annotation, pathSuffix) {
		return false
	}
	inodes := strings.TrimSuffix(annotation, pathSuffix)
	local, peer, ok := strings.Cut(inodes, "->")
	if !ok || strings.Contains(peer, "->") {
		return false
	}
	if _, err := strconv.ParseUint(local, 10, 64); err != nil {
		return false
	}
	if _, err := strconv.ParseUint(peer, 10, 64); err != nil {
		return false
	}
	return true
}

// hotPathWriteArgs returns the argument string following the write-family
// syscall invocation at the start of an strace record (e.g. `474607 writev(`),
// or ok=false when the record does not start with write, writev, or pwrite64.
// Anchoring to the record start keeps a quoted payload that merely mentions a
// syscall (e.g. a regular-file pwrite64 whose payload contains
// `write(6<{eventfd-count=0}>,`) from being parsed as the target descriptor.
func hotPathWriteArgs(line string) (args string, ok bool) {
	rest := strings.TrimLeft(line, "0123456789 ")
	for _, syscall := range []string{"writev(", "pwrite64(", "write("} {
		if strings.HasPrefix(rest, syscall) {
			return rest[len(syscall):], true
		}
	}
	return "", false
}

// isEventfdAnnotation reports whether the write target's descriptor
// annotation is an eventfd, covering both the legacy
// <anon_inode:[eventfd]> rendering and the strace >= 6.x extended form,
// e.g. write(5<{eventfd-count=0, eventfd-id=112, eventfd-semaphore=0}>, ...).
// Only the descriptor annotation of the record's own syscall is inspected, so
// a regular-file write whose payload merely contains "<{eventfd" or even a
// full `write(...<{eventfd...}>` fragment cannot bypass the gate.
func isEventfdAnnotation(line string) bool {
	rest, ok := hotPathWriteArgs(line)
	if !ok {
		return false
	}
	i := 0
	for i < len(rest) && rest[i] >= '0' && rest[i] <= '9' {
		i++
	}
	if i == 0 || i >= len(rest) || rest[i] != '<' {
		return false
	}
	annotation := rest[i+1:]
	end := strings.IndexByte(annotation, '>')
	if end < 0 {
		return false
	}
	annotation = annotation[:end]
	return annotation == "anon_inode:[eventfd]" ||
		strings.HasPrefix(annotation, "{eventfd")
}

func isAllowedHotPathWrite(line, socket string) bool {
	return strings.Contains(line, "<UNIX") ||
		strings.Contains(line, "<pipe:") ||
		strings.Contains(line, "<socket:[") ||
		isEventfdAnnotation(line) ||
		isLegacyUnixSocketAnnotation(line, socket)
}

func assertNoHotPathFileIO(t *testing.T, segment, socket string) {
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
		if isAllowedHotPathWrite(line, socket) {
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
	assertNoHotPathFileIO(t, hotPathTraceSegment(t, string(data)), socket)
}

func TestLegacyUnixSocketAnnotation(t *testing.T) {
	const socket = "/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"
	for _, tt := range []struct {
		name string
		line string
		want bool
	}{
		{
			name: "legacy exact socket",
			line: `123 write(7<(null):[12708690->12710563,"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"]>, "ok", 2) = 2`,
			want: true,
		},
		{
			name: "different socket",
			line: `123 write(7<(null):[12708690->12710563,"/tmp/other/controller.sock"]>, "ok", 2) = 2`,
		},
		{
			name: "regular file pathname",
			line: `123 write(7</tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock>, "ok", 2) = 2`,
		},
		{
			name: "annotation only in regular file payload",
			line: `123 write(7</tmp/output>, "<(null):[12708690->12710563,\"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock\"]>", 96) = 96`,
		},
		{
			name: "pwrite64 annotation only in regular file payload",
			line: `123 pwrite64(7</tmp/output>, "write(7<(null):[12708690->12710563,\"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock\"]>", 96, 0) = 96`,
		},
		{
			name: "nonnumeric file descriptor",
			line: `123 write(fd<(null):[12708690->12710563,"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"]>, "ok", 2) = 2`,
		},
		{
			name: "missing descriptor boundary",
			line: `123 write(7<(null):[12708690->12710563,"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"]> "ok", 2) = 2`,
		},
		{
			name: "missing peer inode",
			line: `123 write(7<(null):[12708690,"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"]>, "ok", 2) = 2`,
		},
		{
			name: "nonnumeric inode",
			line: `123 write(7<(null):[socket->12710563,"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"]>, "ok", 2) = 2`,
		},
		{
			name: "extra inode arrow",
			line: `123 write(7<(null):[1->2->3,"/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"]>, "ok", 2) = 2`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := isLegacyUnixSocketAnnotation(tt.line, socket); got != tt.want {
				t.Fatalf("isLegacyUnixSocketAnnotation() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestAllowedHotPathWriteEventfd(t *testing.T) {
	const socket = "/tmp/TestResourceRPCHotPathDoesNotPerformFileIO/001/controller.sock"
	for _, tt := range []struct {
		name string
		line string
		want bool
	}{
		{
			name: "legacy eventfd annotation",
			line: `474607 write(5<anon_inode:[eventfd]>, "\1\0\0\0\0\0\0\0", 8) = 8`,
			want: true,
		},
		{
			name: "extended eventfd annotation via writev",
			line: `474607 writev(5<anon_inode:[eventfd]>, [{iov_base="\1\0\0\0\0\0\0\0", iov_len=8}], 1) = 8`,
			want: true,
		},
		{
			name: "extended eventfd annotation via pwrite64",
			line: `474607 pwrite64(5<{eventfd-count=0, eventfd-id=112, eventfd-semaphore=0}>, "\1\0\0\0\0\0\0\0", 8, 0) = 8`,
			want: true,
		},
		{
			name: "regular file still rejected",
			line: `474607 write(5</tmp/output>, "\1\0\0\0\0\0\0\0", 8) = 8`,
			want: false,
		},
		{
			name: "regular file with eventfd text in payload still rejected",
			line: `474607 write(5</tmp/output>, "<{eventfd", 9) = 9`,
			want: false,
		},
		{
			name: "pwrite64 to regular file with eventfd write fragment in payload still rejected",
			line: `474607 pwrite64(5</tmp/output>, "write(6<{eventfd-count=0}>, pad", 27, 0) = 27`,
			want: false,
		},
		{
			name: "writev to regular file with eventfd write fragment in payload still rejected",
			line: `474607 writev(5</tmp/output>, [{iov_base="write(6<{eventfd-count=0}>, ", iov_len=27}], 1) = 27`,
			want: false,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAllowedHotPathWrite(tt.line, socket); got != tt.want {
				t.Fatalf("isAllowedHotPathWrite() = %t, want %t", got, tt.want)
			}
		})
	}
}

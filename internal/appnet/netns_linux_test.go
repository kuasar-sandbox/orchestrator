package appnet

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
)

// Use an unnamed, child-owned namespace so this check cannot collide with or
// clean up another task's named network resources. Run the compiled test as
// root for the real namespace check; ordinary unprivileged unit tests skip it.
func TestListenTCPInIsolatedNetNS(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("real network-namespace check requires root")
	}
	for _, tool := range []string{"unshare", "ip", "sh"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("real network-namespace check requires %s: %v", tool, err)
		}
	}
	// EUID 0 alone does not grant namespace/network capabilities in a container.
	// Probe the actual setup separately; failures after this probe remain fatal.
	if output, err := exec.Command("unshare", "--net", "--", "sh", "-c",
		"ip addr add 192.0.2.2/32 dev lo && ip link set lo up").CombinedOutput(); err != nil {
		t.Skipf("real network-namespace setup is unavailable: %v: %s", err, output)
	}
	command := exec.Command("unshare", "--net", "--", "sh", "-c",
		"set -eu; ip addr add 192.0.2.2/32 dev lo; ip link set lo up; echo ready; read finish")
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = input.Close()
		_ = command.Process.Kill()
		_ = command.Wait()
	})
	scanner := bufio.NewScanner(output)
	if !scanner.Scan() || scanner.Text() != "ready" {
		t.Fatal("isolated namespace setup failed")
	}
	namespace, err := OpenProxyNetNS(fmt.Sprintf("/proc/%d/ns/net", command.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer namespace.Close()
	// Address binding is not an ownership test when ip_nonlocal_bind=1.
	// Keep this goroutine on its original thread and compare namespace inodes.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := os.Stat("/proc/thread-self/ns/net")
	if err != nil {
		t.Fatal(err)
	}
	child, err := os.Stat(fmt.Sprintf("/proc/%d/ns/net", command.Process.Pid))
	if err != nil || os.SameFile(original, child) {
		t.Fatalf("test namespace is not distinct: %v", err)
	}
	listener, err := ListenTCPInNetNS(namespace, "192.0.2.2:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	restored, err := os.Stat("/proc/thread-self/ns/net")
	if err != nil || !os.SameFile(original, restored) {
		t.Fatal("calling thread was not restored to its original namespace")
	}
	// A host-namespace socket cannot receive this connection through the
	// child's private loopback interface, even if nonlocal host binds work.
	if err := namespace.Do(func() error {
		connection, err := net.DialTimeout("tcp4", listener.Addr().String(), time.Second)
		if err != nil {
			return err
		}
		return connection.Close()
	}); err != nil {
		t.Fatalf("listener is not reachable inside the child namespace: %v", err)
	}
}

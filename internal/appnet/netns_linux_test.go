package appnet

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
)

// Use an unnamed, child-owned namespace so this check cannot collide with or
// clean up another task's named network resources. Run the compiled test as
// root for the real namespace check; ordinary unprivileged unit tests skip it.
func TestListenTCPInIsolatedNetNS(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("real network-namespace check requires root")
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
	if listener, err := net.Listen("tcp4", "192.0.2.2:0"); err == nil {
		listener.Close()
		t.Fatal("isolated test address unexpectedly exists in the host namespace")
	}
	listener, err := ListenTCPInNetNS(namespace, "192.0.2.2:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if listener, err := net.Listen("tcp4", "192.0.2.2:0"); err == nil {
		listener.Close()
		t.Fatal("calling thread was not restored to its original namespace")
	}
}

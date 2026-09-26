// Package sandboxproc provides the shared, policy-free sandbox-ctl spawn boundary.
package sandboxproc

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Start attaches trusted runtime descriptors and starts cmd. The caller owns
// command arguments, environment, working directory, stdio, and exactly one
// subsequent Wait after success. No context, signal or cleanup policy is added.
//
// vmmCgroup is borrowed and remains open. Ownership of ready is transferred:
// its parent copy is closed before every return, including a failed Start, so
// the readiness reader can observe EOF independently of the child's lifetime.
func Start(cmd *exec.Cmd, vmmCgroup, ready *os.File) error {
	if ready != nil {
		defer ready.Close()
	}
	if cmd == nil || len(cmd.Args) == 0 {
		return fmt.Errorf("sandboxproc: command and argument vector are required")
	}
	if cmd.Process != nil {
		return fmt.Errorf("sandboxproc: command has already been started")
	}
	if !validDescriptor(vmmCgroup) {
		return fmt.Errorf("sandboxproc: open VMM cgroup descriptor is required")
	}
	if !validDescriptor(ready) {
		return fmt.Errorf("sandboxproc: open readiness descriptor is required")
	}
	for _, arg := range cmd.Args[1:] {
		for _, name := range []string{"--cgroup-path", "--cgroup-adopt", "--ready-fd"} {
			if arg == name || strings.HasPrefix(arg, name+"=") {
				return fmt.Errorf("sandboxproc: command already contains owned argument %q", name)
			}
		}
	}
	vmmFD := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, vmmCgroup)
	readyFD := 3 + len(cmd.ExtraFiles)
	cmd.ExtraFiles = append(cmd.ExtraFiles, ready)
	cmd.Args = append(cmd.Args, fmt.Sprintf("--cgroup-path=fd=%d", vmmFD), fmt.Sprintf("--ready-fd=%d", readyFD))
	if err := cmd.Start(); err != nil {
		return err
	}
	// Closing the parent's readiness copy is deliberately not a new failure
	// after a successful Start: the caller must still own and reap this child.
	return nil
}

func validDescriptor(file *os.File) bool {
	return file != nil && file.Fd() >= 3 && file.Fd() != ^uintptr(0)
}

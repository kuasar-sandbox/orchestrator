// Package helperexec runs directly owned, cooperative utility processes.
// Services, VM runners, guest exec and short admission probes have separate
// ownership and deadline protocols and must not use this policy.
package helperexec

import (
	"context"
	"os/exec"
	"syscall"
	"time"
)

// CommandContext preserves the caller's operation context. Cancellation asks
// the child to unwind, then kills it after five seconds if necessary. Callers
// must still Run or Wait before releasing resources used by the child: sending
// a signal is not proof that it has stopped reading. Wait covers this direct
// child only, not remote work or descendants. Already committed work is not
// rolled back. Five seconds is an internal fallback, not a cleanup guarantee.
func CommandContext(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

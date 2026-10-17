package proxyapp

import (
	"runtime"
	"syscall"

	"golang.org/x/sys/unix"
)

// SIGUSR2's default disposition must not become visible between exec and the
// native entry's Notify registration. A blocked signal survives exec; Notify
// enables delivery again. False never installs an ignore handler or blocks it.
func holdRestartSignal(enabled bool) (func(), error) {
	if !enabled {
		return func() {}, nil
	}
	runtime.LockOSThread()
	var mask, prior unix.Sigset_t
	mask.Val[(uint(syscall.SIGUSR2)-1)/64] |= 1 << ((uint(syscall.SIGUSR2) - 1) % 64)
	if err := unix.PthreadSigmask(unix.SIG_BLOCK, &mask, &prior); err != nil {
		runtime.UnlockOSThread()
		return nil, err
	}
	return func() { _ = unix.PthreadSigmask(unix.SIG_SETMASK, &prior, nil); runtime.UnlockOSThread() }, nil
}

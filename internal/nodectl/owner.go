package nodectl

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"golang.org/x/sys/unix"
)

// OwnerLock prevents two controllers from racing over the stable UDS. It must
// be acquired before an existing socket path is inspected or removed.
type OwnerLock struct {
	Path string

	mu   sync.Mutex
	file *os.File
}

func (o *OwnerLock) Acquire() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file != nil {
		return nil
	}
	fd, err := unix.Open(o.Path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("controller owner open %s: %w", o.Path, err)
	}
	f := os.NewFile(uintptr(fd), "resource-controller-owner")
	if f == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("controller owner adopt fd")
	}
	lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
	if err := unix.FcntlFlock(f.Fd(), unix.F_SETLK, &lock); err != nil {
		_ = f.Close()
		return fmt.Errorf("resource controller already owns %s: %w", o.Path, err)
	}
	if err := f.Truncate(0); err != nil {
		_ = f.Close()
		return fmt.Errorf("controller owner truncate: %w", err)
	}
	if _, err := f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0); err != nil {
		_ = f.Close()
		return fmt.Errorf("controller owner write: %w", err)
	}
	o.file = f
	return nil
}

func (o *OwnerLock) Close() error {
	if o == nil {
		return nil
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.file == nil {
		return nil
	}
	err := o.file.Close()
	o.file = nil
	return err
}

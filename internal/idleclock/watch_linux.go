package idleclock

import (
	"errors"
	"time"

	"golang.org/x/sys/unix"
)

// Watch detects even a forward-and-back realtime step between observations.
// Only the owner under its observation lock calls Poll/Close.
type Watch struct {
	fd     int
	failed error
}

func NewWatch() (*Watch, error) {
	fd, err := unix.TimerfdCreate(unix.CLOCK_REALTIME, unix.TFD_NONBLOCK|unix.TFD_CLOEXEC)
	if err != nil {
		return nil, err
	}
	w := &Watch{fd: fd}
	if err = w.arm(); err != nil {
		w.Close()
		return nil, err
	}
	return w, nil
}
func (w *Watch) arm() error {
	ts := unix.NsecToTimespec(time.Now().Add(365 * 24 * time.Hour).UnixNano())
	return unix.TimerfdSettime(w.fd, unix.TFD_TIMER_ABSTIME|unix.TFD_TIMER_CANCEL_ON_SET, &unix.ItimerSpec{Value: ts}, nil)
}
func (w *Watch) Poll() (changed bool, err error) {
	if w.failed != nil {
		w.failed = w.arm()
		return true, w.failed
	}
	var buf [8]byte
	_, err = unix.Read(w.fd, buf[:])
	if errors.Is(err, unix.EAGAIN) {
		return false, nil
	}
	if err == nil || errors.Is(err, unix.ECANCELED) {
		// A successful read is ordinary expiry, not a realtime clock step.
		changed = errors.Is(err, unix.ECANCELED)
		w.failed = w.arm()
		return changed || w.failed != nil, w.failed
	}
	w.failed = err
	return true, err
}
func (w *Watch) Close() { _ = unix.Close(w.fd) }

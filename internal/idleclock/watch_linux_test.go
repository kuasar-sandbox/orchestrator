package idleclock

import (
	"errors"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestWatchNormalExpirationRearmsWithoutClockChange(t *testing.T) {
	w, err := NewWatch()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	if changed, err := w.Poll(); changed || err != nil {
		t.Fatalf("new watch: changed=%v err=%v", changed, err)
	}
	// Expire this watch immediately without changing the host's realtime clock.
	if err := unix.TimerfdSettime(w.fd, unix.TFD_TIMER_ABSTIME|unix.TFD_TIMER_CANCEL_ON_SET,
		&unix.ItimerSpec{Value: unix.NsecToTimespec(1)}, nil); err != nil {
		t.Fatal(err)
	}
	fds := []unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLIN}}
	if n, err := unix.Poll(fds, 1000); err != nil || n != 1 || fds[0].Revents&unix.POLLIN == 0 {
		t.Fatalf("timer did not expire: n=%d events=%v err=%v", n, fds[0].Revents, err)
	}
	if changed, err := w.Poll(); changed || err != nil {
		t.Fatalf("ordinary expiry invalidated clock proof: changed=%v err=%v", changed, err)
	}
	var remaining unix.ItimerSpec
	if err := unix.TimerfdGettime(w.fd, &remaining); err != nil {
		t.Fatal(err)
	}
	if remaining.Value.Nano() < int64(300*24*time.Hour) {
		t.Fatalf("watch was not rearmed: %v", remaining.Value)
	}
	if changed, err := w.Poll(); changed || err != nil {
		t.Fatalf("rearmed watch: changed=%v err=%v", changed, err)
	}
}

func TestWatchReadAndRearmFailuresStayUncertain(t *testing.T) {
	w := &Watch{fd: -1}
	for _, phase := range []string{"read", "rearm"} {
		if changed, err := w.Poll(); !changed || !errors.Is(err, unix.EBADF) {
			t.Fatalf("%s failure: changed=%v err=%v", phase, changed, err)
		}
	}
}

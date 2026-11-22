package launcher

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
)

func TestStopWaiterCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := new(Systemd)
		unlock, err := s.lockStop(context.Background(), "held.service")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			release, err := s.lockStop(ctx, "held.service")
			if release != nil {
				release()
			}
			result <- err
		}()
		synctest.Wait() // The waiter must be blocked behind the current owner.
		cancel()
		synctest.Wait()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter: %v", err)
		}
		other, err := s.lockStop(context.Background(), "independent.service")
		if err != nil {
			t.Fatal(err)
		}
		other()
		if len(s.stopping) != 1 {
			t.Fatalf("cancellation removed the active stop: %d entries", len(s.stopping))
		}
		unlock()
		if len(s.stopping) != 0 {
			t.Fatal("completed stop retained unit state")
		}
		retry, err := s.lockStop(context.Background(), "held.service")
		if err != nil {
			t.Fatal(err)
		}
		retry()
	})
}

package orch

import (
	"context"
	"testing"
	"time"
)

func TestMMDSSecretWaiterNotifyWakesWaiter(t *testing.T) {
	w := newMMDSSecretWaiter(2 * time.Second)
	done := make(chan bool, 1)
	go func() {
		done <- w.wait(context.Background(), "sbx-1", "key1")
	}()

	// Give the goroutine a chance to register itself before notifying.
	time.Sleep(20 * time.Millisecond)
	w.Notify("sbx-1", "key1")

	select {
	case woke := <-done:
		if !woke {
			t.Fatal("wait returned false, want true (woken by Notify)")
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after Notify")
	}
}

func TestMMDSSecretWaiterTimesOut(t *testing.T) {
	w := newMMDSSecretWaiter(30 * time.Millisecond)
	start := time.Now()
	woke := w.wait(context.Background(), "sbx-1", "key1")
	if woke {
		t.Fatal("wait returned true, want false (timeout, no Notify)")
	}
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Fatalf("wait returned after %v, want >= timeout", elapsed)
	}
}

func TestMMDSSecretWaiterContextCancel(t *testing.T) {
	w := newMMDSSecretWaiter(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() {
		done <- w.wait(ctx, "sbx-1", "key1")
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case woke := <-done:
		if woke {
			t.Fatal("wait returned true, want false (ctx canceled)")
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return after ctx cancel")
	}
}

func TestMMDSSecretWaiterNotifyOnlyWakesMatchingKey(t *testing.T) {
	w := newMMDSSecretWaiter(200 * time.Millisecond)
	done := make(chan bool, 1)
	go func() {
		done <- w.wait(context.Background(), "sbx-1", "key1")
	}()

	time.Sleep(20 * time.Millisecond)
	w.Notify("sbx-1", "other-key")
	w.Notify("sbx-2", "key1")

	select {
	case woke := <-done:
		if woke {
			t.Fatal("wait returned true, want false (Notify was for a different key)")
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not return")
	}
}

func TestMMDSSecretWaiterForgetSandboxWakesAllItsWaiters(t *testing.T) {
	w := newMMDSSecretWaiter(2 * time.Second)
	done1 := make(chan bool, 1)
	done2 := make(chan bool, 1)
	doneOther := make(chan bool, 1)
	go func() { done1 <- w.wait(context.Background(), "sbx-1", "key1") }()
	go func() { done2 <- w.wait(context.Background(), "sbx-1", "key2") }()
	go func() { doneOther <- w.wait(context.Background(), "sbx-2", "key1") }()

	time.Sleep(20 * time.Millisecond)
	w.ForgetSandbox("sbx-1")

	for _, ch := range []chan bool{done1, done2} {
		select {
		case woke := <-ch:
			if !woke {
				t.Fatal("wait returned false, want true (force-woken by ForgetSandbox)")
			}
		case <-time.After(time.Second):
			t.Fatal("wait did not return after ForgetSandbox")
		}
	}

	select {
	case <-doneOther:
		t.Fatal("unrelated sandbox's waiter was woken by ForgetSandbox on a different sandbox")
	case <-time.After(50 * time.Millisecond):
	}

	w.mu.Lock()
	_, stillThere := w.waiters[mmdsSecretWaitKey("sbx-1", "key1")]
	w.mu.Unlock()
	if stillThere {
		t.Fatal("ForgetSandbox left a stale waiter entry behind")
	}

	// Cleanup: unblock the still-parked unrelated waiter.
	w.Notify("sbx-2", "key1")
	<-doneOther
}

func TestMMDSSecretWaiterZeroTimeoutDefaults(t *testing.T) {
	w := newMMDSSecretWaiter(0)
	if w.timeout <= 0 {
		t.Fatalf("timeout = %v, want a positive default", w.timeout)
	}
}

// TestKillForgetsSecretWaiters is an integration check (not just the
// primitive in isolation) that Kill's o.secretWait.ForgetSandbox(id) call
// actually wakes a goroutine parked on that sandbox's never-configured
// secret, rather than leaving it to time out on its own.
func TestKillForgetsSecretWaiters(t *testing.T) {
	o := testMMDSSecretsOrch(t, 1024)
	o.vs = stubVS{}
	o.secretWait = newMMDSSecretWaiter(5 * time.Second) // long -- must be woken, not timed out
	sb := putTestSandboxForMMDSSecretAdmin(t, o, "sbx-1", testMMDSSecretSpec)
	_, apiKey := defaultTestCredentials(t, sb.ManifestKey)

	done := make(chan bool, 1)
	go func() { done <- o.secretWait.wait(context.Background(), "sbx-1", "key1") }()
	time.Sleep(30 * time.Millisecond) // let the goroutine reach the park

	start := time.Now()
	ok, err := o.Kill(context.Background(), "sbx-1", apiKey)
	if err != nil || !ok {
		t.Fatalf("Kill: ok=%t err=%v", ok, err)
	}

	select {
	case woke := <-done:
		if !woke {
			t.Fatal("expected Kill to force-wake the parked waiter via ForgetSandbox")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("wake took too long: %v", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("parked waiter was not woken by Kill")
	}
}

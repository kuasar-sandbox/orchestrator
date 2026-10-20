package proxyapp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
)

// Decode actual stats protocol frames without applying lifecycle controls.
// Only a validated Goodbye returns nil; EOF is a separate failure.
func shutdownStatsResult(t *testing.T, ending string) error {
	t.Helper()
	stats := proxystats.NewMasterStats(nil, []string{"proxy-0"})
	if err := stats.BeginWorker("proxy-0", 1); err != nil {
		t.Fatal(err)
	}
	var wire bytes.Buffer
	frames := []proxystats.Frame{
		{Type: proxystats.TypeHello, Version: proxystats.Version, WorkerID: "proxy-0", Epoch: 1},
		{Type: proxystats.TypeReady, Version: proxystats.Version, Epoch: 1, Sequence: 1},
	}
	if ending != "eof" {
		sequence := uint64(2)
		if ending == "sequence-gap" {
			sequence = 3
		}
		frames = append(frames, proxystats.Frame{
			Type: proxystats.TypeGoodbye, Version: proxystats.Version, Epoch: 1, Sequence: sequence,
		})
	}
	for _, frame := range frames {
		if err := proxystats.WriteFrame(&wire, frame); err != nil {
			t.Fatal(err)
		}
	}
	payload := wire.Bytes()
	if ending == "truncated" {
		payload = payload[:len(payload)-1]
	}
	return stats.ReadWorkerStream(context.Background(), bytes.NewReader(payload), "proxy-0", 1)
}

func shutdownStreamInstance(t *testing.T, stopping bool, deadline time.Time) *Instance {
	t.Helper()
	instance := NewInstance("proxy", "", "", 1)
	if !instance.attachWorker(0, 1, nil) {
		t.Fatal("could not attach worker")
	}
	if stopping {
		instance.requestStop(deadline)
	}
	return instance
}

func TestWorkerStatsGoodbyeBeforeQueuedControlCompletion(t *testing.T) {
	deadline := time.Now().Add(time.Second)
	instance := shutdownStreamInstance(t, true, deadline)
	err := shutdownStatsResult(t, "goodbye")
	if err != nil {
		t.Fatalf("valid Goodbye was not accepted: %v", err)
	}
	if instance.workerFinishing(0, 1) {
		t.Fatal("test accidentally processed lifecycle frames")
	}
	if !instance.workerStreamFinishing(0, 1, err) {
		t.Fatal("valid Goodbye overtook lifecycle controls and would kill the draining worker")
	}
	worker := instance.workers[0]
	if instance.deadline != deadline || !worker.alive || worker.dataDone || worker.mmdsDone || worker.stopped {
		t.Fatal("Goodbye changed the deadline or fabricated process/control completion")
	}
	if err := instance.workerEvent(0, 1, controlMessage{Kind: "stopped"}); err == nil {
		t.Fatal("Goodbye allowed stopped before endpoint completion")
	}
	// Lifecycle acknowledgments and independently observed process exit,
	// rather than Goodbye, complete the drain.
	for _, kind := range []string{"stop_requested", "data_drained", "mmds_drained", "stopped"} {
		if err := instance.workerEvent(0, 1, controlMessage{Kind: kind}); err != nil {
			t.Fatal(err)
		}
	}
	if !instance.workers[0].alive {
		t.Fatal("control completion fabricated process exit")
	}
	instance.workerExited(0, 1, nil)
	if err := instance.drainWorkers(); err != nil {
		t.Fatalf("valid lifecycle completion failed: %v", err)
	}
}

func TestWorkerStatsEarlyClosureStillFails(t *testing.T) {
	for _, test := range []struct {
		ending string
		want   error
	}{
		{"eof", io.EOF},
		{"truncated", io.ErrUnexpectedEOF},
		{"sequence-gap", nil},
	} {
		t.Run(test.ending, func(t *testing.T) {
			err := shutdownStatsResult(t, test.ending)
			if err == nil || (test.want != nil && !errors.Is(err, test.want)) {
				t.Fatalf("unexpected stats decode result: %v", err)
			}
			instance := shutdownStreamInstance(t, true, time.Now().Add(time.Second))
			if instance.workerStreamFinishing(0, 1, err) {
				t.Fatalf("stats failure deferred before endpoint completion: %v", err)
			}
		})
	}
}

func TestWorkerStatsGoodbyeRequiresSameStoppingInstance(t *testing.T) {
	instance := shutdownStreamInstance(t, false, time.Time{})
	if instance.workerStreamFinishing(0, 1, nil) {
		t.Fatal("unexpected Goodbye outside instance stop was accepted")
	}
	if err := instance.workerEvent(0, 1, controlMessage{Kind: "stop_requested"}); err != nil {
		t.Fatal(err)
	}
	if instance.workerStreamFinishing(0, 1, nil) {
		t.Fatal("worker-only stop broadened the instance-stop exception")
	}
	instance.requestStop(time.Now().Add(time.Second))
	if instance.workerStreamFinishing(0, 2, nil) {
		t.Fatal("stale worker epoch was accepted")
	}
}

func TestWorkerStatsGoodbyeCannotReplaceStoppedAcknowledgement(t *testing.T) {
	instance := shutdownStreamInstance(t, true, time.Now().Add(time.Second))
	if !instance.workerStreamFinishing(0, 1, nil) {
		t.Fatal("valid Goodbye should defer to terminal evidence")
	}
	exitErr := errors.New("exit status 23")
	instance.workerExited(0, 1, exitErr)
	if err := instance.drainWorkers(); !errors.Is(err, ErrForcedClose) || !errors.Is(err, exitErr) {
		t.Fatalf("missing lifecycle completion lost the actual terminal failure: %v", err)
	}
}

func TestWorkerStatsGoodbyeCannotExtendOriginalDrainDeadline(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	instance := shutdownStreamInstance(t, true, deadline)
	// An independently stopping worker supplies neither endpoint completion
	// nor process exit; no control socket send is needed to test the deadline.
	if err := instance.workerEvent(0, 1, controlMessage{Kind: "stop_requested"}); err != nil {
		t.Fatal(err)
	}
	if !instance.workerStreamFinishing(0, 1, nil) {
		t.Fatal("valid Goodbye should defer to terminal evidence")
	}
	err := instance.drainWorkers()
	if !errors.Is(err, ErrForcedClose) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("nonterminating worker escaped the original drain deadline: %v", err)
	}
	if instance.deadline != deadline {
		t.Fatal("Goodbye reset the original deadline")
	}
}

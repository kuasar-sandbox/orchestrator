package proxyapp

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestPauseActivityCollectsEveryWorkerEpochIncludingDrain(t *testing.T) {
	instance := NewInstance("proxy", "boot", "", 2)
	errors := make(chan error, 4)
	for index := 0; index < 2; index++ {
		master, worker := net.Pipe()
		defer master.Close()
		defer worker.Close()
		identity := controlIdentity{BootID: "boot", WorkerID: fmt.Sprint(index), WorkerEpoch: 1}
		peer, child := newControlPeer(master, identity), newControlPeer(worker, identity)
		instance.attachWorker(index, 1, peer)
		instance.mu.Lock()
		instance.workers[index].prepared = true
		instance.workers[index].stopping = index == 1
		instance.mu.Unlock()
		go func() {
			request, err := child.receive()
			if err != nil {
				errors <- err
				return
			}
			errors <- child.send(controlMessage{Kind: "pause_ack", BarrierID: request.BarrierID, SID: request.SID, RunID: request.RunID, Activity: map[string]routesync.PauseActivity{"forward": {Parking: 1, Connected: 2}}})
		}()
		go func(index int) {
			message, err := peer.receive()
			if err == nil {
				err = instance.workerEvent(index, 1, message)
			}
			errors <- err
		}(index)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ack := instance.pauseActivity(ctx, "attempt", routesync.RouteEntry{SandboxID: "sandbox", RunID: "run", State: routesync.StatePausing})
	if ack.Error != "" || ack.Activity["forward"].Parking != 2 || ack.Activity["forward"].Connected != 4 {
		t.Fatalf("ACK=%+v", ack)
	}
	for range 4 {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPauseActivityFailureReturnsReceivedCounts(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("worker_changed=%v", changed), func(t *testing.T) {
			instance := NewInstance("proxy", "boot", "", 2)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			sent := make(chan struct{}, 2)
			for index := 0; index < 2; index++ {
				master, worker := net.Pipe()
				defer master.Close()
				defer worker.Close()
				identity := controlIdentity{BootID: "boot", WorkerID: fmt.Sprint(index), WorkerEpoch: 1}
				peer, child := newControlPeer(master, identity), newControlPeer(worker, identity)
				instance.attachWorker(index, 1, peer)
				instance.mu.Lock()
				instance.workers[index].prepared = true
				instance.mu.Unlock()
				go func() {
					if _, err := child.receive(); err == nil {
						sent <- struct{}{}
					}
				}()
			}
			go func() {
				for range 2 {
					select {
					case <-sent:
					case <-ctx.Done():
						return
					}
				}
				_ = instance.workerEvent(0, 1, controlMessage{Kind: "pause_ack", BarrierID: "attempt", SID: "sandbox", RunID: "run", Activity: map[string]routesync.PauseActivity{"exec": {Connected: 3}}})
				if changed {
					instance.mu.Lock()
					instance.workers[1].epoch++
					instance.notifyLocked()
					instance.mu.Unlock()
				} else {
					cancel()
				}
			}()
			ack := instance.pauseActivity(ctx, "attempt", routesync.RouteEntry{SandboxID: "sandbox", RunID: "run", State: routesync.StatePausing})
			if ack.Error == "" || ack.Activity["exec"].Connected != 3 {
				t.Fatalf("incomplete ACK=%+v", ack)
			}
		})
	}
}

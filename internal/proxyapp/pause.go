package proxyapp

import (
	"context"
	"fmt"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func (s *readinessSink) PreparePause(ctx context.Context, id string, route routesync.RouteEntry) routesync.PauseAck {
	if err := s.Sink.ApplyUpsert(route); err != nil {
		return routesync.PauseAck{Activity: map[string]routesync.PauseActivity{}, Error: err.Error()}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.instance.pauseActivity(ctx, id, route)
}

func (i *Instance) pauseActivity(ctx context.Context, id string, route routesync.RouteEntry) (out routesync.PauseAck) {
	out.Activity = make(map[string]routesync.PauseActivity)
	i.mu.Lock()
	participants := append([]workerState(nil), i.workers...)
	if i.pauseSamples == nil {
		i.pauseSamples = make(map[string]map[int]controlMessage)
	}
	i.pauseSamples[id] = make(map[int]controlMessage)
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		defer i.mu.Unlock()
		// Include every sample actually received, even if another worker failed
		// or timed out. Error keeps this partial observation from certifying idle.
		for _, sample := range i.pauseSamples[id] {
			if sample.SID != route.SandboxID || sample.RunID != route.RunID || sample.Activity == nil {
				out.Error = "invalid worker pause ACK"
				continue
			}
			if sample.Error != "" {
				out.Error = fmt.Sprintf("worker pause ACK: %s", sample.Error)
			}
			for service, activity := range sample.Activity {
				total := out.Activity[service]
				total.Parking += activity.Parking
				total.Connected += activity.Connected
				out.Activity[service] = total
			}
		}
		delete(i.pauseSamples, id)
	}()
	if len(participants) == 0 {
		out.Error = "no workers"
		return
	}
	for _, w := range participants {
		if !w.alive || !w.prepared || w.peer == nil {
			out.Error = "worker is not prepared"
			return
		}
	}
	deadline, _ := ctx.Deadline()
	sent := make(chan error, len(participants))
	for _, w := range participants {
		go func(w workerState) {
			sent <- w.peer.send(controlMessage{Kind: "pause_stats", BarrierID: id, SID: route.SandboxID, RunID: route.RunID, Deadline: deadline})
		}(w)
	}
	for range participants {
		select {
		case err := <-sent:
			if err != nil {
				out.Error = err.Error()
				return
			}
		case <-ctx.Done():
			out.Error = ctx.Err().Error()
			return
		}
	}
	for {
		i.mu.Lock()
		samples := i.pauseSamples[id]
		changed := i.changed
		for index, w := range participants {
			current := i.workers[index]
			if !current.alive || current.epoch != w.epoch || current.peer != w.peer {
				out.Error = "worker changed during pause handshake"
				break
			}
		}
		complete := len(samples) == len(participants)
		if complete || out.Error != "" {
			i.mu.Unlock()
			return
		}
		i.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			out.Error = ctx.Err().Error()
			return
		}
	}
}

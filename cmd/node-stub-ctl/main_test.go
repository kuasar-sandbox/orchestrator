package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestBuildRegisterReplayPreservesIdentityAndState(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := newService("", log)
	node := newStubNode(stubNodeOptions{ID: "n1"}, svc)
	cmd := &routesync.Command{
		CmdID: "c1", Kind: routesync.CmdBuildRegister,
		BuildID: "b1", TemplateRef: "transient-1", Profile: "bare", KeyFingerprint: "fp1",
		Config: map[string]string{"stub.build_result": "timeout"},
	}
	if got := node.handleBuildRegister(cmd); got.Status != routesync.AckAccepted {
		t.Fatalf("first build_register ack = %+v", got)
	}
	node.mu.Lock()
	node.builds[cmd.BuildID].State = "ready"
	node.mu.Unlock()

	replay := *cmd
	replay.CmdID = "c2"
	if got := node.handleBuildRegister(&replay); got.Status != routesync.AckAccepted {
		t.Fatalf("identical replay ack = %+v", got)
	}
	for name, mutate := range map[string]func(*routesync.Command){
		"profile":  func(c *routesync.Command) { c.Profile = "e2b" },
		"template": func(c *routesync.Command) { c.TemplateRef = "transient-2" },
		"tenant":   func(c *routesync.Command) { c.KeyFingerprint = "fp2" },
	} {
		t.Run(name, func(t *testing.T) {
			conflict := replay
			mutate(&conflict)
			if got := node.handleBuildRegister(&conflict); got.Status != routesync.AckRejected {
				t.Fatalf("conflicting replay ack = %+v", got)
			}
		})
	}

	node.mu.Lock()
	stored := node.builds[cmd.BuildID]
	buildCount := len(node.builds)
	node.mu.Unlock()
	if buildCount != 1 || stored.Profile != "bare" || stored.TemplateID != "transient-1" || stored.State != "ready" {
		t.Fatalf("replay changed stored build: count=%d build=%+v", buildCount, stored)
	}
	svc.mu.Lock()
	eventCount := len(svc.events)
	svc.mu.Unlock()
	if eventCount != 1 {
		t.Fatalf("replay restarted build state machine: events=%d", eventCount)
	}
}

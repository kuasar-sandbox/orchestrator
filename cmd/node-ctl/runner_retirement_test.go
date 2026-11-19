package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	nativeusage "github.com/kuasar-sandbox/sandboxer/pkg/usage"
)

func TestParentRetirementFreezesUsageBeforeAckAndNeverReadsSuccessor(t *testing.T) {
	base := t.TempDir()
	old, err := nativeusage.Open(base, "sid", "old-epoch", time.Now(), time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	old.Close(ctx, time.Now())
	cancel()
	dir, err := os.MkdirTemp("", "retire-parent-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "c.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var stopped atomic.Int32
	var acknowledged atomic.Int32
	event := configsock.RunRetirementEvent{OperationID: "op", SandboxID: "sid", InstanceID: "instance", RunID: "run", CheckpointID: "cp", MemoryID: "mem", Epoch: 1}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req configsock.RunRoleRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if stopped.Load() != 1 {
			t.Error("ACK arrived before sampler stop/join")
		}
		if req.Retirement == nil || *req.Retirement != event {
			t.Error("wrong exact event")
			w.WriteHeader(409)
			return
		}
		if acknowledged.Add(1) == 1 {
			// The host may publish a new runtime immediately after accepting ACK.
			// Replace the pathname to expose any subsequent stale final read.
			if err := os.Remove(filepath.Join(base, "sid.usage")); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(filepath.Join(base, "sid.usage"), []byte("new run owns this path"), 0600); err != nil {
				t.Error(err)
			}
		}
		_ = json.NewEncoder(w).Encode(configsock.RunRoleResponse{Pending: true, Retirement: &event, Acknowledged: true})
	})}
	go server.Serve(listener)
	defer server.Close()
	metrics := newRunnerMetrics(runnerMetricsIdentity{sandboxID: "sid", runID: "run", baseDir: base, runDir: filepath.Join(base, "run")}, nil)
	p := newParentRetirement(socket, "sid", "run", metrics, func() { stopped.Add(1) }, nil)
	p.accept(event)
	p.accept(event) // replay after a lost ACK response must not reread the path
	if !p.retired || p.terminal == nil || p.terminal.Saved == nil || p.terminal.Saved.Snapshot.RunEpoch != "old-epoch" {
		t.Fatalf("private terminal view = %+v", p.terminal)
	}
	if stopped.Load() != 1 || acknowledged.Load() != 2 {
		t.Fatalf("stop=%d ack=%d", stopped.Load(), acknowledged.Load())
	}
}

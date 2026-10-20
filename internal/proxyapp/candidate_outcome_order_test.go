package proxyapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// This exercises runCandidate, the real packet control reader/writer and the
// real startLocal result publication. Only MasterRunner is a controlled fake:
// this is a service outcome regression, not a worker/process cleanup test.
func TestCandidateOutcomeIndependentOfCompletionOrder(t *testing.T) {
	for _, order := range []string{"local-first", "control-first"} {
		for _, result := range []struct {
			name string
			err  error
			want string
		}{
			{"clean", nil, "completed"},
			{"forced", ErrForcedClose, "completed_with_forced_close"},
			{"failure", errors.New("fixture shutdown callback"), "failed"},
		} {
			t.Run(order+"/"+result.name, func(t *testing.T) {
				coordinator, candidate := terminalErrorWires(t)
				// Read deadlines bound test failures; they do not determine order.
				if err := coordinator.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				effective, _ := testMasterEffective(t)
				cfg := effective.Config()
				service := NewService()
				service.started, service.candidate = true, true
				service.parent = candidate
				service.operation = &RestartStatus{ID: candidate.operation}
				service.resources = &retainedResources{
					StatsPath: cfg.StatsSocket, ShmPath: cfg.ShmPath,
					MetricsExpression: cfg.MetricsListen,
				}
				var err error
				service.stopRecord, err = newStopRecord()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { service.stopRecord.file.Close() })
				var logs bytes.Buffer
				service.prepare = func(context.Context, ServiceInput) (*Startup, error) {
					return &Startup{Effective: effective,
						Runtime: &Runtime{Logger: slog.New(slog.NewJSONHandler(&logs, nil))}}, nil
				}
				started := make(chan *Instance, 1)
				draining := make(chan struct{})
				release := make(chan struct{})
				var releaseOnce sync.Once
				releaseRunner := func() { releaseOnce.Do(func() { close(release) }) }
				service.runner = func(_ context.Context, _ *EffectiveConfig, runtime *Runtime) error {
					started <- runtime.Instance
					<-runtime.Instance.stop
					close(draining)
					<-release
					runtime.Instance.finish(result.err)
					return result.err
				}
				done := make(chan error, 1)
				go func() { done <- service.runCandidate(context.Background()) }()
				var instance *Instance
				select {
				case instance = <-started:
				case err := <-done:
					t.Fatalf("candidate failed before starting: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("candidate did not start")
				}
				t.Cleanup(func() {
					coordinator.conn.Close()
					instance.requestStop(time.Now())
					releaseRunner()
				})
				readKind := func(want string) {
					t.Helper()
					message, err := coordinator.read()
					if err != nil || message.Kind != want {
						t.Fatalf("control packet: got %q, want %q, error=%v", message.Kind, want, err)
					}
				}
				readKind("snapshot")
				if err := coordinator.send(serviceMessage{Kind: "detach"}); err != nil {
					t.Fatal(err)
				}
				readKind("detached")
				// The real exec path pauses B at this acknowledgement. A paused
				// candidate sends no more snapshots until localDone is consumed.
				deadline := time.Now().Add(time.Minute)
				if err := coordinator.send(serviceMessage{Kind: "drain", Deadline: deadline}); err != nil {
					t.Fatal(err)
				}
				select {
				case <-draining:
				case <-time.After(5 * time.Second):
					t.Fatal("candidate did not apply the original drain")
				}
				if order == "local-first" {
					releaseRunner()
					// This packet proves that the actual localDone branch won.
					// Close control only after that branch's final snapshot.
					readKind("snapshot")
					coordinator.conn.Close()
				} else {
					coordinator.conn.Close()
					// Only finishCandidateAfterControlLoss closes service.stop
					// in this initialized candidate. It is now joining localDone.
					select {
					case <-service.stop:
					case <-time.After(5 * time.Second):
						t.Fatal("candidate did not enter control-loss completion")
					}
					releaseRunner()
				}
				select {
				case err := <-done:
					if result.err != nil && !errors.Is(err, result.err) {
						t.Fatalf("business result was lost: %v", err)
					}
					if errors.Is(err, ErrForcedClose) != errors.Is(result.err, ErrForcedClose) {
						t.Fatalf("control ordering changed forced-close result: %v", err)
					}
					if order == "local-first" && result.err == nil && err != nil {
						t.Fatalf("clean local completion changed its return contract: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("candidate joined its consumed result twice or failed to finish")
				}
				instance.mu.Lock()
				actualDeadline := instance.deadline
				instance.mu.Unlock()
				if !actualDeadline.Equal(deadline) {
					t.Fatalf("drain deadline changed: got %v, want %v", actualDeadline, deadline)
				}
				var outcomes []map[string]any
				for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
					if line == "" {
						continue
					}
					var record map[string]any
					if err := json.Unmarshal([]byte(line), &record); err != nil {
						t.Fatal(err)
					}
					if record["msg"] == "proxy candidate shutdown" {
						outcomes = append(outcomes, record)
					}
				}
				if len(outcomes) != 1 || outcomes[0]["result"] != result.want ||
					outcomes[0]["boot_id"] != candidate.boot ||
					outcomes[0]["operation_id"] != string(candidate.operation) {
					t.Fatalf("terminal outcome depends on completion order: logs=%s", &logs)
				}
			})
		}
	}
}
